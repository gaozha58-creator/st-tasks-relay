import { extension_settings, renderExtensionTemplateAsync, getContext } from '../../../extensions.js';
import { saveSettings, saveSettingsDebounced } from '../../../../script.js';
import { chat_completion_sources } from '../../../openai.js';

const MODULE_NAME = 'st-tasks-relay';
const GENERATE_URL = '/api/backends/chat-completions/generate';
const DEFAULT_GATEWAY = 'https://relay.20261121.icu';
const LAST_TASK_KEY = 'st_tasks_relay_last_task_v1';

const LONG_POLL_WAIT_MS = 25000;
const SUBMIT_TIMEOUT_MS = 30000;
const POLL_OVERHEAD_MS = 15000;
const FALLBACK_IDLE_MS = 1500;
const FALLBACK_BUSY_MS = 350;
const POLL_MAX_MIN = 30;
const RETRY_BASE_MS = 1000;
const RETRY_MAX_MS = 10000;
const RETRY_STATUS = new Set([408, 429, 502, 503, 504, 520, 521, 522, 523, 524]);

const ST_INTERNAL = new Set([
    'chat_completion_source', 'type', 'reverse_proxy', 'proxy_password',
    'custom_url', 'custom_include_headers', 'custom_include_body', 'custom_exclude_body',
    'custom_prompt_post_processing', 'group_names', 'use_sysprompt', 'show_thoughts',
    'enable_web_search', 'request_images', 'image_inlining', 'function_calling',
    'assistant_prefill', 'assistant_impersonation', 'max_context', 'api_key_custom',
    'model_source', 'stream_openai', 'include_names', 'continue_prefill', 'continue_postfix',
]);
const VENDOR_RE = /^(claude|vertexai|openrouter|mistral|cohere|google|makersuite|perplexity|xai|zai|ai21|groq|nanogpt|moonshot|chutes|electronhub|pollinations|aimlapi)_/i;

const defaultSettings = {
    enabled: true,
    onlyMainChat: true,
    apiKey: '',
    upstreamOverride: '',
    useLongPoll: true,
    offlineGraceMinutes: 10,
};

let quietActive = false;
function bindGenEvents() {
    try {
        const ctx = getContext(), es = ctx?.eventSource, ev = ctx?.eventTypes || {};
        if (!es || typeof es.on !== 'function') return;
        es.on(ev.GENERATION_STARTED || 'generation_started', (type) => { quietActive = (type === 'quiet'); });
        const clr = () => { quietActive = false; };
        es.on(ev.GENERATION_ENDED || 'generation_ended', clr);
        es.on(ev.GENERATION_STOPPED || 'generation_stopped', clr);
    } catch {}
}

function resolveTemplateBase() {
    try {
        const m = new URL(import.meta.url).pathname.match(/\/scripts\/extensions\/(.+)\/[^/]+$/);
        if (m?.[1]) return m[1];
    } catch {}
    return `third-party/${MODULE_NAME}`;
}

const getOai = () => getContext()?.chatCompletionSettings ?? {};
const normUrl = (u) => String(u || '').trim().replace(/\/+$/, '').toLowerCase();
function loadSettings() {
    extension_settings[MODULE_NAME] = extension_settings[MODULE_NAME] || {};
    for (const k of Object.keys(defaultSettings)) {
        if (extension_settings[MODULE_NAME][k] === undefined) extension_settings[MODULE_NAME][k] = defaultSettings[k];
    }
}
const getSettings = () => extension_settings[MODULE_NAME] ?? defaultSettings;
const setStatus = (t) => { const el = document.getElementById('st_tasks_relay_status'); if (el) el.textContent = t; };
const uuid = () => (typeof crypto?.randomUUID === 'function' ? crypto.randomUUID() : 'id-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 12));

function resolveUpstream() {
    const u = getSettings().upstreamOverride?.trim();
    return (u || DEFAULT_GATEWAY).replace(/\/+$/, '').replace(/\/(v1|v1beta)$/i, '');
}
function resolveApiKey() {
    const k = getSettings().apiKey?.trim();
    if (k) return k;
    throw new Error('未设置 API 密钥：请在 ST Tasks Relay 面板填写 sk- 密钥并保存');
}
const saveLastTask = (info) => { try { localStorage.setItem(LAST_TASK_KEY, JSON.stringify(info)); } catch {} };
const loadLastTask = () => { try { return JSON.parse(localStorage.getItem(LAST_TASK_KEY) || 'null'); } catch { return null; } };

function smartSleep(ms, signal, wake = false) {
    if (ms <= 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
        if (signal?.aborted) return reject(new DOMException('aborted', 'AbortError'));
        let done = false;
        const clean = () => {
            clearTimeout(tm);
            signal?.removeEventListener('abort', onA);
            if (wake) { window.removeEventListener('online', onW); document.removeEventListener('visibilitychange', onV); }
        };
        const fin = () => { if (!done) { done = true; clean(); resolve(); } };
        const onA = () => { if (!done) { done = true; clean(); reject(new DOMException('aborted', 'AbortError')); } };
        const onW = () => fin();
        const onV = () => { if (document.visibilityState === 'visible') fin(); };
        const tm = setTimeout(fin, ms);
        signal?.addEventListener('abort', onA, { once: true });
        if (wake) { window.addEventListener('online', onW, { once: true }); document.addEventListener('visibilitychange', onV); }
    });
}

const nativeFetch = window.fetch.bind(window);
async function fetchTimeout(url, opts, ms, outerSig) {
    const ac = new AbortController();
    let timedOut = false;
    const tm = setTimeout(() => { timedOut = true; try { ac.abort(); } catch {} }, ms);
    const onA = () => { try { ac.abort(); } catch {} };
    if (outerSig) {
        if (outerSig.aborted) { clearTimeout(tm); throw new DOMException('aborted', 'AbortError'); }
        outerSig.addEventListener('abort', onA, { once: true });
    }
    try {
        return await nativeFetch(url, { ...opts, signal: ac.signal });
    } catch (e) {
        if (outerSig?.aborted) throw new DOMException('aborted', 'AbortError');
        if (timedOut) throw Object.assign(new Error(`请求超时 (${Math.round(ms / 1000)}s)`), { retryable: true });
        throw Object.assign(e instanceof Error ? e : new Error(String(e)), { retryable: true });
    } finally {
        clearTimeout(tm);
        outerSig?.removeEventListener('abort', onA);
    }
}

async function withRetry(label, fn, sig) {
    const graceMin = Math.max(1, Math.min(30, Number(getSettings().offlineGraceMinutes) || 10));
    const deadline = Date.now() + graceMin * 60000;
    let attempt = 0, lastErr = null;
    while (true) {
        if (sig?.aborted) throw new DOMException('aborted', 'AbortError');
        if (navigator.onLine === false) {
            if (Date.now() >= deadline) break;
            setStatus(`${label}：检测到断网，等待恢复…`);
            await smartSleep(5000, sig, true);
            continue;
        }
        attempt++;
        try {
            return await fn();
        } catch (err) {
            if (sig?.aborted || (err instanceof DOMException && err.name === 'AbortError')) throw new DOMException('aborted', 'AbortError');
            const st = err?.statusCode;
            if (!err?.retryable && !(st !== undefined && RETRY_STATUS.has(st))) throw err;
            lastErr = err;
            const rem = deadline - Date.now();
            if (rem <= 0) break;
            const raw = Math.min(RETRY_BASE_MS * (2 ** Math.min(attempt - 1, 4)), RETRY_MAX_MS);
            const delay = Math.min(Math.round(raw * (0.8 + Math.random() * 0.4)), rem);
            setStatus(`${label}异常(${st || '断网'})，${(delay / 1000).toFixed(1)}s 后重试（剩 ${Math.ceil(rem / 1000)}s）`);
            await smartSleep(delay, sig, true);
        }
    }
    throw Object.assign(new Error(`${label}：断网超时（${graceMin} 分钟未恢复：${lastErr?.message ?? '网络断开'}）`), { statusCode: 502 });
}

async function submitTask(up, key, body, sig) {
    const idem = uuid();
    return withRetry('提交任务', async () => {
        const r = await fetchTimeout(`${up}/v1/openai/tasks`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json', 'X-Idempotency-Key': idem },
            body: JSON.stringify(body),
        }, SUBMIT_TIMEOUT_MS, sig);
        const txt = await r.text();
        if (!r.ok) throw Object.assign(new Error(`提交失败 (${r.status}): ${txt.slice(0, 400)}`), { statusCode: r.status, retryable: RETRY_STATUS.has(r.status) });
        const d = JSON.parse(txt);
        if (!d?.id) throw new Error('未返回 task id');
        return d.id;
    }, sig);
}

async function pollOnce(up, key, tid, after, waitMs, sig) {
    const qs = waitMs > 0 ? `?after=${after}&wait=${waitMs}` : `?after=${after}`;
    return withRetry('拉取进度', async () => {
        const t0 = Date.now();
        const r = await fetchTimeout(`${up}/v1/openai/tasks/${encodeURIComponent(tid)}${qs}`, {
            method: 'GET',
            headers: { Authorization: `Bearer ${key}` },
        }, (waitMs > 0 ? waitMs : 0) + POLL_OVERHEAD_MS, sig);
        if (!r.ok) {
            const txt = await r.text().catch(() => '');
            throw Object.assign(new Error(`查询失败 (${r.status}): ${txt.slice(0, 400)}`), { statusCode: r.status, retryable: RETRY_STATUS.has(r.status) });
        }
        return { env: await r.json(), elapsedMs: Date.now() - t0 };
    }, sig);
}

async function cancelTask(up, key, tid) {
    try { await fetchTimeout(`${up}/v1/openai/tasks/${encodeURIComponent(tid)}/cancel`, { method: 'POST', headers: { Authorization: `Bearer ${key}` } }, 10000, null); } catch {}
}

function extractBody(gd) {
    const body = {};
    for (const [k, v] of Object.entries(gd)) {
        if (v === undefined || ST_INTERNAL.has(k) || VENDOR_RE.test(k)) continue;
        body[k] = v;
    }
    body.stream = Boolean(gd.stream);
    if (gd.custom_include_body) {
        try {
            const inc = typeof gd.custom_include_body === 'string' ? JSON.parse(gd.custom_include_body) : gd.custom_include_body;
            if (inc && typeof inc === 'object' && !Array.isArray(inc)) Object.assign(body, inc);
        } catch {}
    }
    if (Array.isArray(gd.custom_exclude_body)) for (const k of gd.custom_exclude_body) delete body[k];
    else if (typeof gd.custom_exclude_body === 'string') for (const k of gd.custom_exclude_body.split(/[\s,]+/).filter(Boolean)) delete body[k];
    return body;
}

function shouldIntercept(gd) {
    const s = getSettings(), oai = getOai();
    if (!s.enabled) return false;
    if (oai.chat_completion_source !== chat_completion_sources.CUSTOM) return false;
    if (!gd || typeof gd !== 'object' || !gd.model) return false;
    if (s.onlyMainChat !== false) {
        if (quietActive) return false;
        if (gd.chat_completion_source && gd.chat_completion_source !== chat_completion_sources.CUSTOM) return false;
        const mainUrl = normUrl(oai.custom_url), reqUrl = normUrl(gd.custom_url);
        if (mainUrl && reqUrl && reqUrl !== mainUrl) return false;
        const mainModel = String(oai.custom_model_id || '').trim(), reqModel = String(gd.model || '').trim();
        if (mainModel && reqModel && reqModel !== mainModel) return false;
    }
    return true;
}

function envToResp(env) {
    const st = env.status === 'failed' ? (env.status_code ?? 502) : (env.status === 'cancelled' ? 499 : 200);
    const body = env.status === 'completed' ? JSON.stringify(env.result ?? {}) : JSON.stringify(env.result ?? { error: { message: env.error ?? `任务 ${env.status}`, code: st } });
    return new Response(body, { status: st, headers: { 'Content-Type': 'application/json' } });
}

function streamResp(up, key, tid, sig) {
    const enc = new TextEncoder();
    let cancelled = false, cancelSent = false;
    const cancelOnce = () => { if (!cancelSent) { cancelSent = true; cancelTask(up, key, tid); } };
    const stream = new ReadableStream({
        async start(ctrl) {
            const deadline = Date.now() + POLL_MAX_MIN * 60000;
            let after = 0, seenDone = false, seenErr = false, lp = Boolean(getSettings().useLongPoll);
            try {
                while (Date.now() < deadline && !sig?.aborted && !cancelled) {
                    const { env, elapsedMs } = await pollOnce(up, key, tid, after, lp ? LONG_POLL_WAIT_MS : 0, sig);
                    const chs = Array.isArray(env.chunks) ? env.chunks : [];
                    for (const c of chs) {
                        if (c.includes('[DONE]')) seenDone = true;
                        if (c.includes('"error"')) seenErr = true;
                        ctrl.enqueue(enc.encode(c));
                    }
                    after = env.next_after ?? (after + chs.length);
                    setStatus(`接收中 ${tid.slice(0, 12)}（已收 ${after} 片）`);
                    if (env.done) {
                        if (env.status === 'failed' && env.error && !seenErr) ctrl.enqueue(enc.encode(`data: ${JSON.stringify({ error: { message: env.error, code: env.status_code ?? 502 } })}\n\n`));
                        if (env.status === 'cancelled' && !seenErr) ctrl.enqueue(enc.encode(`data: ${JSON.stringify({ error: { message: '任务已取消', code: 499 } })}\n\n`));
                        if (!seenDone) ctrl.enqueue(enc.encode('data: [DONE]\n\n'));
                        setStatus(`完成 ${tid.slice(0, 12)}（${env.status}，${after} 片）`);
                        break;
                    }
                    if (lp && !chs.length && elapsedMs < 300) lp = false;
                    if (!lp) await smartSleep(chs.length ? FALLBACK_BUSY_MS : FALLBACK_IDLE_MS, sig, true);
                }
                if (Date.now() >= deadline && !sig?.aborted && !cancelled) {
                    await cancelTask(up, key, tid);
                    ctrl.enqueue(enc.encode(`data: ${JSON.stringify({ error: { message: `任务超时 (${POLL_MAX_MIN} 分钟)`, code: 504 } })}\n\ndata: [DONE]\n\n`));
                }
            } catch (e) {
                if (sig?.aborted || cancelled || (e instanceof DOMException && e.name === 'AbortError')) {
                    cancelOnce(); setStatus(`已取消 ${tid.slice(0, 12)}`);
                } else {
                    setStatus(`中断：${e.message}`);
                    try { ctrl.enqueue(enc.encode(`data: ${JSON.stringify({ error: { message: e.message ?? String(e), code: e.statusCode ?? 502 } })}\n\ndata: [DONE]\n\n`)); } catch {}
                }
            } finally { try { ctrl.close(); } catch {} }
        },
        cancel() { cancelled = true; cancelOnce(); },
    });
    return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache' } });
}

async function runTask(gd, sig) {
    const up = resolveUpstream(), key = resolveApiKey(), body = extractBody(gd);
    const isCtrl = JSON.stringify(body.messages || body.prompt || '').includes('[[PG_CACHE_CTRL:');
    setStatus(`提交中 (${body.model})…`);
    const tid = await submitTask(up, key, body, sig);
    if (!isCtrl) saveLastTask({ taskId: tid, upstream: up, model: body.model, stream: body.stream, createdAt: Date.now() });
    setStatus(isCtrl ? `执行缓存指令 (${tid.slice(0, 12)})…` : `已提交 ${tid.slice(0, 12)}，等待生成…`);
    try {
        if (!body.stream) {
            const deadline = Date.now() + POLL_MAX_MIN * 60000;
            let lp = Boolean(getSettings().useLongPoll), after = 0;
            while (Date.now() < deadline) {
                if (sig?.aborted) { cancelTask(up, key, tid); throw new DOMException('aborted', 'AbortError'); }
                const { env, elapsedMs } = await pollOnce(up, key, tid, after, lp ? LONG_POLL_WAIT_MS : 0, sig);
                after = env.next_after ?? after;
                if (env.done) { setStatus(`完成 ${tid.slice(0, 12)} (${env.status})`); return envToResp(env); }
                if (lp && elapsedMs < 300) lp = false;
                if (!lp) await smartSleep(FALLBACK_IDLE_MS, sig, true);
            }
            await cancelTask(up, key, tid);
            throw Object.assign(new Error(`任务超过 ${POLL_MAX_MIN} 分钟未完成`), { statusCode: 504 });
        }
        return streamResp(up, key, tid, sig);
    } catch (e) {
        if (e instanceof DOMException && e.name === 'AbortError') { cancelTask(up, key, tid); throw e; }
        throw Object.assign(e instanceof Error ? e : new Error(String(e)), { taskSubmitted: true });
    }
}

let patched = false;
function installPatch() {
    if (patched) return;
    patched = true;
    const prev = window.fetch;
    window.fetch = async function (input, init) {
        if (!getSettings().enabled) return prev.call(this, input, init);
        const url = typeof input === 'string' ? input : (input?.url ?? '');
        const method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
        if (url.includes(GENERATE_URL) && method === 'POST') {
            let raw = init?.body;
            if (raw === undefined && input instanceof Request) raw = await input.clone().text().catch(() => '');
            if (typeof raw === 'string') {
                let gd = null; try { gd = JSON.parse(raw); } catch {}
                if (gd && shouldIntercept(gd)) {
                    try {
                        return await runTask(gd, init?.signal ?? (input instanceof Request ? input.signal : null));
                    } catch (e) {
                        if (e instanceof DOMException && e.name === 'AbortError') throw e;
                        if (e?.taskSubmitted) return new Response(JSON.stringify({ error: { message: e.message, code: e.statusCode ?? 502 } }), { status: e.statusCode ?? 502, headers: { 'Content-Type': 'application/json' } });
                        if (typeof toastr !== 'undefined') toastr.warning(`任务网关接管失败：${e.message}`, '已回退直连');
                    }
                }
            }
        }
        return prev.call(this, input, init);
    };
}

function extractText(env) {
    if (!env.stream && env.result) {
        const m = env.result?.choices?.[0]?.message;
        const r = m?.reasoning_content || m?.reasoning || '', c = m?.content || '';
        return (r ? `<think>\n${r}\n</think>\n\n` : '') + c;
    }
    let r = '', c = '';
    for (const ch of (env.chunks || [])) for (const l of String(ch).split('\n')) {
        const t = l.trim(); if (!t.startsWith('data:')) continue;
        const p = t.slice(5).trim(); if (!p || p === '[DONE]') continue;
        try {
            const d = JSON.parse(p)?.choices?.[0]?.delta;
            if (d?.reasoning_content) r += d.reasoning_content; else if (d?.reasoning) r += d.reasoning;
            if (d?.content) c += d.content;
        } catch {}
    }
    return (r ? `<think>\n${r}\n</think>\n\n` : '') + c;
}

async function testConn() {
    try {
        const up = resolveUpstream(), key = resolveApiKey();
        setStatus('测试连接中…');
        const mr = await fetchTimeout(`${up}/v1/models`, { headers: { Authorization: `Bearer ${key}` } }, 15000, null);
        if (!mr.ok) throw new Error(`HTTP ${mr.status}`);
        const d = await mr.json();
        const msg = `连接正常（可用模型 ${Array.isArray(d?.data) ? d.data.length : '?'} 个）`;
        setStatus(msg); toastr.success(msg, 'ST Tasks Relay');
    } catch (e) { setStatus(`测试失败：${e.message}`); toastr.error(e.message, '连接失败'); }
}

async function recoverLast() {
    try {
        const last = loadLastTask();
        if (!last?.taskId) return toastr.info('本地暂无近期任务记录', 'ST Tasks Relay');
        const up = last.upstream || resolveUpstream(), key = resolveApiKey();
        setStatus(`正在找回 ${last.taskId.slice(0, 12)}…`);
        const { env } = await pollOnce(up, key, last.taskId, 0, 0, null);
        const text = extractText(env);
        if (!text) {
            const tip = env.done ? `状态 ${env.status}${env.error ? `（${env.error}）` : ''}，无正文` : '服务端仍在生成中，请稍后再试';
            setStatus(tip); return toastr.warning(tip, 'ST Tasks Relay');
        }
        let copied = false; try { await navigator.clipboard.writeText(text); copied = true; } catch {}
        const tip = `已找回 ${text.length} 字${copied ? '，已复制' : ''}`;
        setStatus(tip); toastr.success(tip, 'ST Tasks Relay');
        if (!copied) window.prompt('请全选复制找回的正文：', text);
    } catch (e) { setStatus(`找回失败：${e.message}`); toastr.error(e.message, '找回失败'); }
}

async function addPanel() {
    const html = await renderExtensionTemplateAsync(resolveTemplateBase(), 'settings', {});
    $('#extensions_settings2').append(html);
    const s = getSettings();
    $('#st_tasks_relay_enabled').prop('checked', s.enabled).on('change', function () {
        s.enabled = $(this).prop('checked'); saveSettingsDebounced();
        toastr.info(s.enabled ? '已开启断网续传接管' : '已关闭接管，恢复原生直连', 'ST Tasks Relay');
    });
    const ki = $('#st_tasks_relay_key').val(s.apiKey).toggleClass('has-key', Boolean(s.apiKey?.trim()));
    ki.on('input', function () { s.apiKey = String($(this).val()); $(this).toggleClass('has-key', Boolean(s.apiKey?.trim())); });
    $('#st_tasks_relay_key_toggle').on('click', function () {
        ki.attr('type', ki.attr('type') === 'password' ? 'text' : 'password');
        $(this).toggleClass('fa-eye fa-eye-slash');
    });
    const ui = $('#st_tasks_relay_upstream').val(s.upstreamOverride || '');
    $('#st_tasks_relay_only_main').prop('checked', s.onlyMainChat !== false).on('change', function () { s.onlyMainChat = $(this).prop('checked'); saveSettingsDebounced(); });
    $('#st_tasks_relay_longpoll').prop('checked', Boolean(s.useLongPoll)).on('change', function () { s.useLongPoll = $(this).prop('checked'); saveSettingsDebounced(); });
    $('#st_tasks_relay_grace_min').val(s.offlineGraceMinutes ?? 10).on('change', function () { s.offlineGraceMinutes = Math.max(1, Math.min(30, Number($(this).val()) || 10)); $(this).val(s.offlineGraceMinutes); saveSettingsDebounced(); });

    const bindSave = (sel, fn) => $(sel).on('click', async function () {
        await fn(); await saveSettings();
        const ic = $(this); ic.addClass('fa-check').removeClass('fa-save');
        setTimeout(() => ic.addClass('fa-save').removeClass('fa-check'), 1500);
    });
    bindSave('#st_tasks_relay_key_save', () => { s.apiKey = String(ki.val()).trim(); ki.val(s.apiKey).toggleClass('has-key', Boolean(s.apiKey)); toastr.success('API 密钥已保存', 'ST Tasks Relay'); });
    bindSave('#st_tasks_relay_upstream_save', () => { s.upstreamOverride = String(ui.val()).trim(); ui.val(s.upstreamOverride); toastr.success('网关地址已保存', 'ST Tasks Relay'); });
    $('#st_tasks_relay_test').on('click', testConn);
    $('#st_tasks_relay_recover').on('click', recoverLast);
    const last = loadLastTask();
    if (last?.taskId) setStatus(`上次任务：${last.taskId.slice(0, 12)}（${last.model}）`);
}

export async function init() {
    loadSettings();
    bindGenEvents();
    installPatch();
    await addPanel();
}
