/**
 * ST Tasks Relay — SillyTavern 前端抗断网扩展 (v2.0.0)
 *
 * 基于 st-catiecli-tasks 深度重构，配合自建 OpenAI Tasks 异步中继网关（并向下兼容原版 catiecli）：
 *   1. 幂等提交（X-Idempotency-Key）：弱网重试绝不重复扣费
 *   2. 事件驱动长轮询（?after=N&wait=25000）：新分片 0ms 秒推，无新数据不空转；旧后端自动平滑降级
 *   3. 时间预算抗断网重连：默认容忍最长 10 分钟断网/切后台/换网；监听 online/visibilitychange 0ms 瞬时恢复；
 *      对网络异常及 408/429/502/503/504 网关闪断自动指数退避重试
 *   4. 修复原版 8 项 Bug：上游地址回显、非流式取消补发、已提交不再误回退直连防双倍扣费、
 *      黑名单参数透传（保留 thinking/reasoning/top_k/min_p）、Request 对象兼容、全程 [DONE]/error 追踪等
 *   5. 刷新/切后台被杀后「一键找回上次回复」：持久化 taskId，随时从服务端拉回已生成正文并复制
 */

import { extension_settings, renderExtensionTemplateAsync, getContext } from '../../../extensions.js';
import { saveSettings, saveSettingsDebounced } from '../../../../script.js';
import { chat_completion_sources } from '../../../openai.js';

const MODULE_NAME = 'st-tasks-relay';
const GENERATE_URL = '/api/backends/chat-completions/generate';
const DEFAULT_GATEWAY = 'https://relay.20261121.icu';
const LAST_TASK_STORAGE_KEY = 'st_tasks_relay_last_task_v1';

// 轮询与容错常量
const LONG_POLL_WAIT_MS = 25000;     // 长轮询服务端挂起时间
const SUBMIT_TIMEOUT_MS = 30000;     // 单次提交超时
const POLL_HTTP_OVERHEAD_MS = 15000; // 单次轮询在 wait 之外的额外网络超时缓冲
const FALLBACK_IDLE_MS = 1500;       // 兼容旧服务端（不支持长轮询）时的空闲间隔
const FALLBACK_BUSY_MS = 350;        // 兼容旧服务端时的有数据跟进间隔
const POLL_MAX_MINUTES = 30;         // 单任务最长总等待时间
const RETRY_BASE_MS = 1000;          // 退避基数
const RETRY_MAX_DELAY_MS = 10000;    // 退避上限

// 瞬时可重试 HTTP 状态码（代理闪断、CDN 超时、限流）
const RETRYABLE_HTTP_STATUS = new Set([408, 429, 502, 503, 504, 520, 521, 522, 523, 524]);

// ST 内部控制字段黑名单（并自动剥离各非 Custom 供应商专用前缀字段）
const ST_INTERNAL_FIELDS = new Set([
    'chat_completion_source',
    'type',
    'reverse_proxy',
    'proxy_password',
    'custom_url',
    'custom_include_headers',
    'custom_include_body',
    'custom_exclude_body',
    'custom_prompt_post_processing',
    'group_names',
    'use_sysprompt',
    'show_thoughts',
    'enable_web_search',
    'request_images',
    'image_inlining',
    'function_calling',
    'assistant_prefill',
    'assistant_impersonation',
    'max_context',
    'api_key_custom',
    'model_source',
    'stream_openai',
    'include_names',
    'continue_prefill',
    'continue_postfix',
]);
const ST_VENDOR_PREFIX_RE = /^(claude|vertexai|openrouter|mistral|cohere|google|makersuite|perplexity|xai|zai|ai21|groq|nanogpt|moonshot|chutes|electronhub|pollinations|aimlapi)_/i;

/** 从当前模块 URL 动态解析模板目录，避免用户修改仓库名/目录名后 settings.html 404 */
function resolveTemplateBase() {
    try {
        const m = new URL(import.meta.url).pathname.match(/\/scripts\/extensions\/(.+)\/[^/]+$/);
        if (m && m[1]) return m[1];
    } catch { /* ignore */ }
    return `third-party/${MODULE_NAME}`;
}

const defaultSettings = {
    enabled: false,
    apiKey: '',
    upstreamOverride: '',
    useLongPoll: true,
    offlineGraceMinutes: 10,
};

function getOaiSettings() {
    return getContext()?.chatCompletionSettings ?? {};
}

function loadSettings() {
    extension_settings[MODULE_NAME] = extension_settings[MODULE_NAME] || {};
    for (const key of Object.keys(defaultSettings)) {
        if (extension_settings[MODULE_NAME][key] === undefined) {
            extension_settings[MODULE_NAME][key] = defaultSettings[key];
        }
    }
}

function getSettings() {
    return extension_settings[MODULE_NAME] ?? defaultSettings;
}

function setStatusText(text) {
    const el = document.getElementById('st_tasks_relay_status');
    if (el) el.textContent = text;
}

function uuid() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
        return crypto.randomUUID();
    }
    return 'id-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 12);
}

// ==================== 地址与密钥解析 ====================

function resolveUpstream() {
    const s = getSettings();
    if (s.upstreamOverride?.trim()) {
        return s.upstreamOverride.trim().replace(/\/+$/, '').replace(/\/(v1|v1beta)$/i, '');
    }
    return DEFAULT_GATEWAY;
}

function resolveApiKey() {
    const key = getSettings().apiKey?.trim();
    if (key) return key;
    throw new Error('未设置 API 密钥：请在 ST Tasks Relay 扩展面板填写密钥并保存');
}

// ==================== 持久化最近任务（刷新/被杀找回） ====================

function saveLastTask(info) {
    try {
        localStorage.setItem(LAST_TASK_STORAGE_KEY, JSON.stringify(info));
    } catch { /* ignore quota */ }
}

function loadLastTask() {
    try {
        const raw = localStorage.getItem(LAST_TASK_STORAGE_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch {
        return null;
    }
}

// ==================== 抗断网等待与重试引擎 ====================

/** 可被用户停止、网络恢复、切回前台立即打断的 sleep */
function smartSleep(ms, signal, wakeOnReconnect = false) {
    if (ms <= 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(new DOMException('aborted', 'AbortError'));
            return;
        }
        let settled = false;
        const cleanup = () => {
            clearTimeout(timer);
            signal?.removeEventListener('abort', onAbort);
            if (wakeOnReconnect) {
                window.removeEventListener('online', onWake);
                document.removeEventListener('visibilitychange', onVis);
            }
        };
        const finish = () => {
            if (settled) return;
            settled = true;
            cleanup();
            resolve();
        };
        const onAbort = () => {
            if (settled) return;
            settled = true;
            cleanup();
            reject(new DOMException('aborted', 'AbortError'));
        };
        const onWake = () => finish();
        const onVis = () => {
            if (document.visibilityState === 'visible') finish();
        };
        const timer = setTimeout(finish, ms);
        signal?.addEventListener('abort', onAbort, { once: true });
        if (wakeOnReconnect) {
            window.addEventListener('online', onWake, { once: true });
            document.addEventListener('visibilitychange', onVis);
        }
    });
}

/** 带独立超时的原生 fetch，不受自身 patch 影响 */
const nativeFetch = window.fetch.bind(window);

async function fetchWithTimeout(url, options, timeoutMs, outerSignal) {
    const ac = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => {
        timedOut = true;
        try { ac.abort(); } catch { /* ignore */ }
    }, timeoutMs);
    const onOuterAbort = () => {
        try { ac.abort(); } catch { /* ignore */ }
    };
    if (outerSignal) {
        if (outerSignal.aborted) {
            clearTimeout(timer);
            throw new DOMException('aborted', 'AbortError');
        }
        outerSignal.addEventListener('abort', onOuterAbort, { once: true });
    }
    try {
        return await nativeFetch(url, { ...options, signal: ac.signal });
    } catch (err) {
        if (outerSignal?.aborted) throw new DOMException('aborted', 'AbortError');
        if (timedOut) {
            throw Object.assign(new Error(`请求超时 (${Math.round(timeoutMs / 1000)}s)`), { retryable: true });
        }
        throw Object.assign(err instanceof Error ? err : new Error(String(err)), { retryable: true });
    } finally {
        clearTimeout(timer);
        outerSignal?.removeEventListener('abort', onOuterAbort);
    }
}

/**
 * 时间预算抗断网重试：
 *   - 连续不可达期间按指数退避 + 抖动重试，最长持续 offlineGraceMinutes 分钟
 *   - 检测到浏览器离线时挂起等待 online / visibilitychange，不空烧重试
 *   - 遇到 4xx 非重试错误立即抛出；502/503/504/429/408 与网络断连自动重试
 */
async function withResilientRetry(label, fn, outerSignal) {
    const graceMin = Math.max(1, Math.min(30, Number(getSettings().offlineGraceMinutes) || 10));
    const retryDeadline = Date.now() + graceMin * 60 * 1000;
    let attempt = 0;
    let lastErr = null;

    while (true) {
        if (outerSignal?.aborted) throw new DOMException('aborted', 'AbortError');

        // 明确离线时：先等待联网信号（每 5s 探一次防某些 WebView 的 navigator.onLine 滞后）
        if (typeof navigator !== 'undefined' && navigator.onLine === false) {
            if (Date.now() >= retryDeadline) break;
            setStatusText(`${label}：检测到断网，等待恢复连接…`);
            await smartSleep(5000, outerSignal, true);
            continue;
        }

        attempt++;
        try {
            return await fn();
        } catch (err) {
            if (outerSignal?.aborted || (err instanceof DOMException && err.name === 'AbortError')) {
                throw new DOMException('aborted', 'AbortError');
            }
            const status = err?.statusCode;
            const retryable = Boolean(err?.retryable) || (status !== undefined && RETRYABLE_HTTP_STATUS.has(status));
            if (!retryable) throw err;

            lastErr = err;
            const remaining = retryDeadline - Date.now();
            if (remaining <= 0) break;

            const rawDelay = Math.min(RETRY_BASE_MS * (2 ** Math.min(attempt - 1, 4)), RETRY_MAX_DELAY_MS);
            const jitter = rawDelay * (0.8 + Math.random() * 0.4);
            const delay = Math.min(Math.round(jitter), remaining);
            const remainSec = Math.ceil(remaining / 1000);

            setStatusText(`${label}异常(${status || '网络断开'})，${(delay / 1000).toFixed(1)}s 后重试（剩余容忍 ${remainSec}s）`);
            console.warn(`[st-tasks-relay] ${label} 第 ${attempt} 次重试 (${delay}ms):`, err.message);
            await smartSleep(delay, outerSignal, true);
        }
    }

    throw Object.assign(
        new Error(`${label}：断网超时（已容忍 ${graceMin} 分钟仍无法恢复，最后错误：${lastErr?.message ?? '网络断开'}）`),
        { statusCode: 502 },
    );
}

// ==================== 任务 API 客户端 ====================

async function submitTask(upstream, apiKey, body, outerSignal) {
    const idempotencyKey = uuid();
    return withResilientRetry('提交任务', async () => {
        const resp = await fetchWithTimeout(
            `${upstream}/v1/openai/tasks`,
            {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type': 'application/json',
                    'X-Idempotency-Key': idempotencyKey,
                },
                body: JSON.stringify(body),
            },
            SUBMIT_TIMEOUT_MS,
            outerSignal,
        );
        const text = await resp.text();
        if (!resp.ok) {
            throw Object.assign(
                new Error(`任务提交失败 (${resp.status}): ${text.slice(0, 500)}`),
                { statusCode: resp.status, retryable: RETRYABLE_HTTP_STATUS.has(resp.status) },
            );
        }
        const data = JSON.parse(text);
        if (!data?.id) throw new Error('任务提交未返回有效 task id');
        return data.id;
    }, outerSignal);
}

async function pollTaskOnce(upstream, apiKey, taskId, after, waitMs, outerSignal) {
    const qs = waitMs > 0 ? `?after=${after}&wait=${waitMs}` : `?after=${after}`;
    const timeoutMs = (waitMs > 0 ? waitMs : 0) + POLL_HTTP_OVERHEAD_MS;

    return withResilientRetry('拉取进度', async () => {
        const started = Date.now();
        const resp = await fetchWithTimeout(
            `${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}${qs}`,
            {
                method: 'GET',
                headers: { 'Authorization': `Bearer ${apiKey}` },
            },
            timeoutMs,
            outerSignal,
        );
        if (!resp.ok) {
            const text = await resp.text().catch(() => '');
            throw Object.assign(
                new Error(`任务查询失败 (${resp.status}): ${text.slice(0, 500)}`),
                { statusCode: resp.status, retryable: RETRYABLE_HTTP_STATUS.has(resp.status) },
            );
        }
        const env = await resp.json();
        return { env, elapsedMs: Date.now() - started };
    }, outerSignal);
}

async function cancelTask(upstream, apiKey, taskId) {
    try {
        await fetchWithTimeout(
            `${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}/cancel`,
            {
                method: 'POST',
                headers: { 'Authorization': `Bearer ${apiKey}` },
            },
            10000,
            null,
        );
    } catch (err) {
        console.warn(`[st-tasks-relay] 取消任务 ${taskId} 失败:`, err.message);
    }
}

// ==================== 请求体提取（黑名单透明透传） ====================

function parseMaybeJsonObj(val) {
    if (!val) return null;
    if (typeof val === 'object' && !Array.isArray(val)) return val;
    if (typeof val === 'string') {
        try {
            const parsed = JSON.parse(val);
            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed;
        } catch { /* ignore YAML/invalid */ }
    }
    return null;
}

function extractOpenAIBody(generateData) {
    const body = {};
    for (const [k, v] of Object.entries(generateData)) {
        if (v === undefined || ST_INTERNAL_FIELDS.has(k) || ST_VENDOR_PREFIX_RE.test(k)) continue;
        body[k] = v;
    }
    body.stream = Boolean(generateData.stream);

    // 尽量尊重 ST 的 custom_include_body / custom_exclude_body（JSON 格式时）
    const inc = parseMaybeJsonObj(generateData.custom_include_body);
    if (inc) Object.assign(body, inc);

    if (Array.isArray(generateData.custom_exclude_body)) {
        for (const k of generateData.custom_exclude_body) delete body[k];
    } else if (typeof generateData.custom_exclude_body === 'string') {
        for (const k of generateData.custom_exclude_body.split(/[\s,]+/).filter(Boolean)) {
            delete body[k];
        }
    }
    return body;
}

function shouldIntercept(generateData) {
    const s = getSettings();
    if (!s.enabled) return false;
    if (getOaiSettings().chat_completion_source !== chat_completion_sources.CUSTOM) return false;
    if (!generateData || typeof generateData !== 'object' || !generateData.model) return false;
    return true;
}

// ==================== 响应封装（流式 / 非流式） ====================

function envelopeToResponse(env) {
    const status = env.status === 'failed'
        ? (env.status_code ?? 502)
        : (env.status === 'cancelled' ? 499 : 200);
    const body = env.status === 'completed'
        ? JSON.stringify(env.result ?? {})
        : JSON.stringify(env.result ?? { error: { message: env.error ?? `任务 ${env.status}`, code: status } });
    return new Response(body, {
        status,
        headers: { 'Content-Type': 'application/json' },
    });
}

function streamTaskResponse(upstream, apiKey, taskId, signal) {
    const encoder = new TextEncoder();
    const useLongPoll = Boolean(getSettings().useLongPoll);
    let cancelledByReader = false;
    let cancelSent = false;
    const cancelOnce = () => {
        if (cancelSent) return;
        cancelSent = true;
        cancelTask(upstream, apiKey, taskId);
    };

    const stream = new ReadableStream({
        async start(controller) {
            const deadline = Date.now() + POLL_MAX_MINUTES * 60 * 1000;
            let after = 0;
            let seenDone = false;
            let seenError = false;
            let serverSupportsLongPoll = useLongPoll;

            try {
                while (Date.now() < deadline) {
                    if (signal?.aborted || cancelledByReader) break;

                    const waitMs = serverSupportsLongPoll ? LONG_POLL_WAIT_MS : 0;
                    const { env, elapsedMs } = await pollTaskOnce(upstream, apiKey, taskId, after, waitMs, signal);

                    const chunks = Array.isArray(env.chunks) ? env.chunks : [];
                    for (const chunk of chunks) {
                        if (chunk.includes('[DONE]')) seenDone = true;
                        if (chunk.includes('"error"')) seenError = true;
                        controller.enqueue(encoder.encode(chunk));
                    }
                    after = env.next_after ?? (after + chunks.length);
                    setStatusText(`接收中 ${taskId.slice(0, 12)}（已收 ${after} 片）`);

                    if (env.done) {
                        if (env.status === 'failed' && env.error && !seenError) {
                            controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: env.error, code: env.status_code ?? 502 } })}\n\n`));
                            seenError = true;
                        }
                        if (env.status === 'cancelled' && !seenError) {
                            controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: '任务已取消', code: 499 } })}\n\n`));
                            seenError = true;
                        }
                        if (!seenDone) {
                            controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                        }
                        setStatusText(`完成 ${taskId.slice(0, 12)}（${env.status}，共 ${after} 片）`);
                        break;
                    }

                    // 自动识别旧版短轮询后端：请求带 wait=25000 却在 <300ms 内空返回，平滑降级间隔防空转
                    if (serverSupportsLongPoll && chunks.length === 0 && elapsedMs < 300) {
                        serverSupportsLongPoll = false;
                        console.info('[st-tasks-relay] 检测到上游不支持长轮询，已自动平滑降级为智能间隔轮询');
                    }

                    if (!serverSupportsLongPoll) {
                        const delay = chunks.length > 0 ? FALLBACK_BUSY_MS : FALLBACK_IDLE_MS;
                        await smartSleep(delay, signal, true);
                    }
                }

                if (Date.now() >= deadline && !signal?.aborted && !cancelledByReader) {
                    await cancelTask(upstream, apiKey, taskId);
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: `任务 ${taskId} 超时 (${POLL_MAX_MINUTES} 分钟)`, code: 504 } })}\n\n`));
                    controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                    setStatusText(`超时取消 ${taskId.slice(0, 12)}`);
                }
            } catch (err) {
                if (signal?.aborted || cancelledByReader || (err instanceof DOMException && err.name === 'AbortError')) {
                    cancelOnce();
                    setStatusText(`已取消 ${taskId.slice(0, 12)}`);
                } else {
                    setStatusText(`中断：${err.message}`);
                    try {
                        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: err.message ?? String(err), code: err.statusCode ?? 502 } })}\n\n`));
                        controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                    } catch { /* controller closed */ }
                }
            } finally {
                try { controller.close(); } catch { /* already closed */ }
            }
        },
        cancel() {
            cancelledByReader = true;
            cancelOnce();
        },
    });

    return new Response(stream, {
        status: 200,
        headers: {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache',
        },
    });
}

async function runTaskAsResponse(generateData, signal) {
    const upstream = resolveUpstream();
    const apiKey = resolveApiKey();
    const requestBody = extractOpenAIBody(generateData);

    setStatusText(`提交任务中 (${requestBody.model})…`);
    const rawPreview = JSON.stringify(requestBody.messages || requestBody.prompt || '');
    const isCtrlPacket = rawPreview.includes('[[PG_CACHE_CTRL:');
    const taskId = await submitTask(upstream, apiKey, requestBody, signal);

    // 仅普通生成任务落盘，防止 GG跳蛋缓存 3.2 的 probe/replay 控制包覆盖上次真实任务记录
    if (!isCtrlPacket) {
        saveLastTask({
            taskId,
            upstream,
            model: requestBody.model,
            stream: requestBody.stream,
            createdAt: Date.now(),
        });
    }
    setStatusText(isCtrlPacket ? `执行缓存控制指令 (${taskId.slice(0, 12)})…` : `已提交 ${taskId.slice(0, 12)}，等待服务端生成…`);
    console.info(`[st-tasks-relay] 任务已提交: ${taskId} (stream=${requestBody.stream})`);

    // 从此刻起任务已在服务端启动，任何后续异常都标记 submitted=true，外层绝不再回退原生直连
    try {
        if (!requestBody.stream) {
            const deadline = Date.now() + POLL_MAX_MINUTES * 60 * 1000;
            const useLongPoll = Boolean(getSettings().useLongPoll);
            let serverSupportsLongPoll = useLongPoll;
            let after = 0;

            while (Date.now() < deadline) {
                if (signal?.aborted) {
                    cancelTask(upstream, apiKey, taskId);
                    throw new DOMException('aborted', 'AbortError');
                }
                const waitMs = serverSupportsLongPoll ? LONG_POLL_WAIT_MS : 0;
                const { env, elapsedMs } = await pollTaskOnce(upstream, apiKey, taskId, after, waitMs, signal);
                after = env.next_after ?? after;
                if (env.done) {
                    setStatusText(`完成 ${taskId.slice(0, 12)} (${env.status})`);
                    return envelopeToResponse(env);
                }
                if (serverSupportsLongPoll && elapsedMs < 300) {
                    serverSupportsLongPoll = false;
                }
                if (!serverSupportsLongPoll) {
                    await smartSleep(FALLBACK_IDLE_MS, signal, true);
                }
            }
            await cancelTask(upstream, apiKey, taskId);
            throw Object.assign(new Error(`任务 ${taskId} 超过 ${POLL_MAX_MINUTES} 分钟未完成`), { statusCode: 504 });
        }

        return streamTaskResponse(upstream, apiKey, taskId, signal);
    } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') {
            cancelTask(upstream, apiKey, taskId);
            throw err;
        }
        throw Object.assign(err instanceof Error ? err : new Error(String(err)), { taskSubmitted: true });
    }
}

// ==================== 无侵入式 fetch 拦截 ====================

let patchInstalled = false;

async function readRequestMeta(input, init) {
    const url = typeof input === 'string' ? input : (input?.url ?? '');
    const method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
    if (!url.includes(GENERATE_URL) || method !== 'POST') return null;

    let rawBody = init?.body;
    if (rawBody === undefined && input instanceof Request) {
        rawBody = await input.clone().text();
    }
    if (typeof rawBody !== 'string') return null;
    const signal = init?.signal ?? (input instanceof Request ? input.signal : null);
    return { rawBody, signal };
}

function installFetchPatchOnce() {
    if (patchInstalled) return;
    patchInstalled = true;
    const prevFetch = window.fetch;

    window.fetch = async function (input, init) {
        // 动态检查开关：关闭时零开销直接透传，避免卸载 patch 破坏后装插件的 fetch 链
        if (!getSettings().enabled) {
            return prevFetch.call(this, input, init);
        }
        let meta = null;
        try {
            meta = await readRequestMeta(input, init);
        } catch { /* ignore */ }

        if (meta) {
            let generateData = null;
            try {
                generateData = JSON.parse(meta.rawBody);
            } catch { /* ignore non-JSON */ }

            if (generateData && shouldIntercept(generateData)) {
                try {
                    return await runTaskAsResponse(generateData, meta.signal ?? null);
                } catch (err) {
                    if (err instanceof DOMException && err.name === 'AbortError') throw err;
                    // 已成功提交到任务网关后绝不回退直连，防止重复扣费！直接返回错误响应给 ST
                    if (err?.taskSubmitted) {
                        const status = err.statusCode ?? 502;
                        return new Response(
                            JSON.stringify({ error: { message: err.message, code: status } }),
                            { status, headers: { 'Content-Type': 'application/json' } },
                        );
                    }
                    // 仅配置缺失等提交前错误才回退原生直连
                    console.error('[st-tasks-relay] 接管前置校验失败，回退原生请求:', err);
                    if (typeof toastr !== 'undefined') {
                        toastr.warning(`任务网关接管失败：${err.message}`, '已回退直连模式');
                    }
                }
            }
        }
        return prevFetch.call(this, input, init);
    };
    console.info('[st-tasks-relay] fetch 拦截器已就绪');
}

// ==================== 辅助功能：测试连接 & 找回上次回复 ====================

function extractTextFromEnvelope(env) {
    if (!env.stream && env.result) {
        const msg = env.result?.choices?.[0]?.message;
        const reasoning = msg?.reasoning_content || msg?.reasoning || '';
        const content = msg?.content || '';
        return (reasoning ? `<think>\n${reasoning}\n</think>\n\n` : '') + content;
    }
    let reasoning = '';
    let content = '';
    for (const chunk of (env.chunks || [])) {
        for (const line of String(chunk).split('\n')) {
            const trimmed = line.trim();
            if (!trimmed.startsWith('data:')) continue;
            const payload = trimmed.slice(5).trim();
            if (!payload || payload === '[DONE]') continue;
            try {
                const obj = JSON.parse(payload);
                const delta = obj?.choices?.[0]?.delta;
                if (delta?.reasoning_content) reasoning += delta.reasoning_content;
                else if (delta?.reasoning) reasoning += delta.reasoning;
                if (delta?.content) content += delta.content;
            } catch { /* ignore */ }
        }
    }
    return (reasoning ? `<think>\n${reasoning}\n</think>\n\n` : '') + content;
}

async function testGatewayConnection() {
    try {
        const upstream = resolveUpstream();
        const apiKey = resolveApiKey();
        setStatusText('正在测试网关连接…');

        let healthInfo = '';
        try {
            const hr = await fetchWithTimeout(`${upstream}/health`, { method: 'GET' }, 8000, null);
            if (hr.ok) {
                const h = await hr.json();
                healthInfo = `运行中 ${h.running_tasks ?? 0} / 缓存 ${h.cached_tasks ?? 0}`;
            }
        } catch { /* 原版上游可能无 /health */ }

        const mr = await fetchWithTimeout(
            `${upstream}/v1/models`,
            { headers: { 'Authorization': `Bearer ${apiKey}` } },
            15000,
            null,
        );
        if (!mr.ok) {
            const t = await mr.text().catch(() => '');
            throw new Error(`HTTP ${mr.status} ${t.slice(0, 200)}`);
        }
        const mdata = await mr.json();
        const count = Array.isArray(mdata?.data) ? mdata.data.length : '?';
        const msg = `连接正常（模型 ${count} 个${healthInfo ? `，${healthInfo}` : ''}）`;
        setStatusText(msg);
        toastr.success(msg, 'ST Tasks Relay');
    } catch (err) {
        setStatusText(`测试失败：${err.message}`);
        toastr.error(err.message, 'ST Tasks Relay 连接失败');
    }
}

async function recoverLastTask() {
    try {
        const last = loadLastTask();
        if (!last?.taskId) {
            toastr.info('本地暂无可找回的近期任务记录', 'ST Tasks Relay');
            return;
        }
        const upstream = last.upstream || resolveUpstream();
        const apiKey = resolveApiKey();
        setStatusText(`正在从服务端找回 ${last.taskId.slice(0, 12)}…`);

        const { env } = await pollTaskOnce(upstream, apiKey, last.taskId, 0, 0, null);
        const text = extractTextFromEnvelope(env);
        if (!text) {
            const tip = env.done
                ? `任务状态为 ${env.status}${env.error ? `（${env.error}）` : ''}，未提取到正文`
                : '任务仍在服务端生成中，请稍后再试';
            setStatusText(tip);
            toastr.warning(tip, 'ST Tasks Relay');
            return;
        }

        let copied = false;
        try {
            await navigator.clipboard.writeText(text);
            copied = true;
        } catch { /* 某些 HTTP 页面无剪贴板权限 */ }

        const statusTip = `已找回 ${text.length} 字（任务状态：${env.status}）${copied ? '，已复制到剪贴板' : ''}`;
        setStatusText(statusTip);
        toastr.success(statusTip, 'ST Tasks Relay');
        if (!copied) {
            window.prompt('剪贴板受限，请手动全选复制找回的正文：', text);
        }
    } catch (err) {
        setStatusText(`找回失败：${err.message}`);
        toastr.error(err.message, 'ST Tasks Relay 找回失败');
    }
}

// ==================== 设置面板绑定 ====================

async function addSettingsPanel() {
    const html = await renderExtensionTemplateAsync(resolveTemplateBase(), 'settings', {});
    $('#extensions_settings2').append(html);

    const s = getSettings();

    $('#st_tasks_relay_enabled').prop('checked', s.enabled).on('change', function () {
        s.enabled = $(this).prop('checked');
        saveSettingsDebounced();
        toastr.info(s.enabled ? '已开启断网续传任务接管' : '已关闭接管，恢复原生直连', 'ST Tasks Relay');
    });

    // API 密钥回显与切换可见
    const keyInput = $('#st_tasks_relay_key');
    keyInput.val(s.apiKey).toggleClass('has-key', Boolean(s.apiKey?.trim()));
    keyInput.on('input', function () {
        s.apiKey = String($(this).val());
        $(this).toggleClass('has-key', Boolean(s.apiKey?.trim()));
    });
    $('#st_tasks_relay_key_toggle').on('click', function () {
        const isPwd = keyInput.attr('type') === 'password';
        keyInput.attr('type', isPwd ? 'text' : 'password');
        $(this).toggleClass('fa-eye fa-eye-slash');
    });

    // 修复原版 Bug：回显已保存的上游网关地址！
    const upstreamInput = $('#st_tasks_relay_upstream');
    upstreamInput.val(s.upstreamOverride || '');

    // 高级选项回显与绑定
    $('#st_tasks_relay_longpoll').prop('checked', Boolean(s.useLongPoll)).on('change', function () {
        s.useLongPoll = $(this).prop('checked');
        saveSettingsDebounced();
    });
    $('#st_tasks_relay_grace_min').val(s.offlineGraceMinutes ?? 10).on('change', function () {
        s.offlineGraceMinutes = Math.max(1, Math.min(30, Number($(this).val()) || 10));
        $(this).val(s.offlineGraceMinutes);
        saveSettingsDebounced();
    });

    function bindSaveIcon(selector, onSave) {
        $(selector).on('click', async function () {
            await onSave();
            await saveSettings();
            const icon = $(this);
            icon.addClass('fa-check').removeClass('fa-save');
            setTimeout(() => icon.addClass('fa-save').removeClass('fa-check'), 1500);
        });
    }

    bindSaveIcon('#st_tasks_relay_key_save', async () => {
        s.apiKey = String(keyInput.val()).trim();
        keyInput.val(s.apiKey).toggleClass('has-key', Boolean(s.apiKey));
        toastr.success('API 密钥已保存', 'ST Tasks Relay');
    });

    bindSaveIcon('#st_tasks_relay_upstream_save', async () => {
        s.upstreamOverride = String(upstreamInput.val()).trim();
        upstreamInput.val(s.upstreamOverride);
        toastr.success('任务网关地址已保存', 'ST Tasks Relay');
    });

    $('#st_tasks_relay_test').on('click', () => testGatewayConnection());
    $('#st_tasks_relay_recover').on('click', () => recoverLastTask());

    const last = loadLastTask();
    if (last?.taskId) {
        const ago = Math.max(0, Math.round((Date.now() - last.createdAt) / 60000));
        setStatusText(`上次任务：${last.taskId.slice(0, 12)}（${last.model}，${ago} 分钟前）`);
    }
}

export async function init() {
    loadSettings();
    installFetchPatchOnce();
    await addSettingsPanel();
    console.info('[st-tasks-relay] v2.0.0 已加载');
}
