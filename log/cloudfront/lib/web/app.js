/* Connect AI Agent 日志排查前端逻辑
 *
 * 数据来源: window.__CONNECT_AI_LOG_DATA__ = [{timestamp, message}]
 * message 是 AI Agent 写入 CloudWatch 的原始 JSON 字符串。
 *
 * 关键映射:
 *   - Amazon Connect 的 contactId 在日志里体现为 `session_name`
 *     (也出现在 span 的 session_name=...)。
 *   - `session_id` 是底层 Q in Connect 的内部会话 ID。
 *   一个 contactId 可能对应一个或多个 session_id，这里以 contactId 为主键分组。
 */
(function () {
  "use strict";

  const RAW = window.__CONNECT_AI_LOG_DATA__ || [];

  // ---- 界面语言 (UI i18n) ----
  const I18N = window.I18N || {};
  const I18N_LANGS = window.I18N_LANGS || ["中文"];
  const DEFAULT_LANG = "中文";

  // 把 config.env 里可能出现的语言代码/别名归一化为字典里的规范名称
  const LANG_ALIASES = {
    "zh": "中文", "zh-cn": "中文", "zh-hans": "中文", "chinese": "中文", "中文": "中文",
    "en": "English", "en-us": "English", "english": "English", "英语": "English", "英文": "English",
    "es": "Español", "spanish": "Español", "espanol": "Español", "español": "Español", "西班牙语": "Español",
    "it": "Italiano", "italian": "Italiano", "italiano": "Italiano", "意大利语": "Italiano",
    "de": "Deutsch", "german": "Deutsch", "deutsch": "Deutsch", "德语": "Deutsch", "德文": "Deutsch",
  };
  function normalizeLang(v) {
    if (!v) return null;
    const s = String(v).trim();
    if (I18N[s]) return s;                    // 已经是规范名称
    return LANG_ALIASES[s.toLowerCase()] || null;
  }

  let currentLang = DEFAULT_LANG;
  // 1) config.env 注入的默认界面语言(site-config.js -> window.__UI_CONFIG__)
  try {
    const cfgLang = window.__UI_CONFIG__ && window.__UI_CONFIG__.defaultLang;
    const n = normalizeLang(cfgLang);
    if (n) currentLang = n;
  } catch (e) { /* 无注入配置时用默认值 */ }
  // 2) 用户上次在页面上手动选择的语言优先于配置默认值
  try {
    const saved = localStorage.getItem("connectAiUiLang");
    if (saved && I18N[saved]) currentLang = saved;
  } catch (e) { /* localStorage 不可用时忽略 */ }

  // 翻译单个 key，可传入 {占位符: 值} 做替换。缺失时回退到中文、再回退到 key 本身。
  function t(key, params) {
    const dict = I18N[currentLang] || I18N[DEFAULT_LANG] || {};
    let s = dict[key];
    if (s == null && I18N[DEFAULT_LANG]) s = I18N[DEFAULT_LANG][key];
    if (s == null) s = key;
    if (params) {
      for (const k in params) {
        s = s.split("{" + k + "}").join(String(params[k]));
      }
    }
    return s;
  }

  // 把带 data-i18n / data-i18n-ph 属性的静态元素替换为当前语言文本
  function applyStaticI18n() {
    document.querySelectorAll("[data-i18n]").forEach((el) => {
      el.textContent = t(el.getAttribute("data-i18n"));
    });
    document.querySelectorAll("[data-i18n-ph]").forEach((el) => {
      el.setAttribute("placeholder", t(el.getAttribute("data-i18n-ph")));
    });
    document.documentElement.setAttribute("lang", currentLang);
  }

  // ---- 按需翻译状态 ----
  let TRANSLATE_CFG = { available: false, lang: "中文" };
  const translatedContacts = new Set();   // 已翻译过的 Contact
  const translatingContacts = new Set();  // 正在翻译中的 Contact

  // ---- 工具: 安全 JSON 解析 ----
  function tryParse(s) {
    if (typeof s !== "string") return s;
    try { return JSON.parse(s); } catch (e) { return null; }
  }
  // ---- 本地 /api 后端可用性与统一请求封装 ----
  // state: "unknown" | "online" | "offline"。仅针对本地 ./api/*(serve.py);
  // CloudFront 部署不探测、不使用本地 API，状态永远保持 "unknown"。
  const API = { state: "unknown", reason: "", probe: null, uiApplied: false };

  function isCloudFrontMode() {
    return !!window.__CF_TRANSLATE_CFG__ || typeof window.__CONNECT_DESCRIBE_CONTACT__ === "function";
  }

  // 永不 reject 的 fetch 封装: 网络错误 / 超时 / 非 2xx / 非 JSON / JSON 解析失败都归一成结果对象。
  // 返回 { ok, status, data, error, kind }; kind 为 "" | "network" | "timeout" | "http" | "non-json"
  async function apiFetch(url, opts) {
    opts = opts || {};
    const res = { ok: false, status: 0, data: null, error: "", kind: "" };
    let timer = null;
    let timedOut = false;
    try {
      const init = { method: opts.method || "GET" };
      if (opts.headers) init.headers = opts.headers;
      if (opts.body != null) init.body = opts.body;
      if (typeof AbortController === "function") {
        const ctrl = new AbortController();
        init.signal = ctrl.signal;
        timer = setTimeout(() => { timedOut = true; ctrl.abort(); }, opts.timeoutMs || 10000);
      }
      const resp = await fetch(url, init);
      res.status = resp.status;
      const text = await resp.text();
      const ctype = (resp.headers && resp.headers.get("content-type")) || "";
      const trimmed = text.trim();
      let parsed = null;
      let isJson = false;
      if (/json/i.test(ctype) || trimmed.charAt(0) === "{" || trimmed.charAt(0) === "[") {
        try { parsed = JSON.parse(trimmed); isJson = true; } catch (e) { isJson = false; }
      }
      if (!isJson) {
        res.kind = "non-json";
        res.error = resp.ok ? t("apiErrNonJson") : "HTTP " + resp.status;
        return res;
      }
      res.data = parsed;
      if (resp.ok) {
        res.ok = true;
      } else {
        res.kind = "http";
        res.error = String((parsed && parsed.error) || ("HTTP " + resp.status));
      }
      return res;
    } catch (err) {
      res.kind = timedOut ? "timeout" : "network";
      res.error = timedOut ? t("apiErrTimeout") : t("apiErrNetwork");
      return res;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  // 失败是否意味着"后端根本不存在/不可达"(而不是存活服务器返回的业务错误)
  function isApiUnavailable(res) {
    if (!res || res.ok) return false;
    if (res.kind === "network" || res.kind === "timeout" || res.kind === "non-json") return true;
    return [404, 405, 501, 502, 503, 504].indexOf(res.status) >= 0;
  }

  // 非阻塞提示(替代 alert): 同一时刻只保留一条, 6 秒后自动消失, 点击可关闭
  let noticeTimer = null;
  function showNotice(msg, kind) {
    let el = document.getElementById("appNotice");
    if (!el) {
      el = document.createElement("div");
      el.id = "appNotice";
      el.setAttribute("role", "status");
      el.setAttribute("aria-live", "polite");
      el.addEventListener("click", () => el.remove());
      document.body.appendChild(el);
    }
    el.className = "app-notice " + (kind === "err" ? "err" : "warn");
    el.textContent = msg;
    clearTimeout(noticeTimer);
    noticeTimer = setTimeout(() => {
      const cur = document.getElementById("appNotice");
      if (cur) cur.remove();
    }, 6000);
  }

  // 离线模式下的静态 UI: 顶部徽标 + 禁用「刷新」+ 隐藏「加载全部摘要」。幂等, 可在切换语言时重复调用。
  function refreshOfflineUi() {
    const off = API.state === "offline" && !isCloudFrontMode();
    let badge = document.getElementById("offlineBadge");
    const loadAll = document.getElementById("btnLoadAllSummaries");
    if (off) {
      if (!badge) {
        badge = document.createElement("span");
        badge.id = "offlineBadge";
        badge.className = "offline-badge";
        badge.setAttribute("tabindex", "0");
        const meta = document.getElementById("globalMeta");
        if (meta && meta.parentNode) meta.parentNode.insertBefore(badge, meta.nextSibling);
        else document.body.insertBefore(badge, document.body.firstChild);
      }
      badge.textContent = t("offlineBadge");
      badge.title = t("offlineBadgeTitle");
      badge.setAttribute("aria-label", t("offlineBadge") + ": " + t("offlineBadgeTitle"));
      if (cdRefreshBtn) { cdRefreshBtn.disabled = true; cdRefreshBtn.title = t("offlineFeatureTitle"); }
      if (loadAll) loadAll.style.display = "none";
      API.uiApplied = true;
    } else if (API.uiApplied) {
      if (badge) badge.remove();
      if (cdRefreshBtn) { cdRefreshBtn.disabled = false; cdRefreshBtn.removeAttribute("title"); }
      if (loadAll) loadAll.style.display = "";
      API.uiApplied = false;
    }
  }

  // 标记本地 API 不可用(幂等, 只会从非 offline 迁移一次, 之后不再产生任何请求)。
  //   opts.initial: 首屏同步检测, 随后 init() 会渲染, 这里不重绘
  //   opts.notify:  由按需操作(翻译)的失败触发时弹出非阻塞提示
  function setApiOffline(reason, opts) {
    opts = opts || {};
    if (isCloudFrontMode() || API.state === "offline") return;
    API.state = "offline";
    API.reason = reason || "";
    TRANSLATE_CFG.available = false;
    refreshOfflineUi();
    if (!opts.initial) {
      renderSidebar(searchBox.value);
      renderDashboard();
      if (cdContactId) updateCdTranslateBtn(cdContactId);
    }
    if (opts.notify) showNotice(t("translateOffline"), "warn");
    try { console.info("[offline] /api unavailable (" + (reason || "unknown") + "), running read-only"); } catch (e) { /* ignore */ }
  }

  // 探测本地 API 是否存在(只做一次, 结果缓存在 API.probe / API.state)。
  // 优先级: CloudFront 模式跳过 > 构建脚本写入的 window.__API_AVAILABLE__ === false > file:// > 探测 ./api/config
  function detectLocalApi() {
    if (isCloudFrontMode()) return Promise.resolve(false);
    if (API.probe) return API.probe;
    if (window.__API_AVAILABLE__ === false || location.protocol === "file:") {
      setApiOffline(window.__API_AVAILABLE__ === false ? "flag" : "file", { initial: true });
      API.probe = Promise.resolve(false);
      return API.probe;
    }
    API.probe = apiFetch("./api/config", { timeoutMs: 2500 }).then((res) => {
      if (res.ok && res.data && typeof res.data === "object" && !Array.isArray(res.data)) {
        API.state = "online";
        TRANSLATE_CFG = { available: !!res.data.available, lang: res.data.lang || "中文", apiUrl: "" };
        return true;
      }
      if (isApiUnavailable(res)) {
        setApiOffline(res.kind === "http" ? "http-" + res.status : res.kind);
        return false;
      }
      // 后端存在但 /api/config 报业务错误: 视为在线, 翻译按钮保持隐藏(与原行为一致)
      API.state = "online";
      return true;
    }).catch(() => false);
    return API.probe;
  }

  // ---- 解析 span 字符串 "{k=v, k=v, nested=[...]}" ----
  // 这是 OpenTelemetry 风格的非 JSON 文本，做一个容错的浅解析。
  function parseSpan(spanStr) {
    const out = {};
    if (typeof spanStr !== "string") return out;
    let s = spanStr.trim();
    if (s.startsWith("{")) s = s.slice(1);
    if (s.endsWith("}")) s = s.slice(0, -1);
    // 顶层按逗号切分，但要跳过中括号 / 大括号内部的逗号
    const parts = [];
    let depth = 0, cur = "";
    for (let i = 0; i < s.length; i++) {
      const c = s[i];
      if (c === "[" || c === "{") depth++;
      else if (c === "]" || c === "}") depth--;
      if (c === "," && depth === 0) { parts.push(cur); cur = ""; }
      else cur += c;
    }
    if (cur.trim()) parts.push(cur);
    for (const p of parts) {
      const eq = p.indexOf("=");
      if (eq === -1) continue;
      const k = p.slice(0, eq).trim();
      const v = p.slice(eq + 1).trim();
      out[k] = v;
    }
    return out;
  }

  const SHORT_TYPE = (t) => (t || "").replace(/^TRANSCRIPT_/, "");

  // ---- 归一化每条日志 ----
  const events = [];
  for (const row of RAW) {
    const source = row.source || "connect";

    // Gateway 日志: 来自 Bedrock AgentCore Gateway，可能是 JSON 也可能是纯文本。
    // 关联用的 contactId 由后端预先计算并写在 row.contactId 上。
    if (source === "gateway") {
      const gobj = tryParse(row.message); // 可能为 null(纯文本)
      events.push({
        ts: row.timestamp,
        source: "gateway",
        type: "GATEWAY",
        fullType: "GATEWAY",
        contactId: row.contactId || "",
        sessionId: "",
        span: null,
        obj: gobj,
        raw: row.message,
      });
      continue;
    }

    const obj = tryParse(row.message);
    if (!obj || !obj.event_type) {
      events.push({
        ts: row.timestamp, source: "connect", type: "UNKNOWN", contactId: row.contactId || "(unparsed)",
        sessionId: "", raw: row.message, obj: null,
      });
      continue;
    }
    let contactId = row.contactId || "";
    let span = null;
    if (!contactId && obj.session_name) contactId = obj.session_name;
    if (obj.span) {
      span = parseSpan(obj.span);
      if (!contactId && span.session_name) contactId = span.session_name;
    }
    // 没有 session_name 的事件(如 UTTERANCE / ORCHESTRATION)先记下 session_id，
    // 之后用 session_id -> contactId 的映射补全。
    events.push({
      ts: row.timestamp,
      source: "connect",
      type: SHORT_TYPE(obj.event_type),
      fullType: obj.event_type,
      contactId: contactId,
      sessionId: obj.session_id || "",
      span: span,
      obj: obj,
      raw: row.message,
      // 运行时点击「翻译」按钮后，由 /api/translate 填充
      translation: "",
      translationLang: "",
    });
  }

  // ---- 构建 session_id -> contactId 映射 ----
  const sidToCid = {};
  for (const e of events) {
    if (e.contactId && e.sessionId) sidToCid[e.sessionId] = e.contactId;
  }
  // 补全缺失 contactId
  for (const e of events) {
    if (!e.contactId && e.sessionId && sidToCid[e.sessionId]) {
      e.contactId = sidToCid[e.sessionId];
    }
    if (!e.contactId) {
      // gateway 没关联到会话的，单独归入一个分组，避免污染真实 Contact 列表
      e.contactId = e.source === "gateway"
        ? "(未关联的 Gateway 日志)"
        : (e.sessionId || "(unknown)");
    }
  }

  // ---- 按 contactId 分组 ----
  let groups = {};
  for (const e of events) {
    (groups[e.contactId] = groups[e.contactId] || []).push(e);
  }
  // 本地模式: RAW 是完整事件行; CloudFront 模式: RAW 是摘要行(_summaryOnly)，与 init() 的判据一致
  const LOCAL_RAW_MODE = RAW.length > 0 && !RAW[0]._summaryOnly;
  // 每个 Contact 的元信息(最早时间 / 事件数)，供侧栏排序使用
  let contactMeta = {};
  for (const cid of Object.keys(groups)) {
    const list = groups[cid];
    let mn = Infinity;
    for (const x of list) { if (x.ts < mn) mn = x.ts; }
    contactMeta[cid] = { minTs: mn, count: list.length };
    if (LOCAL_RAW_MODE) {
      // 本地事件已全部在内存里，加载时直接算转人工标志与原因，首屏就能显示
      try {
        const esc = detectEscalation(list);
        contactMeta[cid].escalated = esc.flag;
        contactMeta[cid].escalationReason = esc.reason;
      } catch (_) { /* 单个 contact 解析失败不影响其它 contact */ }
    }
  }
  let contactIds = Object.keys(groups);

  // 前端从事件里补全的转人工信息，按 contactId 保存，供 rebuildFromSummaries / 搜索结果合并回去
  const escOverride = {};
  function mergeEscOverride(cid, meta) {
    const o = escOverride[cid];
    if (!o || !meta) return;
    if (o.escalated) meta.escalated = true;
    // 只补空值，绝不覆盖非空的后端 reason
    if (!meta.escalationReason && o.escalationReason) meta.escalationReason = o.escalationReason;
  }

  // ---- 用新数据重建 groups / contactMeta / contactIds（切换实例时调用）----
  function rebuildFromSummaries(summaries) {
    // CloudFront 版: summaries 是摘要列表（含 contactId / minTs / count 等）
    // 事件明细按需加载，此时 groups[cid] 先用空数组占位，selectContact 时再填充
    groups = {};
    contactMeta = {};
    for (const s of (summaries || [])) {
      const cid = s.contactId;
      if (!cid) continue;
      // _summaryOnly 标记表示尚未拉取事件，直接放空数组占位
      groups[cid] = groups[cid] || [];
      contactMeta[cid] = {
        minTs:            s.minTs || 0,
        count:            s.count || 0,
        maxTs:            s.maxTs || 0,
        sourceCounts:     s.sourceCounts || {},
        locale:           s.locale || "",
        bu:               s.bu || "",
        mcpToolCalls:     s.mcpToolCalls || 0,
        faqAsk:           s.faqAsk || 0,
        faqSend:          s.faqSend || 0,
        srIntent:         s.srIntent || 0,
        srSuccess:        s.srSuccess || 0,
        srNumber:         s.srNumber || "",
        srRounds:         s.srRounds != null ? s.srRounds : null,
        aiAgentCalls:     s.aiAgentCalls || 0,
        errors:           s.errors || 0,
        guardBlocks:      s.guardBlocks || 0,
        guardReasons:     s.guardReasons || [],
        responseLatency:  s.responseLatency != null ? s.responseLatency : null,
        escalated:        s.escalated || false,
        escalationReason: s.escalationReason || "",
        channel:          s.channel || "",
        did:              s.did || "",
        disconnectReason: s.disconnectReason || "",
        csat:             s.csat != null ? s.csat : "",
        initiationMethod: s.initiationMethod || "",
      };
      mergeEscOverride(cid, contactMeta[cid]);
    }
    contactIds = Object.keys(groups);
  }

  // 侧栏排序: 时间(最早时间戳) 或 事件数，可切换升/降序
  let currentSort = "time_desc";
  function getOrderedContactIds() {
    return contactIds.slice().sort((a, b) => {
      const ma = contactMeta[a], mb = contactMeta[b];
      switch (currentSort) {
        case "time_asc":   return ma.minTs - mb.minTs;
        case "count_desc": return (mb.count - ma.count) || (mb.minTs - ma.minTs);
        case "count_asc":  return (ma.count - mb.count) || (ma.minTs - mb.minTs);
        case "time_desc":
        default:           return mb.minTs - ma.minTs; // 最近的会话排前面
      }
    });
  }

  // ---- 会话级统计(用于侧栏徽标) ----
  function summarize(list) {
    let errors = 0, escalations = 0, toolCalls = 0, llmCalls = 0;
    let gateway = 0;
    // 护栏拦截次数与 Contact 列表口径一致(结构化评估优先, 避免一次拦截记两次)
    const guardrailBlocks = guardStats(list).blocks;
    for (const e of list) {
      if (e.source === "gateway") {
        gateway++;
        if (isGatewayError(e)) errors++;
        continue;
      }
      if (e.span) {
        if (spanFailed(e.span)) errors++;    // barge-in 造成的 ERROR 不算错误
        if (e.span.span_name === "escalate_agent") escalations++;
        if (e.span.span_name === "execute_tool") toolCalls++;
        if (e.span.span_name === "inference") llmCalls++;
      }
    }
    return { errors, escalations, toolCalls, llmCalls, guardrailBlocks, gateway };
  }

  // 用户打断(barge-in)导致的 span: 客户抢话时 Connect 会把正在进行的
  // inference / invoke_agent 标成 status=ERROR，
  // 但这属于正常中止而非故障，不应计入错误。
  function spanBargeIn(sp) {
    if (!sp) return false;
    if (sp.span_name === "barge_in" || sp.operation_name === "barge_in") return true;
    if (sp.error_type === "barge_in") return true;
    return /barge[-_ ]?in/i.test(sp.status_description || "");
  }

  // span 是否真的失败: 状态非 OK，且不是 barge-in 造成的中止
  function spanFailed(sp) {
    if (!sp || !sp.status || sp.status === "OK") return false;
    return !spanBargeIn(sp);
  }

  // gateway 日志的错误判定: level/status 字段或文本含 ERROR/Exception
  function isGatewayError(e) {
    if (e.obj) {
      const lvl = (e.obj.level || e.obj.severity || e.obj.status || "").toString().toUpperCase();
      if (/ERROR|FATAL|CRITICAL/.test(lvl)) return true;
    }
    return /\b(ERROR|Exception|Traceback|FATAL)\b/.test(e.raw || "");
  }

  // Amazon Connect 侧的可选补充数据(通话智能摘要 / 挂断方 / CSAT / AI Agent 次数),
  // 这些字段无法从 CloudWatch AI Agent 日志里获取，需要额外调用 Amazon Connect API
  // (见 fetch-connect-contact-details.py 生成的 connect-enrich.js)。缺失时显示 N/A。
  const ENRICH = window.__CONNECT_CONTACT_ENRICH__ || {};

  // 「CSAT 满意度评分」对应的 Amazon Connect 联系人属性键名(不写死)。
  // 优先取运行时配置(site-config.js / aws-config.js 注入)，缺省回退 botevaluation。
  const CSAT_ATTR =
    (window.__UI_CONFIG__ && window.__UI_CONFIG__.csatAttribute) ||
    (window.__AWS_CONFIG__ && window.__AWS_CONFIG__.csatAttribute) ||
    "botevaluation";

  // 去掉 MCP 工具名的命名空间前缀: connect_repair_mcp_agent___faqSearch -> faqSearch
  function shortToolName(name) {
    if (!name) return "";
    const i = name.lastIndexOf("___");
    return i >= 0 ? name.slice(i + 3) : name;
  }

  // 取 execute_tool span 的输出正文(用于判断该次工具调用是否"成功有响应")
  function toolOutputText(sp) {
    let txt = "";
    const om = tryParse(sp.output_messages);
    if (Array.isArray(om)) {
      om.forEach((m) => (m.values || []).forEach((v) => {
        if (v.toolResult) {
          (v.toolResult.content || []).forEach((c) => { txt += (c.text || JSON.stringify(c)); });
        }
      }));
    }
    if (!txt && typeof sp.output_messages === "string") txt = sp.output_messages;
    return txt;
  }

  // 判断一次工具调用是否成功返回(span 状态 OK 且输出里没有 error / HTTP 4xx-5xx)
  function toolSucceeded(sp) {
    if (sp.status && sp.status !== "OK") return false;
    const txt = toolOutputText(sp);
    if (/"error"/.test(txt)) return false;
    if (/HTTP\s*[45]\d\d/.test(txt)) return false;
    return true;
  }

  // 判断一次 requestRepair 工具调用是否成功创建了 SR Draft:
  // 以工具输出里 ticketNumber 是否有非空返回值为准。
  function repairTicketReturned(sp) {
    const txt = toolOutputText(sp);
    if (!txt) return false;
    // 兼容多层转义(\"ticketNumber\": \"4685750498\" / "ticketNumber":"xxx")
    const m = txt.match(/ticketNumber\\*["']?\s*:\s*\\*["']?\s*([^"'\\,}\s]+)/i);
    if (!m) return false;
    const val = (m[1] || "").trim();
    return val && val.toLowerCase() !== "null" && val.toLowerCase() !== "none" && val !== "";
  }

  // 从原始日志文本里抽取 SR 工单号(srNumber):
  // 兼容多层转义, 例如 result={content=[{type=text, text={\"status\": \"success\",
  // ..., \"srNumber\": \"MISSR202607205000005\"}}
  function extractSrNumber(raw) {
    if (!raw) return "";
    const m = raw.match(/srNumber\\*["']?\s*:\s*\\*["']?\s*([^"'\\,}\s]+)/i);
    if (!m) return "";
    const val = (m[1] || "").trim();
    if (!val || val.toLowerCase() === "null" || val.toLowerCase() === "none") return "";
    return val;
  }

  // 从原始日志文本里抽取 BU (<customer_info> 中的 "- BU: xxx")
  function extractBU(raw) {
    if (!raw) return "";
    const m = raw.match(/-\s*BU:\s*([^\\"\n<}]+)/);
    return m ? m[1].trim() : "";
  }

  // 从原始日志文本里抽取 locale code (如 en_US / fr-FR)，优先取"配置的 locale"
  function extractLocale(raw) {
    if (!raw) return "";
    let m = raw.match(/language locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})/);
    if (!m) m = raw.match(/configured locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})/);
    if (!m) m = raw.match(/\blocale\b[^A-Za-z]{0,12}([a-z]{2}_[A-Z]{2})/);
    return m ? m[1] : "";
  }

  // 兜底: 当 extractLocale 未命中时，从 <system-variables> 节点里的
  // responseLanguage 取语言值(如 responseLanguage: en_US)。
  function extractResponseLanguage(raw) {
    if (!raw) return "";
    const m = raw.match(/responseLanguage[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})/);
    return m ? m[1] : "";
  }

  // ---- Guardrail 评估解析 ----
  // 从起始的 [ 或 { 开始做括号配对，截出完整的一段文本
  function sliceBalanced(s, start) {
    let depth = 0;
    for (let i = start; i < s.length; i++) {
      const c = s[i];
      if (c === "[" || c === "{") depth++;
      else if (c === "]" || c === "}") {
        depth--;
        if (depth === 0) return s.slice(start, i + 1);
      }
    }
    return "";
  }

  // 提取某个事件里全部 guardrail 评估对象(优先用已解析的 span 字段，兜底扫原始文本)
  function eventGuardAssessments(e) {
    if (e._guardAssessments) return e._guardAssessments;
    const out = [];
    const texts = [];
    if (e.span && e.span.guardrail_assessments) texts.push(e.span.guardrail_assessments);
    const raw = e.raw || "";
    if (!texts.length && /guardrail_assessments/.test(raw)) texts.push(raw);

    for (const text of texts) {
      // 原始消息里 guardrail 段是 JSON 转义过的(\" 或 \\")，先还原成普通引号
      const s = String(text).replace(/\\+"/g, '"');
      const arrays = [];
      if (s.indexOf("guardrail_assessments") === -1) {
        const i = s.indexOf("[");
        if (i !== -1) arrays.push(sliceBalanced(s, i));
      } else {
        let idx = 0, k;
        while ((k = s.indexOf("guardrail_assessments", idx)) !== -1) {
          const i = s.indexOf("[", k);
          if (i === -1) break;
          const seg = sliceBalanced(s, i);
          if (!seg) break;
          arrays.push(seg);
          idx = i + seg.length;
        }
      }
      for (const a of arrays) {
        const parsed = tryParse(a);
        if (Array.isArray(parsed)) {
          for (const item of parsed) if (item && typeof item === "object") out.push(item);
        }
      }
    }
    e._guardAssessments = out;
    return out;
  }

  // policy 的 details 可能是字符串，也可能是对象/数组(不同 policyType 结构不同)
  function guardrailDetailText(d) {
    if (d == null) return "";
    if (typeof d === "string") return d.trim();
    if (Array.isArray(d)) return d.map(guardrailDetailText).filter(Boolean).join(" / ");
    if (typeof d === "object") {
      const picked = ["name", "topic", "type", "filterType", "match", "detail"]
        .map((k) => d[k]).filter((v) => typeof v === "string" && v);
      if (picked.length) return picked.join(": ");
      try { return JSON.stringify(d); } catch (err) { return ""; }
    }
    return String(d);
  }

  // 判断某事件是否触发了 guardrail 拦截(action 非 NONE 即视为拦截)
  function eventGuardBlocked(e) {
    const assessments = eventGuardAssessments(e);
    if (assessments.length) {
      return assessments.some((a) => a.action && String(a.action).toUpperCase() !== "NONE");
    }
    if (e.obj && e.obj.guardrail_blocked === true) return true;
    const raw = e.raw || "";
    if (!/guardrail/i.test(raw)) return false;
    const re = /action[\\"'\s]*[:=][\\"'\s]*([A-Z_]+)/g;
    let m;
    while ((m = re.exec(raw)) !== null) {
      if (m[1] && m[1] !== "NONE") return true;
    }
    return false;
  }

  // 收集该事件里出现的 guardrail 拦截原因:
  // 每个被触发的 guardrailId 输出 "guardrailName · policyType · details"
  function eventGuardReasons(e) {
    const out = [];
    const assessments = eventGuardAssessments(e);
    for (const a of assessments) {
      const action = String(a.action || "").toUpperCase();
      if (action === "NONE") continue;                     // 未触发的护栏不计入原因
      const name = (a.guardrailName || a.guardrailId || "guardrail").trim();
      const policies = Array.isArray(a.policies) ? a.policies.filter(Boolean) : [];
      const hit = policies.filter((p) => String(p.action || "").toUpperCase() !== "NONE");
      const use = hit.length ? hit : policies;
      if (!use.length) {
        out.push(name + " · " + (action || "BLOCKED"));      // 没有 policies 明细时只给名称+动作
        continue;
      }
      for (const p of use) {
        const parts = [name];
        if (p.policyType) parts.push(String(p.policyType));
        const detail = guardrailDetailText(p.details);
        if (detail) parts.push(detail);
        out.push(parts.join(" · "));
      }
    }
    // 已能结构化解析时以解析结果为准(全部 action=NONE 则确实没有拦截原因)
    if (assessments.length) return out;

    // 兜底: 结构化解析失败时，仍按老逻辑从文本里抓 guardrailName
    const raw = e.raw || "";
    if (!/guardrail/i.test(raw)) return out;
    const re = /guardrailName[\\"'\s]*[:=][\\"'\s]*([^\\"',}\]]+)/g;
    let m;
    while ((m = re.exec(raw)) !== null) {
      const n = (m[1] || "").trim();
      if (n) out.push(n);
    }
    return out;
  }

  // 统计一批事件里的护栏拦截: 次数 + 每条原因各命中多少次。
  // 口径说明:
  //   1) 同一次拦截会在日志里出现两次 —— ORCHESTRATION_MESSAGE(guardrail_blocked=true)
  //      与 inference span 的 guardrail_assessments=BLOCKED。只要有结构化评估就以评估为准，
  //      避免把一次拦截算成两次。
  //   2) 次数由「原因命中次数」累加而来，各原因次数之和恒等于拦截次数。
  //   3) 只有拦截标记、拿不到原因的事件归入「未提供原因」，保证两列仍然对得上。
  function guardStats(list) {
    const assessed = [], fallback = [];
    for (const e of list) {
      if (!eventGuardBlocked(e)) continue;
      (eventGuardAssessments(e).length ? assessed : fallback).push(e);
    }
    const use = assessed.length ? assessed : fallback;

    const counts = new Map();   // 原因文本 -> 命中次数
    let blocks = 0;
    for (const e of use) {
      let rs = eventGuardReasons(e);
      if (!rs.length) rs = [t("guardReasonUnknown")];
      for (const r of rs) {
        counts.set(r, (counts.get(r) || 0) + 1);
        blocks++;
      }
    }
    const reasons = Array.from(counts, ([text, count]) => ({ text, count }))
      .sort((a, b) => b.count - a.count || a.text.localeCompare(b.text));
    return { blocks, reasons };
  }

  // ---- 转人工原因提取 ----
  function isEscalateTool(name) {
    return /Escalate/i.test(name || "");
  }

  // 去掉引号前的转义反斜杠: \"  \\\"  \'  ->  "  '
  function unescapeQuotes(s) {
    return s.replace(/\\+(["'\\])/g, "$1");
  }

  // 从文本里取 reason: 先认 Escalate(reason="...") / Escalate(reason=\"...\")，
  // 再认 "reason": "..." / reason=\"...\" 这类键值形式。
  // 只能用在已知属于 Escalate 调用或模型输出的文本上(不要扫 prompt)。
  function reasonFromText(s) {
    if (typeof s !== "string") return "";
    let m = s.match(/\bEscalate\s*\(\s*reason\s*=\s*(\\*)(["'])([\s\S]*?)\1\2\s*\)/);
    if (m) return unescapeQuotes(m[3]);
    m = s.match(/\breason\\*["']?\s*[:=]\s*(\\*)(["'])([\s\S]*?)\1\2/);
    if (m) return unescapeQuotes(m[3]);
    return "";
  }

  // 工具入参可能是对象 {reason: "..."}，也可能是 JSON 字符串 "{\"reason\": \"...\"}"
  function reasonFromToolInput(input) {
    if (input && typeof input === "object") {
      return typeof input.reason === "string" ? input.reason : "";
    }
    if (typeof input === "string") {
      const o = tryParse(input);
      if (o && typeof o === "object" && typeof o.reason === "string") return o.reason;
      return reasonFromText(input);
    }
    return "";
  }

  // parsed_response 形如 ToolUseBlock(ToolUseId=..., Name=Escalate, Input="{\"reason\": \"...\"}")
  function reasonsFromToolUseBlocks(s) {
    const out = [];
    const re = /Name=([^,\s)]+),\s*Input="((?:[^"\\]|\\.)*)"/g;
    let m;
    while ((m = re.exec(s))) {
      if (!isEscalateTool(m[1])) continue;
      let inp;
      try { inp = JSON.parse('"' + m[2] + '"'); } catch (_) { inp = m[2]; }
      out.push(reasonFromToolInput(inp));
    }
    return out;
  }

  // 检测该会话是否触发转人工(Escalate 工具 / escalate_agent span)并取原因
  // 同一次转人工会出现在多条事件里，原因按时间顺序去重后用 "; " 连接
  function detectEscalation(list) {
    let flag = false;
    const reasons = [];
    const add = (r) => {
      const v = (r || "").replace(/\s+/g, " ").trim();
      if (!v) return;
      flag = true;
      if (!reasons.includes(v)) reasons.push(v);
    };
    for (const e of list) {
      if (e.span && e.span.span_name === "escalate_agent") flag = true;
      if (e.span && e.span.span_name === "execute_tool") {
        if (isEscalateTool(toolNameFromSpan(e.span))) flag = true;
        // execute_tool span 的 input/output_messages 里带 toolUse.input
        for (const raw of [e.span.input_messages, e.span.output_messages]) {
          if (!raw) continue;
          const arr = tryParse(raw);
          if (Array.isArray(arr)) {
            for (const m of arr) {
              for (const v of ((m && m.values) || [])) {
                if (v && v.toolUse && isEscalateTool(v.toolUse.name)) add(reasonFromToolInput(v.toolUse.input));
              }
            }
          } else if (typeof raw === "string" && /Escalate/i.test(raw)) {
            add(reasonFromText(raw));
          }
        }
      }
      if (e.obj && e.obj.completion) {
        // completion: {"toolUseList":[{"toolName":"Escalate","toolInput":"{\"reason\": ...}"}]} 或纯文本
        const c = tryParse(e.obj.completion);
        if (c && typeof c === "object") {
          for (const tu of (c.toolUseList || [])) {
            if (tu && isEscalateTool(tu.toolName)) add(reasonFromToolInput(tu.toolInput));
          }
        } else if (typeof e.obj.completion === "string" && /Escalate/i.test(e.obj.completion)) {
          add(reasonFromText(e.obj.completion));
        }
      }
      if (e.obj && typeof e.obj.parsed_response === "string") {
        const pr = e.obj.parsed_response;
        const rs = reasonsFromToolUseBlocks(pr);
        rs.forEach(add);
        if (!rs.length && /\bEscalate\s*\(/.test(pr)) add(reasonFromText(pr));
      }
      // 预检: prompt 是字符串且根本不含 Escalate 时，不可能有 Escalate toolUse，跳过整段 JSON 解析
      if (e.obj && e.obj.prompt && (typeof e.obj.prompt !== "string" || /Escalate/i.test(e.obj.prompt))) {
        const p = tryParse(e.obj.prompt) || {};
        for (const msg of (p.messages || [])) {
          for (const c of (msg.content || [])) {
            if (c.toolUse && isEscalateTool(c.toolUse.name)) {
              flag = true;
              add(reasonFromToolInput(c.toolUse.input));
            }
          }
        }
      }
    }
    return { flag: flag, reason: reasons.join("; ") };
  }

  // ---- 把毫秒时长格式化为易读文本 ----
  function formatDuration(ms) {
    if (!ms || ms < 0) return t("durationZero");
    const totalSec = Math.round(ms / 1000);
    const h = Math.floor(totalSec / 3600);
    const m = Math.floor((totalSec % 3600) / 60);
    const s = totalSec % 60;
    const parts = [];
    if (h) parts.push(h + t("durationHour"));
    if (m) parts.push(m + t("durationMin"));
    parts.push(s + t("durationSec"));
    // 中文单位紧凑无空格，其他语言用空格分隔更易读
    return parts.join(currentLang === DEFAULT_LANG ? "" : " ");
  }

  // ---- 把 epoch 毫秒格式化为本地日期时间 ----
  function formatDateTime(ts) {
    if (ts == null) return "—";
    return new Date(ts).toLocaleString([], {
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    });
  }

  // ---- 渲染侧栏 ----
  const contactListEl = document.getElementById("contactList");
  const searchBox = document.getElementById("searchBox");
  let activeCid = null;

  function renderSidebar(filter) {
    const f = (filter || "").trim().toLowerCase();
    contactListEl.innerHTML = "";
    const shown = getOrderedContactIds().filter((c) => !f || c.toLowerCase().includes(f));
    if (shown.length === 0) {
      contactListEl.innerHTML = '<div class="empty" style="margin-top:30px;font-size:13px">' + esc(t("noMatchContact")) + '</div>';
      return;
    }
    for (const cid of shown) {
      const list = groups[cid];
      const meta = contactMeta[cid] || {};
      // 事件未加载时（CloudFront 按需拉取）用 contactMeta 兜底显示
      const eventCount = list.length || meta.count || 0;
      const s = summarize(list);
      const minTsVal = list.length
        ? Math.min.apply(null, list.map((x) => x.ts))
        : (meta.minTs || 0);
      const tmin = new Date(minTsVal);
      const item = document.createElement("div");
      item.className = "contact-item" + (cid === activeCid ? " active" : "");
      let badges = `<span class="badge">${esc(t("badgeEvents", { n: eventCount }))}</span>`;
      if (s.toolCalls) badges += `<span class="badge">${esc(t("badgeTools", { n: s.toolCalls }))}</span>`;
      if (s.gateway) badges += `<span class="badge gw">${esc(t("badgeGateway", { n: s.gateway }))}</span>`;
      if (s.escalations) badges += `<span class="badge esc">${esc(t("badgeEscalation", { n: s.escalations }))}</span>`;
      if (s.errors) badges += `<span class="badge err">${esc(t("badgeError", { n: s.errors }))}</span>`;
      if (s.guardrailBlocks) badges += `<span class="badge err">${esc(t("badgeGuard", { n: s.guardrailBlocks }))}</span>`;
      if (!s.errors && !s.escalations) badges += `<span class="badge ok">${esc(t("badgeOk"))}</span>`;
      // 翻译按钮(仅当本地服务器提供翻译接口时显示)，并反映"未译/翻译中/已译"三态
      let transBtn = "";
      if (TRANSLATE_CFG.available && cid !== "(未关联的 Gateway 日志)") {
        if (translatingContacts.has(cid)) {
          transBtn = `<button class="icon-btn js-translate busy" title="${esc(t("titleTranslating"))}" disabled>⏳</button>`;
        } else if (translatedContacts.has(cid)) {
          transBtn = `<button class="icon-btn js-translate ok" title="${esc(t("titleRetranslate"))}">${esc(t("btnTranslated"))}</button>`;
        } else {
          transBtn = `<button class="icon-btn js-translate" title="${esc(t("titleTranslate", { lang: TRANSLATE_CFG.lang }))}">${esc(t("btnTranslate"))}</button>`;
        }
      }

      item.innerHTML =
        `<div class="cid-row">` +
          `<span class="cid" title="${cid}">${cid}</span>` +
          `<span class="cid-actions">` +
            transBtn +
            `<button class="icon-btn js-copy" title="${esc(t("titleCopy"))}">⧉</button>` +
            `<button class="icon-btn js-download" title="${esc(t("titleDownload"))}">⬇</button>` +
          `</span>` +
        `</div>` +
        `<div class="sub">${tmin.toLocaleString()}</div>` +
        `<div class="badges">${badges}</div>`;

      // 点击整行选中会话
      item.addEventListener("click", () => selectContact(cid));

      // 拷贝按钮(阻止冒泡，避免触发选中)
      const copyBtn = item.querySelector(".js-copy");
      copyBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        copyToClipboard(cid, copyBtn);
      });

      // 下载按钮
      const dlBtn = item.querySelector(".js-download");
      dlBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        downloadContactCsv(cid);
      });

      // 翻译按钮
      const trBtn = item.querySelector(".js-translate");
      if (trBtn) {
        trBtn.addEventListener("click", (ev) => {
          ev.stopPropagation();
          translateContact(cid).catch(() => {});
        });
      }

      contactListEl.appendChild(item);
    }
  }

  // ---- 拷贝 Contact ID 到剪贴板 ----
  function copyToClipboard(text, btn) {
    const done = () => {
      if (!btn) return;
      const old = btn.textContent;
      btn.textContent = "✓";
      btn.classList.add("ok");
      setTimeout(() => { btn.textContent = old; btn.classList.remove("ok"); }, 1200);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(() => fallbackCopy(text, done));
    } else {
      fallbackCopy(text, done);
    }
  }

  function fallbackCopy(text, done) {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      if (done) done();
    } catch (e) {
      window.prompt(t("copyPrompt"), text);
    }
  }

  // ---- 下载该 Contact 的日志为 CSV(含 connect + gateway) ----
  function csvEscape(v) {
    const s = String(v == null ? "" : v);
    if (/[",\r\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }

  function downloadContactCsv(cid) {
    const list = groups[cid].slice().sort((a, b) => a.ts - b.ts);
    const header = ["timestamp_ms", "datetime", "source", "event_type", "message", t("csvOriginal"), t("csvTranslation")];
    const lines = [header.join(",")];
    for (const e of list) {
      const dt = new Date(e.ts).toISOString();
      lines.push([
        csvEscape(e.ts),
        csvEscape(dt),
        csvEscape(e.source || ""),
        csvEscape(e.fullType || e.type || ""),
        csvEscape(e.raw != null ? e.raw : ""),
        csvEscape(getTranslatableText(e) || ""),
        csvEscape(e.translation || ""),
      ].join(","));
    }
    // 加 BOM，便于 Excel 正确识别 UTF-8
    const blob = new Blob(["\ufeff" + lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const safeCid = String(cid).replace(/[^a-zA-Z0-9_-]+/g, "_");
    a.href = url;
    a.download = `contact-${safeCid}-logs.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  // ---- 渲染消息气泡的辅助 ----
  function extractMessageText(values) {
    // values 可能是数组 [{type:"text", value:"..."}] 或字符串
    if (typeof values === "string") {
      const p = tryParse(values);
      if (p) values = p; else return values;
    }
    if (Array.isArray(values)) {
      return values.map((v) => v.value || v.text || "").join("\n").trim();
    }
    return "";
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  // 从 AI Agent 回复中拆出 <message> 与 <thinking>
  function splitMessageThinking(text) {
    const msgs = [];
    const thinks = [];
    const re = /<(message|thinking)>([\s\S]*?)<\/\1>/g;
    let m, matched = false;
    while ((m = re.exec(text)) !== null) {
      matched = true;
      if (m[1] === "message") msgs.push(m[2].trim());
      else thinks.push(m[2].trim());
    }
    if (!matched) return { messages: [text.trim()], thinking: [] };
    return { messages: msgs, thinking: thinks };
  }

  // ---- 判定事件的角色(客户/机器人/工具)与严重度(错误/护栏)，用于高亮 ----
  function eventRoleInfo(e) {
    let role = "", badge = "", sev = "";
    if (e.type === "UTTERANCE") {
      const u = (e.obj && e.obj.utterance) || "";
      role = /^\[CUSTOMER\]/.test(u) ? "customer" : "bot";
      badge = role === "customer" ? "CUSTOMER" : "BOT";
    } else if (e.type === "ORCHESTRATION_MESSAGE") {
      role = ((e.obj && e.obj.participant) || "BOT").toLowerCase() === "customer" ? "customer" : "bot";
      badge = role === "customer" ? "CUSTOMER" : "BOT";
      if (e.obj && e.obj.guardrail_blocked) sev = "guard";
    } else if (e.type === "AI_AGENT_TRACE" && e.span && e.span.span_name === "execute_tool") {
      role = "tool"; badge = "TOOL";
      if (spanFailed(e.span)) sev = "err";
    } else if (e.type === "GATEWAY") {
      role = "tool"; badge = t("whoGateway");
      if (isGatewayError(e)) sev = "err";
    } else if (e.type === "AI_AGENT_TRACE") {
      if (spanFailed(e.span)) sev = "err";
      else if (spanBargeIn(e.span)) sev = "barge";   // 用户打断: 单独着色, 不算错误
    }
    return { role: role, badge: badge, sev: sev };
  }

  // ---- 渲染单个事件 ----
  function renderEvent(e) {
    const div = document.createElement("div");
    const ri = eventRoleInfo(e);
    div.className = "event t-" + e.type +
      (ri.role ? " role-" + ri.role : "") +
      (ri.sev ? " sev-" + ri.sev : "") +
      (e.translation ? " open" : "");

    const time = new Date(e.ts).toLocaleTimeString([], { hour12: false }) +
      "." + String(e.ts % 1000).padStart(3, "0");

    let summary = "";
    let tags = "";
    let bodyHtml = "";

    if (e.type === "UTTERANCE") {
      const u = e.obj.utterance || "";
      summary = u;
      const isCustomer = /^\[CUSTOMER\]/.test(u);
      const who = isCustomer ? "customer" : "bot";
      const txt = u.replace(/^\[(CUSTOMER|AGENT|BOT|SYSTEM)\]\s*/, "");
      bodyHtml = `<div class="bubble ${who}">${esc(txt)}</div>`;
      if (e.translation) bodyHtml += renderTranslationBubble(e.translation, e.translationLang, who);

    } else if (e.type === "ORCHESTRATION_MESSAGE") {
      const txt = extractMessageText(e.obj.values);
      const who = (e.obj.participant || "BOT").toLowerCase() === "customer" ? "customer" : "bot";
      summary = `[${e.obj.participant || "BOT"}] ${txt}`.replace(/\s+/g, " ");
      const parts = splitMessageThinking(txt);
      bodyHtml += `<div class="kv"><b>participant:</b> ${esc(e.obj.participant || "")} · <b>model:</b> ${esc(e.obj.model_id || "")}</div>`;
      for (const mm of parts.messages) if (mm) bodyHtml += `<div class="bubble ${who}">${esc(mm)}</div>`;
      for (const th of parts.thinking) if (th) bodyHtml += `<div class="bubble thinking">💭 ${esc(th)}</div>`;
      if (e.translation) bodyHtml += renderTranslationBubble(e.translation, e.translationLang, who);
      if (e.obj.guardrail_blocked) tags += `<span class="ev-tag err">guardrail</span>`;

    } else if (e.type === "AGENTIC_MESSAGE" || e.type === "LARGE_LANGUAGE_MODEL_INVOCATION") {
      const prompt = tryParse(e.obj.prompt) || {};
      const msgs = prompt.messages || [];
      summary = t("evLlmCall", {
        kind: e.type === "AGENTIC_MESSAGE" ? "Agentic" : "LLM",
        model: prompt.modelId || e.obj.model_id || "?",
        n: msgs.length,
      });
      bodyHtml += `<div class="kv"><b>modelId:</b> ${esc(prompt.modelId || "")}</div>`;
      bodyHtml += renderConversation(msgs);
      // 工具配置
      if (prompt.toolConfig && prompt.toolConfig.tools) {
        const names = prompt.toolConfig.tools.map((tc) => (tc.toolSpec && tc.toolSpec.name) || "").filter(Boolean);
        bodyHtml += `<details><summary>${esc(t("evAvailableTools", { n: names.length }))}</summary><div class="kv">${esc(names.join(", "))}</div></details>`;
      }

    } else if (e.type === "AI_AGENT_TRACE") {
      const sp = e.span || {};
      const dur = (sp.start_timestamp && sp.end_timestamp)
        ? (Number(sp.end_timestamp) - Number(sp.start_timestamp)) + "ms" : "";
      summary = `${sp.span_name || "span"} · ${sp.status || ""} ${dur ? "· " + dur : ""}`;
      if (sp.ai_agent_name) summary += ` · ${sp.ai_agent_name.trim()}`;
      if (spanFailed(sp)) tags += `<span class="ev-tag err">${esc(sp.status)}</span>`;
      else if (spanBargeIn(sp)) tags += `<span class="ev-tag barge">${esc(t("tagBargeIn"))}</span>`;
      else tags += `<span class="ev-tag ok">OK</span>`;
      if (sp.span_name === "execute_tool") {
        const tn = toolNameFromSpan(sp);
        tags += `<span class="ev-tag mcp">🔌 MCP${tn ? ": " + esc(tn) : ""}</span>`;
      }

      const interesting = ["span_name", "span_type", "status", "operation_name",
        "ai_agent_type", "ai_agent_name", "ai_agent_version", "provider_name",
        "request_id", "span_id", "parent_span_id"];
      for (const k of interesting) {
        if (sp[k] !== undefined) bodyHtml += `<div class="kv"><b>${k}:</b> ${esc(String(sp[k]).trim())}</div>`;
      }
      // token 使用
      const tokenKeys = Object.keys(sp).filter((k) => /usage_|tokens|finish_reason/.test(k));
      if (tokenKeys.length) {
        bodyHtml += `<div class="kv" style="margin-top:6px"><b>${esc(t("evUsage"))}</b> ` +
          tokenKeys.map((k) => `${k}=${esc(sp[k])}`).join(" · ") + `</div>`;
      }
      // 工具调用 input/output
      if (sp.span_name === "execute_tool") {
        const im = tryParse(sp.input_messages);
        const om = tryParse(sp.output_messages);
        bodyHtml += renderToolSpan(im, om);
      }

    } else if (e.type === "CREATE_SESSION") {
      summary = t("evCreateSession", { n: e.obj.session_name || "" });
      bodyHtml += `<div class="kv"><b>session_id:</b> ${esc(e.obj.session_id || "")}</div>`;
      bodyHtml += `<div class="kv"><b>session_name:</b> ${esc(e.obj.session_name || "")}</div>`;

    } else if (e.type === "SESSION_POLLED") {
      summary = t("evSessionPolled") + (e.obj.connect_user_arn ? " · " + t("evAssignedAgent") : "");
      if (e.obj.connect_user_arn) bodyHtml += `<div class="kv"><b>connect_user_arn:</b> ${esc(e.obj.connect_user_arn)}</div>`;

    } else if (e.type === "GATEWAY") {
      // Bedrock AgentCore Gateway 应用日志: 结构化或纯文本都兼容
      const o = e.obj;
      if (o && typeof o === "object") {
        const msgText = o.message || o.msg || o.body || "";
        const lvl = o.level || o.severity || o.status || "";
        summary = `${lvl ? "[" + lvl + "] " : ""}${typeof msgText === "string" ? msgText : JSON.stringify(msgText)}`.replace(/\s+/g, " ");
        const interesting = ["level", "severity", "status", "logger", "tool", "tool_name",
          "method", "operation", "requestId", "request_id", "duration", "durationMs", "latency"];
        for (const k of interesting) {
          if (o[k] !== undefined) bodyHtml += `<div class="kv"><b>${k}:</b> ${esc(typeof o[k] === "string" ? o[k] : JSON.stringify(o[k]))}</div>`;
        }
        if (msgText) bodyHtml += `<div class="bubble" style="background:#1a1f2b;border:1px solid var(--border)">${esc(typeof msgText === "string" ? msgText : JSON.stringify(msgText))}</div>`;
      } else {
        summary = (e.raw || "").replace(/\s+/g, " ");
        bodyHtml += `<div class="bubble" style="background:#1a1f2b;border:1px solid var(--border)">${esc(e.raw || "")}</div>`;
      }
      if (isGatewayError(e)) tags += `<span class="ev-tag err">error</span>`;

    } else {
      summary = e.fullType || e.type;
    }

    bodyHtml += `<details><summary>${esc(t("rawJson"))}</summary><pre class="raw">${esc(prettyRaw(e.raw))}</pre></details>`;

    const srcTag = `<span class="src-tag src-${e.source}">${e.source === "gateway" ? "Gateway" : "Connect"}</span>`;
    const whoBadge = ri.badge ? `<span class="who ${ri.role}">${ri.badge}</span>` : "";

    div.innerHTML =
      `<div class="event-head">` +
        `<span class="ev-time">${time}</span>` +
        srcTag +
        whoBadge +
        `<span class="ev-type">${esc(e.type)}</span>` +
        `<span class="ev-summary">${esc(summary)}</span>` +
        tags +
      `</div>` +
      `<div class="event-body">${bodyHtml}</div>`;

    div.querySelector(".event-head").addEventListener("click", () => {
      div.classList.toggle("open");
    });
    return div;
  }

  function prettyRaw(raw) {
    const o = tryParse(raw);
    if (o) { try { return JSON.stringify(o, null, 2); } catch (e) {} }
    return raw;
  }

  // ---- 渲染高亮的译文气泡(原文保留在上方，译文高亮显示在下方) ----
  function renderTranslationBubble(translation, lang, who) {
    const label = lang ? esc(t("transLabelLang", { lang: lang })) : esc(t("transLabel"));
    return (
      `<div class="bubble ${who} translated">` +
        `<span class="trans-badge">${label}</span>` +
        esc(translation) +
      `</div>`
    );
  }

  // ---- 取一条事件里可翻译的对话正文(客户/机器人) ----
  function getTranslatableText(e) {
    if (e.source !== "connect") return "";
    if (e.type === "UTTERANCE") {
      const u = e.obj && e.obj.utterance || "";
      return u.replace(/^\[(CUSTOMER|AGENT|BOT|SYSTEM)\]\s*/, "").trim();
    }
    if (e.type === "ORCHESTRATION_MESSAGE") {
      return (e.obj ? extractMessageText(e.obj.values) : "").trim();
    }
    return "";
  }

  // ---- 从 execute_tool span 里取工具名 ----
  function toolNameFromSpan(sp) {
    const im = tryParse(sp.input_messages);
    if (Array.isArray(im)) {
      for (const m of im) {
        for (const v of (m.values || [])) {
          if (v.toolUse && v.toolUse.name) return v.toolUse.name;
        }
      }
    }
    return sp.tool_name || sp.tool || "";
  }

  // ---- 收集某会话里被调用的 MCP 工具名 ----
  function collectToolNames(list) {
    const set = new Set();
    for (const e of list) {
      if (e.source === "connect" && e.span && e.span.span_name === "execute_tool") {
        const n = toolNameFromSpan(e.span);
        if (n) set.add(n);
      }
      if (e.obj && e.obj.prompt) {
        const prompt = tryParse(e.obj.prompt) || {};
        for (const m of (prompt.messages || [])) {
          for (const c of (m.content || [])) {
            if (c.toolUse && c.toolUse.name) set.add(c.toolUse.name);
          }
        }
      }
    }
    return Array.from(set);
  }

  function renderConversation(msgs) {
    let html = "";
    for (const m of msgs) {
      const role = m.role || "?";
      const content = m.content || [];
      for (const c of content) {
        if (c.text !== undefined) {
          const parts = splitMessageThinking(c.text);
          for (const mm of parts.messages) if (mm) html += `<div class="bubble ${role === "user" ? "customer" : "bot"}">${esc(mm)}</div>`;
          for (const th of parts.thinking) if (th) html += `<div class="bubble thinking">💭 ${esc(th)}</div>`;
        } else if (c.guardContent && c.guardContent.text) {
          const gt = c.guardContent.text.text || c.guardContent.text;
          html += `<div class="bubble ${role === "user" ? "customer" : "bot"}">${esc(typeof gt === "string" ? gt : JSON.stringify(gt))}</div>`;
        } else if (c.toolUse) {
          html += `<div class="toolcall mcp">🔌 <span class="mcp-label">${esc(t("evMcpTool"))}</span> <span class="name">${esc(c.toolUse.name)}</span><div class="kv">input: ${esc(JSON.stringify(c.toolUse.input))}</div></div>`;
        } else if (c.toolResult) {
          const rc = (c.toolResult.content || []).map((x) => x.text || JSON.stringify(x)).join("\n");
          html += `<div class="toolcall">↩️ ${esc(t("evToolResult"))}<pre class="raw">${esc(rc)}</pre></div>`;
        }
      }
    }
    return html;
  }

  function renderToolSpan(im, om) {
    let html = "";
    const collect = (arr) => {
      const r = [];
      (arr || []).forEach((msg) => (msg.values || []).forEach((v) => {
        if (v.toolUse) r.push(`<div class="toolcall mcp">🔌 <span class="mcp-label">${esc(t("evMcpTool"))}</span> <span class="name">${esc(v.toolUse.name)}</span><div class="kv">input: ${esc(typeof v.toolUse.input === "string" ? v.toolUse.input : JSON.stringify(v.toolUse.input))}</div></div>`);
        if (v.toolResult) {
          const rc = (v.toolResult.content || []).map((x) => x.text || JSON.stringify(x)).join("\n");
          r.push(`<div class="toolcall">↩️ ${esc(t("evResult"))}<pre class="raw">${esc(rc)}</pre></div>`);
        }
      }));
      return r.join("");
    };
    html += collect(im);
    html += collect(om);
    return html;
  }

  // ---- 过滤器 ----
  const ALL_TYPES = ["UTTERANCE", "ORCHESTRATION_MESSAGE", "AGENTIC_MESSAGE",
    "LARGE_LANGUAGE_MODEL_INVOCATION", "AI_AGENT_TRACE", "CREATE_SESSION", "SESSION_POLLED", "GATEWAY"];
  const activeFilters = new Set(ALL_TYPES);

  const filtersEl = document.getElementById("filters");
  function renderFilters() {
    filtersEl.innerHTML = "";
    ALL_TYPES.forEach((t) => {
      const chip = document.createElement("span");
      chip.className = "filter-chip" + (activeFilters.has(t) ? " on" : "");
      chip.textContent = t;
      chip.addEventListener("click", () => {
        if (activeFilters.has(t)) activeFilters.delete(t); else activeFilters.add(t);
        renderFilters();
        if (activeCid) renderTimeline(activeCid);
      });
      filtersEl.appendChild(chip);
    });
  }

  // ---- 渲染时间线 ----
  const timelineEl = document.getElementById("timeline");
  const selectedTitle = document.getElementById("selectedTitle");

  function renderTimeline(cid) {
    timelineEl.innerHTML = "";
    const group = groups[cid];
    if (!group || !group.length) {
      const d = document.createElement("div");
      d.className = "empty";
      d.textContent = t("noEventsFilter");
      timelineEl.appendChild(d);
      return;
    }
    const list = group.slice().sort((a, b) => a.ts - b.ts);
    const s = summarize(list);

    const toolNames = collectToolNames(list);
    const toolsHtml = toolNames.length
      ? `<div class="tool-list">🔌 <b>${esc(t("calledTools"))}</b> ` +
          toolNames.map((n) => `<span class="tool-chip">${esc(n)}</span>`).join("") + `</div>`
      : "";

    const stat = document.createElement("div");
    stat.className = "event";
    stat.style.borderColor = "var(--accent)";
    stat.innerHTML = `<div class="event-body" style="display:block;padding:12px">
      <div class="stat-row">
        <span>${esc(t("statEvents"))} <b>${list.length}</b></span>
        <span>${esc(t("statLlm"))} <b>${s.llmCalls}</b></span>
        <span>${esc(t("statTool"))} <b>${s.toolCalls}</b></span>
        <span>${esc(t("statGateway"))} <b>${s.gateway}</b></span>
        <span>${esc(t("statEscalation"))} <b>${s.escalations}</b></span>
        <span>${esc(t("statError"))} <b style="color:${s.errors ? "var(--red)" : "inherit"}">${s.errors}</b></span>
        <span>${esc(t("statGuard"))} <b>${s.guardrailBlocks}</b></span>
      </div>${toolsHtml}</div>`;
    timelineEl.appendChild(stat);

    let shown = 0;
    for (const e of list) {
      if (!activeFilters.has(e.type) && ALL_TYPES.includes(e.type)) continue;
      // 单条事件渲染出错不应让整个时间线空白，兜底显示原始 JSON。
      try {
        timelineEl.appendChild(renderEvent(e));
      } catch (err) {
        const d = document.createElement("div");
        d.className = "event";
        d.innerHTML = `<div class="event-body" style="display:block;padding:10px">` +
          `<div class="kv"><b>${esc(e.type || "EVENT")}</b> · ${esc(new Date(e.ts).toLocaleString())}</div>` +
          `<pre class="raw">${esc(prettyRaw(e.raw))}</pre></div>`;
        timelineEl.appendChild(d);
      }
      shown++;
    }
    if (shown === 0) {
      const d = document.createElement("div");
      d.className = "empty";
      d.textContent = t("noEventsFilter");
      timelineEl.appendChild(d);
    }
  }

  function selectContact(cid) {
    activeCid = cid;
    selectedTitle.textContent = cid;
    renderSidebar(searchBox.value);

    // CloudFront 版: 事件明细按需从 API Gateway 拉取
    if (typeof window.__CONNECT_LOAD_CONTACT_EVENTS__ === "function"
        && (!groups[cid] || groups[cid].length === 0)) {
      // 先显示加载中占位
      timelineEl.innerHTML = '<div class="empty" style="margin-top:30px;font-size:13px">正在加载事件… Loading events…</div>';
      window.__CONNECT_LOAD_CONTACT_EVENTS__(cid)
        .then(function (rows) {
          // 把拉到的原始事件行解析成 app.js 可用的 event 对象，存入 groups
          if (!groups[cid] || groups[cid].length === 0) {
            groups[cid] = parseRawRows(rows);
            applyEscalationFromEvents(cid);
          }
          if (activeCid === cid) renderTimeline(cid);
        })
        .catch(function (e) {
          if (activeCid === cid) {
            timelineEl.innerHTML = '<div class="empty sev-err" style="margin-top:30px;font-size:13px">加载失败: ' + esc(e && e.message ? e.message : String(e)) + '</div>';
          }
        });
    } else {
      renderTimeline(cid);
    }
  }

  // 事件明细加载后，用前端更完整的提取结果补全 contactMeta 里的转人工信息
  function applyEscalationFromEvents(cid) {
    const list = groups[cid];
    const meta = contactMeta[cid];
    if (!meta || !list || !list.length) return false;
    let esc;
    try { esc = detectEscalation(list); } catch (_) { return false; }
    let changed = false;
    if (esc.flag && !meta.escalated) { meta.escalated = true; changed = true; }
    // 只补空值，绝不覆盖非空的后端 reason
    if (esc.reason && !meta.escalationReason) { meta.escalationReason = esc.reason; changed = true; }
    if (changed) {
      escOverride[cid] = { escalated: !!meta.escalated, escalationReason: meta.escalationReason || "" };
      try { renderDashboard(); } catch (_) { /* 重绘失败不影响日志视图 */ }
    }
    return changed;
  }

  // ---- 把从 API 拉回的原始事件行解析成 app.js 内部 event 对象 ----
  function parseRawRows(rows) {
    const parsed = [];
    for (const row of (rows || [])) {
      const source = row.source || "connect";
      if (source === "gateway") {
        const gobj = tryParse(row.message);
        parsed.push({
          ts: row.timestamp, source: "gateway", type: "GATEWAY", fullType: "GATEWAY",
          contactId: row.contactId || "", sessionId: "", span: null, obj: gobj, raw: row.message,
        });
        continue;
      }
      const obj = tryParse(row.message);
      if (!obj || !obj.event_type) {
        parsed.push({
          ts: row.timestamp, source: "connect", type: "UNKNOWN",
          contactId: row.contactId || "(unparsed)", sessionId: "", raw: row.message, obj: null,
        });
        continue;
      }
      let cid = row.contactId || "";
      let span = null;
      if (!cid && obj.session_name) cid = obj.session_name;
      if (obj.span) {
        span = parseSpan(obj.span);
        if (!cid && span.session_name) cid = span.session_name;
      }
      parsed.push({
        ts: row.timestamp, source: "connect",
        type: SHORT_TYPE(obj.event_type), fullType: obj.event_type,
        contactId: cid, sessionId: obj.session_id || "",
        span: span, obj: obj, raw: row.message,
        translation: "", translationLang: "",
      });
    }
    return parsed;
  }

  // ---- 按需翻译某个 Contact 的客户/机器人对话 ----
  async function translateContact(cid) {
    if (translatingContacts.has(cid)) return;
    const list = groups[cid] || [];
    // 收集尚未翻译、且有正文的对话事件
    const items = [];
    for (const e of list) {
      const text = getTranslatableText(e);
      if (text && !e.translation) items.push({ e: e, text: text });
    }
    if (!items.length) {
      // 没有新内容需要翻译(可能已全部翻译过)
      translatedContacts.add(cid);
      renderSidebar(searchBox.value);
      if (activeCid === cid) renderTimeline(cid);
      return;
    }
    // 本地模式且已确认没有 /api: 不发请求, 保留原文
    if (!isCloudFrontMode() && API.state === "offline") {
      showNotice(t("translateOffline"), "warn");
      return;
    }
    translatingContacts.add(cid);
    renderSidebar(searchBox.value);
    try {
      // CloudFront 版用 API Gateway URL；本地 serve.py 版用相对路径
      const translateUrl = (TRANSLATE_CFG.apiUrl)
        ? TRANSLATE_CFG.apiUrl + "/translate"
        : "./api/translate";
      const res = await apiFetch(translateUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ texts: items.map((i) => i.text), lang: TRANSLATE_CFG.lang }),
        timeoutMs: 60000,
      });
      const data = res.data;
      if (res.ok && data && Array.isArray(data.translations)) {
        items.forEach((it, i) => {
          it.e.translation = data.translations[i] || "";
          it.e.translationLang = data.lang || TRANSLATE_CFG.lang;
        });
        translatedContacts.add(cid);
      } else if (!res.ok && !isCloudFrontMode() && !TRANSLATE_CFG.apiUrl && isApiUnavailable(res)) {
        // 本地后端不存在/不可达: 切到离线模式(此后不再请求), 原文保持不变
        setApiOffline(res.kind === "http" ? "http-" + res.status : res.kind, { notify: true });
      } else {
        // 翻译失败: 不写入 translation, 页面继续只显示原文
        showNotice(t("translateFailed") + (res.error || t("apiErrNonJson")), "err");
      }
    } catch (err) {
      showNotice(t("translateFailed") + (err && err.message ? err.message : String(err)), "err");
    } finally {
      translatingContacts.delete(cid);
      try {
        renderSidebar(searchBox.value);
        if (activeCid === cid) renderTimeline(cid);
        if (cdContactId === cid) updateCdTranslateBtn(cid);
      } catch (e) { /* 渲染失败不应产生未处理的 rejection */ }
    }
  }

  // ---- 读取本地服务器的翻译配置(可用性 / 默认语种) ----
  function loadTranslateConfig() {
    // CloudFront 版: auth.js 在 bootApp 前注入 window.__CF_TRANSLATE_CFG__
    if (window.__CF_TRANSLATE_CFG__) {
      TRANSLATE_CFG = {
        available: !!window.__CF_TRANSLATE_CFG__.available,
        lang: window.__CF_TRANSLATE_CFG__.lang || "中文",
        apiUrl: window.__CF_TRANSLATE_CFG__.apiUrl || "",
      };
      return Promise.resolve();
    }
    // 本地 serve.py 版: 探测 ./api/config(结果缓存; 非翻译服务器如 file:// 或 http.server -> 隐藏翻译按钮)
    return detectLocalApi().then(() => {}, () => {});
  }

  // ---- 渲染 Dashboard ----
  const dashContactRowsEl = document.getElementById("dashContactRows");
  const DASH_COLS = 30; // Contact 列表总列数(含 Action)

  // Channel 过滤（与切换实例模式一致，由 auth.js buildSwitcher 里的 select 调用）
  let dashChannelFilter = "";
  window.__CONNECT_SWITCH_CHANNEL__ = (value) => {
    dashChannelFilter = value || "";
    dashPage = 1;
    dashPageCache = {};
    if (typeof window.__CONNECT_FETCH_CONTACT_PAGE__ !== "function") { renderDashboard(); return; }
    var loadingEl = document.getElementById("dashLoading");
    if (loadingEl) loadingEl.style.display = "flex";
    window.__CONNECT_FETCH_CONTACT_PAGE__(dashPageSize, null, dashChannelFilter || undefined)
      .then((page) => {
        dashPageCache[1] = { items: page.items, nextToken: page.nextToken || null };
        rebuildFromSummaries(page.items);
        window.__CONNECT_AI_LOG_DATA__ = page.items;
        dashNextToken = page.nextToken || null;
        dashHasMore = !!page.nextToken;
        if (page.totalContacts != null) dashTotalContacts = page.totalContacts;
        renderDashboard();
      })
      .catch(() => { renderDashboard(); });
  };

  // N/A 占位: 值为空 / null 时显示 N/A
  function na(v) {
    return (v == null || v === "") ? t("na") : v;
  }

  // Dashboard 分页状态
  let dashPage = 1;               // 当前页(1-indexed)
  let dashPageSize = 15;          // 每页条数
  let dashNextToken = null;       // 下一页游标（当前页末尾的 token）
  let dashHasMore = false;        // 后端是否还有更多数据
  let dashTotalContacts = null;   // 总 contact 数（首次加载时从后端获取，null 表示未知）
  // 页数据缓存：dashPageCache[i] = 第 i 页的 { items, nextToken }（i 从 1 开始）
  // 切换 pageSize 或切换实例时清空
  let dashPageCache = {};

  // 更新分页组件: 页码显示 + 上一页/下一页按钮状态
  function updatePagination(total) {
    const info = document.getElementById("pageInfo");
    const btnPrev = document.getElementById("btnPagePrev");
    const btnNext = document.getElementById("btnPageNext");
    if (!info || !btnPrev || !btnNext) return;
    const totalLabel = dashTotalContacts != null ? dashTotalContacts : "?";
    const totalPages = dashTotalContacts != null
      ? Math.max(1, Math.ceil(dashTotalContacts / dashPageSize))
      : "?";
    info.textContent = total ? t("pageInfo", { page: dashPage, totalPages, total: totalLabel }) : "";
    btnPrev.disabled = dashPage <= 1;
    btnNext.disabled = !dashHasMore;
  }

  function renderDashboard() {
    const rows = contactIds
      .filter((c) => c !== "(未关联的 Gateway 日志)")
      .map((cid) => ({ cid: cid, meta: contactMeta[cid] || {} }));
    rows.sort((a, b) => (b.meta.minTs || 0) - (a.meta.minTs || 0));

    dashContactRowsEl.innerHTML = "";
    if (!rows.length) {
      dashContactRowsEl.innerHTML = `<tr><td colspan="${DASH_COLS}" style="color:var(--muted)">${esc(t("noContactData"))}</td></tr>`;
      updatePagination(0);
      const _loadingEl = document.getElementById("dashLoading");
      if (_loadingEl) _loadingEl.style.display = "none";
      return;
    }

    // 服务端分页：当前内存只有本页数据，直接全部渲染
    updatePagination(rows.length);

    for (const row of rows) {
      const cid = row.cid;
      const meta = row.meta;
      const en = ENRICH[cid] || {};

      const minTs = meta.minTs || null;
      const maxTs = meta.maxTs || null;
      const duration = (minTs && maxTs) ? Math.max(0, maxTs - minTs) : 0;

      // 比率格式化(默认 0，不显示 N/A)
      const srRate = meta.srIntent ? Math.round(((meta.srSuccess || 0) / meta.srIntent) * 100) + "%" : "0%";
      const latency = meta.responseLatency != null ? meta.responseLatency + " ms" : t("na");

      // Connect API 补充字段: DID/DisconnectReason/CSAT 直接读 DDB meta，summary 由 enrichContactsViaConnect 补充
      const summary = en.summary || "";
      const disconnect = meta.disconnectReason || "";
      const csat = meta.csat != null ? meta.csat : "";
      const aiCalls = meta.aiAgentCalls || 0;

      const cell = (v, cls) => `<td class="${cls || ""}${(v == null || v === "") ? " na" : ""}">${esc(v == null || v === "" ? t("na") : v)}</td>`;
      const numCell = (v) => `<td>${esc(v)}</td>`;

      const tr = document.createElement("tr");
      tr.className = "contact-row";
      tr.setAttribute("data-cid", cid);
      tr.innerHTML =
        cell(meta.channel || "", "channel-cell") +
        cell(meta.locale) +
        cell(meta.did || "", "did-cell") +
        `<td class="cid-cell"><span class="cid-value clickable" title="${esc(t("cdOpenTitle"))}">${esc(cid)}</span>` +
          `<button class="icon-btn js-copy-cid" title="${esc(t("titleCopy"))}">⧉</button></td>` +
        summaryCellHtml(cid) +
        `<td class="time-cell">${esc(formatDateTime(minTs))}</td>` +
        `<td class="time-cell">${esc(formatDateTime(maxTs))}</td>` +
        `<td class="time-cell">${esc(formatDuration(duration))}</td>` +
        `<td class="${meta.errors ? "num-bad" : ""}">${esc(meta.errors || 0)}</td>` +
        numCell(meta.faqAsk || 0) +
        numCell(meta.faqSend || 0) +
        cell(disconnect, "disc-cell") +
        numCell(meta.srIntent || 0) +
        numCell(meta.srSuccess || 0) +
        `<td>${esc(srRate)}</td>` +
        cell(meta.srNumber, "sr-num-cell") +
        numCell(meta.srRounds != null ? meta.srRounds : 0) +
        `<td>${esc(latency)}</td>` +
        cell("") /* 意图识别准确率: 需标注数据, N/A */ +
        cell("") /* 用户打断次数: 日志无 barge-in 标记, N/A */ +
        cell(csat, "csat-cell") +
        numCell(aiCalls) +
        numCell(meta.mcpToolCalls || 0) +
        `<td class="${meta.escalated ? "flag-yes" : ""}">${esc(meta.escalated ? t("flagYes") : t("flagNo"))}</td>` +
        `<td class="reason-cell${meta.escalationReason ? "" : " na"}">${esc(meta.escalationReason || t("na"))}</td>` +
        `<td class="${meta.guardBlocks ? "num-warn" : ""}">${esc(meta.guardBlocks || 0)}</td>` +
        guardReasonCellHtml(meta.guardReasons) +
        cell("") /* 静默超时次数: 日志无此信息, N/A */ +
        `<td class="act-cell"><button class="act-btn logs">${esc(t("btnLogs"))}</button></td>`;

      tr.querySelector(".act-btn.logs").addEventListener("click", () => openContactLogs(cid));
      tr.querySelector(".cid-value.clickable").addEventListener("click", () => openContactDetail(cid));
      // 拷贝 Contact ID(阻止冒泡，避免触发打开详情)
      const copyCidBtn = tr.querySelector(".js-copy-cid");
      copyCidBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        copyToClipboard(cid, copyCidBtn);
      });
      dashContactRowsEl.appendChild(tr);
    }
    // 绑定 guardrail 展开/收起按钮
    bindGuardToggles(dashContactRowsEl);
    // 行插入后再评估各摘要单元格是否溢出(需要真实布局), 决定是否显示「展开」按钮
    requestAnimationFrame(refreshAllSummaryToggles);
    // 渲染完成后隐藏 loading 覆盖层
    const _loadingEl = document.getElementById("dashLoading");
    if (_loadingEl) _loadingEl.style.display = "none";
  }

  // Guardrail 拦截原因单元格: 每条原因独立行 + guardrailName(大字) + policyType·details(小字) + ×N 次数
  function guardReasonCellHtml(reasons) {
    if (!reasons || !reasons.length) return `<td class="reason-cell na">${esc(t("na"))}</td>`;
    const items = reasons.map((r) => {
      const text = typeof r === "string" ? r : r.text;
      const count = typeof r === "string" ? 1 : r.count;
      const parts = String(text).split(" · ");
      const name = parts.shift();
      const rest = parts.map((p) => `<span class="guard-part">${esc(p)}</span>`).join("");
      return `<div class="guard-reason"><span class="guard-name">${esc(name)}</span>${rest}` +
        `<span class="guard-count" title="${esc(t("guardReasonCountTitle"))}">×${esc(count)}</span></div>`;
    }).join("");
    // 多条原因时默认折叠，显示展开/收起按钮
    if (reasons.length > 1) {
      return `<td class="reason-cell reason-cell-collapsible">` +
        `<div class="guard-reasons-wrap collapsed">${items}</div>` +
        `<button type="button" class="guard-toggle">${esc(t("expand"))}</button>` +
        `</td>`;
    }
    return `<td class="reason-cell">${items}</td>`;
  }

  // 绑定 guardrail 展开/收起按钮事件（在行插入 DOM 后调用）
  function bindGuardToggles(container) {
    container.querySelectorAll(".guard-toggle").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const wrap = btn.previousElementSibling;
        const collapsed = wrap.classList.toggle("collapsed");
        btn.textContent = collapsed ? t("expand") : t("collapse");
      });
    });
  }

  // 通话摘要列: 初始渲染为「加载」按钮，点击后才请求接口拉取并替换为可折叠文字
  function summaryCellHtml(cid) {
    // 离线模式(无 /api 且无 CloudFront 钩子): 「加载摘要」按钮不会起作用, 改为预抓取缓存文本或 N/A
    if (API.state === "offline" && typeof window.__CONNECT_DESCRIBE_CONTACT__ !== "function") {
      const cached = ((ENRICH[cid] || {}).summary) || "";
      if (cached) {
        return `<td class="summary-cell"><div class="summary-text">${esc(cached)}</div>` +
          `<button type="button" class="summary-toggle" style="display:none">${esc(t("expand"))}</button></td>`;
      }
      return `<td class="summary-cell na">${esc(t("na"))}</td>`;
    }
    return `<td class="summary-cell"><button type="button" class="act-btn summary-load-btn">${esc(t("btnLoadSummary"))}</button></td>`;
  }

  function loadSummaryForRow(cid) {
    var instanceId = extractInstanceId(cid) ||
      (window.__AWS_CONFIG__ && window.__AWS_CONFIG__.connectInstanceId) || "";
    if (!instanceId || typeof window.__CONNECT_DESCRIBE_CONTACT__ !== "function") return;
    var btn = dashContactRowsEl.querySelector(`tr[data-cid="${cid}"] .summary-load-btn`);
    if (!btn) return;
    btn.disabled = true;
    btn.textContent = t("metaLoading");
    window.__CONNECT_DESCRIBE_CONTACT__(cid, instanceId)
      .then(function (data) {
        var contact = data && (data.Contact || data);
        if (!contact) { btn.textContent = "N/A"; btn.disabled = false; return; }
        if (typeof window.__CONNECT_FETCH_SUMMARY__ === "function") {
          return window.__CONNECT_FETCH_SUMMARY__(contact).then(function (s) {
            if (s) { ENRICH[cid] = (ENRICH[cid] || {}); ENRICH[cid].summary = s; }
            renderSummaryCell(cid, s || "");
          });
        }
        renderSummaryCell(cid, "");
      })
      .catch(function () { btn.textContent = t("na"); btn.disabled = false; });
  }

  // 一次性加载当前页所有行的摘要(列头「加载全部摘要」按钮触发)
  function loadAllSummariesForPage() {
    if (!dashContactRowsEl) return;
    var allLoadBtns = dashContactRowsEl.querySelectorAll("tr.contact-row .summary-load-btn");
    allLoadBtns.forEach(function (btn) {
      var tr = btn.closest("tr.contact-row");
      if (tr) loadSummaryForRow(tr.getAttribute("data-cid"));
    });
  }

  function renderSummaryCell(cid, summary) {
    var td = dashContactRowsEl.querySelector(`tr[data-cid="${cid}"] .summary-cell`);
    if (!td) return;
    td.classList.remove("na");
    if (summary) {
      td.innerHTML = `<div class="summary-text">${esc(summary)}</div>` +
        `<button type="button" class="summary-toggle" style="display:none">${esc(t("expand"))}</button>`;
      requestAnimationFrame(function () { refreshSummaryToggle(td); });
    } else {
      td.textContent = t("na");
    }
  }

  // 检测某个摘要单元格在折叠态是否溢出(内容超过列宽/行数), 相应显示或隐藏「展开」按钮
  function refreshSummaryToggle(td) {
    const textEl = td.querySelector(".summary-text");
    const btn = td.querySelector(".summary-toggle");
    if (!textEl || !btn) return;
    const wasExpanded = td.classList.contains("expanded");
    td.classList.remove("expanded"); // 先回到折叠态才能测量溢出
    const overflow = textEl.scrollHeight - textEl.clientHeight > 1;
    if (overflow) {
      btn.style.display = "";
      if (wasExpanded) td.classList.add("expanded"); // 恢复用户之前的展开状态
      btn.textContent = td.classList.contains("expanded") ? t("collapse") : t("expand");
    } else {
      btn.style.display = "none";
      td.classList.remove("expanded");
    }
  }

  function refreshAllSummaryToggles() {
    if (!dashContactRowsEl) return;
    dashContactRowsEl.querySelectorAll(".summary-cell").forEach(refreshSummaryToggle);
  }

  // ---- 视图切换(Dashboard / 日志排查 / Contact 详情) ----
  const viewDashboard = document.getElementById("viewDashboard");
  const viewLog = document.getElementById("viewLog");
  const viewContact = document.getElementById("viewContact");
  const navDashboard = document.getElementById("navDashboard");
  const navLog = document.getElementById("navLog");
  const navContact = document.getElementById("navContact");

  function showView(name) {
    viewDashboard.classList.toggle("show", name === "dashboard");
    viewLog.style.display = name === "log" ? "flex" : "none";
    // viewContact 的 index.html 里带内联 style="display:none"，内联样式优先级高于
    // .contact-detail.show 的样式表规则，因此必须直接用内联 display 控制显隐。
    viewContact.style.display = name === "contact" ? "block" : "none";
    viewContact.classList.toggle("show", name === "contact");
    navDashboard.classList.toggle("active", name === "dashboard");
    navLog.classList.toggle("active", name === "log");
    navContact.classList.toggle("active", name === "contact");
  }

  // 从 Dashboard 点击「日志」: 切到日志排查视图并选中该 Contact
  function openContactLogs(cid) {
    showView("log");
    selectContact(cid);
  }

  // ---- Contact 详情(DescribeContact) ----
  const cdCidEl = document.getElementById("cdCid");
  const cdBodyEl = document.getElementById("cdBody");
  const cdRefreshBtn = document.getElementById("cdRefresh");
  const cdTranslateBtn = document.getElementById("cdTranslate");
  const cdViewLogsBtn = document.getElementById("cdViewLogs");
  let cdContactId = null;

  // 从日志里抽取某个 Contact 对应的 instanceId(DescribeContact 需要)
  function extractInstanceId(cid) {
    const list = groups[cid] || [];
    for (const e of list) {
      const raw = e.raw || "";
      let m = raw.match(/[-"']?\s*instanceId["']?\s*[:=]\s*["']?([0-9a-fA-F-]{20,})/);
      if (m) return m[1];
      // ai_agent_arn / prompt_arn 里也可能带 instance 段, 但优先用显式 instanceId
    }
    return "";
  }

  // 点击 Contact ID: 显示「Contact 详情」tab 并加载 DescribeContact 结果
  function openContactDetail(cid) {
    cdContactId = cid;
    navContact.style.display = "";
    cdCidEl.textContent = cid;
    showView("contact");
    loadDescribeContact(cid).catch(() => {});
    updateCdTranslateBtn(cid);
  }

  // 更新详情页翻译按钮的可见性与状态
  function updateCdTranslateBtn(cid) {
    if (!cdTranslateBtn || !cdViewLogsBtn) return;
    if (!TRANSLATE_CFG.available || !cid || cid === "(未关联的 Gateway 日志)") {
      cdTranslateBtn.style.display = "none";
      cdViewLogsBtn.style.display = "none";
      return;
    }
    const translated = translatedContacts.has(cid);
    const translating = translatingContacts.has(cid);
    if (translating) {
      cdTranslateBtn.textContent = t("cdTranslating");
      cdTranslateBtn.disabled = true;
      cdTranslateBtn.style.display = "";
      cdViewLogsBtn.style.display = "none";
    } else if (translated) {
      cdTranslateBtn.textContent = t("cdRetranslate");
      cdTranslateBtn.disabled = false;
      cdTranslateBtn.style.display = "";
      cdViewLogsBtn.style.display = "";
    } else {
      cdTranslateBtn.textContent = t("cdTranslate");
      cdTranslateBtn.disabled = false;
      cdTranslateBtn.style.display = "";
      cdViewLogsBtn.style.display = "none";
    }
  }

  // 拉取并渲染 DescribeContact 结果:
  //   1) 优先请求本地预览服务器的 ./api/describe-contact(实时调用 Amazon Connect)
  //   2) 回退到预抓取的 connect-enrich.js 里的 describeContact 缓存
  //   3) 都没有 -> 提示如何启用
  async function loadDescribeContact(cid, forceRefresh) {
    cdBodyEl.innerHTML = `<div class="cd-status">${esc(t("cdLoading"))}</div>`;
    const instanceId = extractInstanceId(cid) ||
      (window.__AWS_CONFIG__ && window.__AWS_CONFIG__.connectInstanceId) || "";

    // 1) CloudFront 部署: 浏览器用 AWS SDK + Cognito 临时凭证直接调用 DescribeContact
    //    (结果按 Contact 缓存，forceRefresh=true 时绕过缓存重新拉取)
    if (typeof window.__CONNECT_DESCRIBE_CONTACT__ === "function") {
      try {
        const data = await window.__CONNECT_DESCRIBE_CONTACT__(cid, instanceId, forceRefresh);
        const contact = data && (data.Contact || data);
        if (contact && (contact.Id || contact.Arn || contact.Channel)) {
          renderDescribeContact(cid, contact, "api");
          return;
        }
        throw new Error("DescribeContact 返回空结果");
      } catch (err) {
        const cached = (ENRICH[cid] || {}).describeContact;
        if (cached) { renderDescribeContact(cid, cached.Contact || cached, "cache"); return; }
        cdBodyEl.innerHTML =
          `<div class="cd-status err">${esc(t("cdError"))}${esc(err && err.message ? err.message : String(err))}</div>` +
          `<div class="cd-status">${esc(t("cdHint"))}</div>`;
        return;
      }
    }

    // 2) 本地 serve.py: /api/describe-contact 端点(无 /api 时直接回退, 不发请求)
    const cached = (ENRICH[cid] || {}).describeContact;
    const showOffline = () => {
      if (cached) {
        renderDescribeContact(cid, cached.Contact || cached, "cache");
        return;
      }
      cdBodyEl.innerHTML =
        `<div class="cd-status">${esc(t("cdOffline"))}</div>` +
        `<div class="cd-status">${esc(t("cdOfflineHint"))}</div>`;
    };
    try {
      await detectLocalApi();
      if (API.state === "offline") { showOffline(); return; }
      const qs = "contactId=" + encodeURIComponent(cid) +
        (instanceId ? "&instanceId=" + encodeURIComponent(instanceId) : "");
      const res = await apiFetch("./api/describe-contact?" + qs, { timeoutMs: 15000 });
      const data = res.data;
      if (res.ok && data && data.Contact) {
        renderDescribeContact(cid, data.Contact, "api");
        return;
      }
      if (res.ok && data && !data.Contact && !data.error) {
        // 有的 API 直接返回 Contact 字段在顶层
        renderDescribeContact(cid, data, "api");
        return;
      }
      if (isApiUnavailable(res)) {
        setApiOffline(res.kind === "http" ? "http-" + res.status : res.kind);
        showOffline();
        return;
      }
      throw new Error((data && data.error) || res.error || ("HTTP " + res.status));
    } catch (err) {
      // 回退: enrich 缓存
      if (cached) {
        renderDescribeContact(cid, cached.Contact || cached, "cache");
        return;
      }
      cdBodyEl.innerHTML =
        `<div class="cd-status err">${esc(t("cdError"))}${esc(err && err.message ? err.message : String(err))}</div>` +
        `<div class="cd-status">${esc(t("cdHint"))}</div>`;
    }
  }

  // 渲染 DescribeContact 的 Contact 对象: 关键字段表 + 完整 JSON
  function renderDescribeContact(cid, contact, srcKind) {
    const rows = [
      ["Id", contact.Id],
      ["Arn", contact.Arn],
      ["Channel", contact.Channel],
      ["InitiationMethod", contact.InitiationMethod],
      ["Name", contact.Name],
      ["Description", contact.Description],
      ["InitiationTimestamp", fmtMaybeTime(contact.InitiationTimestamp)],
      ["DisconnectTimestamp", fmtMaybeTime(contact.DisconnectTimestamp)],
      ["LastUpdateTimestamp", fmtMaybeTime(contact.LastUpdateTimestamp)],
      ["ScheduledTimestamp", fmtMaybeTime(contact.ScheduledTimestamp)],
      ["DisconnectReason", contact.DisconnectReason],
      ["QueueInfo", contact.QueueInfo ? JSON.stringify(contact.QueueInfo) : ""],
      ["AgentInfo", contact.AgentInfo ? JSON.stringify(contact.AgentInfo) : ""],
      ["RoutingCriteria", contact.RoutingCriteria ? JSON.stringify(contact.RoutingCriteria) : ""],
      ["Tags", contact.Tags ? JSON.stringify(contact.Tags) : ""],
    ];
    let html = "";
    const srcLabel = srcKind === "cache" ? t("cdSrcCache") : t("cdSrcApi");
    html += `<div class="cd-status">${esc(t("cdSource", { src: srcLabel }))}</div>`;
    html += `<table class="cd-fields"><tbody>`;
    for (const [k, v] of rows) {
      if (v === undefined || v === null || v === "") continue;
      html += `<tr><th>${esc(k)}</th><td>${esc(String(v))}</td></tr>`;
    }
    html += `</tbody></table>`;
    html += `<div class="cd-section-title">${esc(t("cdRawJson"))}</div>`;
    let pretty = "";
    try { pretty = JSON.stringify(contact, null, 2); } catch (e) { pretty = String(contact); }
    html += `<pre class="cd-json">${esc(pretty)}</pre>`;
    // 「完整 JSON」下方再展示一段包含 AutomatedInteractionSummary 的 JSON(异步填充)
    html += `<div id="cdSummaryBox"></div>`;
    cdBodyEl.innerHTML = html;
    fillSummaryJson(cid, contact);
  }

  // 在 Contact 详情里渲染"自动交互摘要"的 JSON 块。摘要不在 DescribeContact 返回体里,
  // 已缓存则直接用, 否则(CloudFront 版)按需从 S3 分析结果拉取后再填充。
  function fillSummaryJson(cid, contact) {
    const box = document.getElementById("cdSummaryBox");
    if (!box) return;
    const render = (summary) => {
      if (!summary || box !== document.getElementById("cdSummaryBox")) return;
      const obj = { ContactSummary: { AutomatedInteractionSummary: { Content: summary } } };
      let j = "";
      try { j = JSON.stringify(obj, null, 2); } catch (e) { j = String(summary); }
      box.innerHTML = `<div class="cd-section-title">${esc(t("cdSummaryJson"))}</div>` +
        `<pre class="cd-json">${esc(j)}</pre>`;
    };
    const existing = (ENRICH[cid] || {}).summary;
    if (existing) { render(existing); return; }
    if (typeof window.__CONNECT_FETCH_SUMMARY__ === "function" && contact) {
      window.__CONNECT_FETCH_SUMMARY__(contact).then((s) => {
        if (s) {
          const e = ENRICH[cid] || (ENRICH[cid] = {});
          e.summary = s;
          render(s);
        }
      }).catch(() => { /* 拉取失败: 不显示该块 */ });
    }
  }

  // 时间戳格式化: 支持 epoch(秒/毫秒) 或 ISO 字符串
  function fmtMaybeTime(v) {
    if (v == null || v === "") return "";
    let d;
    if (typeof v === "number") {
      d = new Date(v > 1e12 ? v : v * 1000);
    } else {
      const n = Number(v);
      d = isNaN(n) ? new Date(v) : new Date(n > 1e12 ? n : n * 1000);
    }
    return isNaN(d.getTime()) ? String(v) : d.toLocaleString([], { hour12: false }) + "  (" + String(v) + ")";
  }

  // 从 DescribeContact 的 Contact 对象里取"接电话的 DID"(系统侧号码)
  //   优先 SystemEndpoint.Address, 回退到 Tags["aws:connect:systemEndpoint"]
  function pickDidFromContact(contact) {
    if (!contact) return "";
    const ep = contact.SystemEndpoint || contact.systemEndpoint;
    if (ep && (ep.Address || ep.address)) return ep.Address || ep.address;
    const tags = contact.Tags || {};
    if (tags["aws:connect:systemEndpoint"]) return tags["aws:connect:systemEndpoint"];
    return "";
  }

  // CloudFront 部署下: 用浏览器 SDK 为每个 Contact 调用 DescribeContact，
  // 派生「接电话的 DID」「挂断方(DisconnectReason)」并缓存完整结果，随后刷新列表。
  // 更新顶部的 Contact 总数（随语言变化）
  function updateGlobalMeta() {
    // 优先使用后端返回的总 contact 数；未知时回退到当前内存中的条数
    const displayTotal = dashTotalContacts != null ? dashTotalContacts : contactIds.length;
    document.getElementById("globalMeta").textContent =
      t("meta", { m: displayTotal });
  }

  // 切换界面语言: 保存选择、刷新静态文案并重绘所有动态视图
  function applyLanguage(lang) {
    if (!I18N[lang]) lang = DEFAULT_LANG;
    currentLang = lang;
    try { localStorage.setItem("connectAiUiLang", lang); } catch (e) { /* 忽略 */ }
    applyStaticI18n();
    updateGlobalMeta();
    refreshOfflineUi();
    renderSidebar(searchBox.value);
    renderDashboard();
    // selectedTitle 在选中会话后显示 Contact ID，未选中时显示占位文案
    if (activeCid) {
      selectedTitle.textContent = activeCid;
      renderTimeline(activeCid);
    }
  }

  // ---- 初始化 ----
  function init() {
    // CloudFront 版: 如果接收到的 RAW 是 summaries（含 _summaryOnly 标记），用 rebuildFromSummaries 重建
    if (RAW.length > 0 && RAW[0]._summaryOnly) {
      rebuildFromSummaries(RAW);
    }

    // 分页状态必须在第一次 renderDashboard 之前初始化，否则 updatePagination 拿不到 dashTotalContacts
    dashNextToken = window.__CONNECT_INITIAL_NEXT_TOKEN__ || null;
    dashHasMore = !!dashNextToken;
    dashTotalContacts = window.__CONNECT_TOTAL_CONTACTS__ != null ? window.__CONNECT_TOTAL_CONTACTS__ : null;
    dashPageCache = {
      1: { items: window.__CONNECT_AI_LOG_DATA__ || [], nextToken: dashNextToken }
    };

    applyStaticI18n();
    updateGlobalMeta();
    // 同步确定 /api 是否可用(构建脚本写入的标记 / file://), 让首屏就是正确状态; 否则异步探测一次
    try { detectLocalApi().catch(() => {}); } catch (e) { /* 探测失败不影响渲染 */ }
    // 语言下拉框
    const langSelect = document.getElementById("langSelect");
    if (langSelect) {
      langSelect.value = currentLang;
      langSelect.addEventListener("change", () => applyLanguage(langSelect.value));
    }
    renderFilters();
    renderSidebar("");
    renderDashboard();

    // 搜索框：有内容时调后端 contactId 前缀搜索，空时恢复本地列表（防抖 300ms）
    let _searchTimer = null;
    let _searchGroups = null;     // 后端搜索结果暂存（null 表示不在搜索模式）
    let _searchContactMeta = null;
    let _searchContactIds = null;

    function renderSidebarSearch(items) {
      // 临时用搜索结果替换全局数据进行渲染，渲染后恢复
      const savedGroups = groups;
      const savedMeta = contactMeta;
      const savedIds = contactIds;
      groups = _searchGroups;
      contactMeta = _searchContactMeta;
      contactIds = _searchContactIds;
      renderSidebar("");
      groups = savedGroups;
      contactMeta = savedMeta;
      contactIds = savedIds;
    }

    searchBox.addEventListener("input", () => {
      clearTimeout(_searchTimer);
      const q = searchBox.value.trim();
      if (!q) {
        // 清空搜索：恢复本地列表
        _searchGroups = null;
        _searchContactMeta = null;
        _searchContactIds = null;
        renderSidebar("");
        return;
      }
      _searchTimer = setTimeout(() => {
        if (typeof window.__CONNECT_SEARCH_CONTACTS__ !== "function") {
          // 降级：前端过滤
          renderSidebar(q);
          return;
        }
        // 显示加载中占位
        contactListEl.innerHTML = '<div class="empty" style="margin-top:30px;font-size:13px">搜索中…</div>';
        window.__CONNECT_SEARCH_CONTACTS__(q, 20)
          .then((result) => {
            // 仅当搜索框内容未变时才更新（避免旧请求覆盖新结果）
            if (searchBox.value.trim() !== q) return;
            const items = result.items || [];
            _searchGroups = {};
            _searchContactMeta = {};
            for (const s of items) {
              const cid = s.contactId;
              if (!cid) continue;
              _searchGroups[cid] = groups[cid] || [];
              const m = contactMeta[cid] || {
                minTs: s.minTs || 0, count: s.count || 0, maxTs: s.maxTs || 0,
                sourceCounts: s.sourceCounts || {}, locale: s.locale || "",
                bu: s.bu || "", mcpToolCalls: s.mcpToolCalls || 0,
                faqAsk: s.faqAsk || 0, faqSend: s.faqSend || 0,
                srIntent: s.srIntent || 0, srSuccess: s.srSuccess || 0,
                srNumber: s.srNumber || "", srRounds: s.srRounds != null ? s.srRounds : null,
                aiAgentCalls: s.aiAgentCalls || 0, errors: s.errors || 0,
                guardBlocks: s.guardBlocks || 0, guardReasons: s.guardReasons || [],
                responseLatency: s.responseLatency != null ? s.responseLatency : null,
                escalated: s.escalated || false, escalationReason: s.escalationReason || "",
              };
              mergeEscOverride(cid, m);
              _searchContactMeta[cid] = m;
            }
            _searchContactIds = Object.keys(_searchGroups);
            renderSidebarSearch();
          })
          .catch(() => {
            if (searchBox.value.trim() !== q) return;
            renderSidebar(q); // 降级：前端过滤
          });
      }, 300);
    });

    // 导航事件
    navDashboard.addEventListener("click", () => showView("dashboard"));
    navLog.addEventListener("click", () => showView("log"));
    navContact.addEventListener("click", () => { if (cdContactId) showView("contact"); });

    // 详情页翻译按钮
    if (cdTranslateBtn) {
      cdTranslateBtn.addEventListener("click", () => {
        if (!cdContactId) return;
        // 如果事件明细尚未加载（CloudFront 按需拉取），先加载再翻译
        const ensureLoaded = (
          typeof window.__CONNECT_LOAD_CONTACT_EVENTS__ === "function"
          && (!groups[cdContactId] || groups[cdContactId].length === 0)
        )
          ? window.__CONNECT_LOAD_CONTACT_EVENTS__(cdContactId).then((rows) => {
              if (!groups[cdContactId] || groups[cdContactId].length === 0) {
                groups[cdContactId] = parseRawRows(rows);
                applyEscalationFromEvents(cdContactId);
              }
            })
          : Promise.resolve();
        ensureLoaded.then(() => translateContact(cdContactId))
          .then(() => updateCdTranslateBtn(cdContactId))
          .catch(() => updateCdTranslateBtn(cdContactId));
      });
    }
    if (cdViewLogsBtn) {
      cdViewLogsBtn.addEventListener("click", () => {
        if (!cdContactId) return;
        openContactLogs(cdContactId);
      });
    }
    // Dashboard 分页控件
    const pageSizeSelect = document.getElementById("pageSizeSelect");
    const btnPagePrev = document.getElementById("btnPagePrev");
    const btnPageNext = document.getElementById("btnPageNext");

    // 切换到指定页缓存并渲染（不发请求）
    function showCachedPage(page) {
      const cached = dashPageCache[page];
      rebuildFromSummaries(cached.items);
      dashNextToken = cached.nextToken;
      dashHasMore = !!cached.nextToken;
      dashPage = page;
      renderDashboard();
    }

    // 在屏幕居中显示 loading 覆盖层（翻页/切换实例请求后端期间调用）
    function showTableLoading() {
      const el = document.getElementById("dashLoading");
      if (el) el.style.display = "flex";
    }

    // 隐藏 loading 覆盖层
    function hideTableLoading() {
      const el = document.getElementById("dashLoading");
      if (el) el.style.display = "none";
    }

    // 从后端拉下一页并缓存
    function loadNextPage() {
      if (typeof window.__CONNECT_FETCH_CONTACT_PAGE__ !== "function") return;
      if (btnPagePrev) btnPagePrev.disabled = true;
      if (btnPageNext) btnPageNext.disabled = true;
      showTableLoading();
      window.__CONNECT_FETCH_CONTACT_PAGE__(dashPageSize, dashNextToken, dashChannelFilter || undefined)
        .then((page) => {
          dashPage++;
          dashPageCache[dashPage] = { items: page.items, nextToken: page.nextToken || null };
          rebuildFromSummaries(page.items);
          window.__CONNECT_AI_LOG_DATA__ = page.items;
          dashNextToken = page.nextToken || null;
          dashHasMore = !!page.nextToken;
          renderDashboard();
        })
        .catch(() => { renderDashboard(); });
    }

    if (pageSizeSelect) {
      pageSizeSelect.value = String(dashPageSize);
      pageSizeSelect.addEventListener("change", () => {
        dashPageSize = Number(pageSizeSelect.value) || 15;
        dashPage = 1;
        dashPageCache = {};
        if (typeof window.__CONNECT_FETCH_CONTACT_PAGE__ !== "function") return;
        showTableLoading();
        window.__CONNECT_FETCH_CONTACT_PAGE__(dashPageSize, null, dashChannelFilter || undefined)
          .then((page) => {
            dashPageCache[1] = { items: page.items, nextToken: page.nextToken || null };
            rebuildFromSummaries(page.items);
            window.__CONNECT_AI_LOG_DATA__ = page.items;
            dashNextToken = page.nextToken || null;
            dashHasMore = !!page.nextToken;
            if (page.totalContacts != null) dashTotalContacts = page.totalContacts;
            renderDashboard();
          })
          .catch(() => { renderDashboard(); });
      });
    }
    if (btnPagePrev) {
      btnPagePrev.addEventListener("click", () => {
        if (dashPage > 1) {
          showCachedPage(dashPage - 1);
        }
      });
    }
    if (btnPageNext) {
      btnPageNext.addEventListener("click", () => {
        if (dashPageCache[dashPage + 1]) {
          // 已缓存，直接读
          showCachedPage(dashPage + 1);
        } else if (dashHasMore) {
          // 未缓存，从后端拉
          loadNextPage();
        }
      });
    }

    // 通话摘要「展开/收起」按钮(事件委托, 适配重建行/异步补数据)
    if (dashContactRowsEl) {
      dashContactRowsEl.addEventListener("click", (ev) => {
        const loadBtn = ev.target.closest(".summary-load-btn");
        if (loadBtn) {
          const tr = loadBtn.closest("tr.contact-row");
          if (tr) loadSummaryForRow(tr.getAttribute("data-cid"));
          return;
        }
        const btn = ev.target.closest(".summary-toggle");
        if (!btn) return;
        const td = btn.closest(".summary-cell");
        if (!td) return;
        const expanded = td.classList.toggle("expanded");
        btn.textContent = expanded ? t("collapse") : t("expand");
      });
    }
    // 列头「加载全部摘要」按钮
    var loadAllBtn = document.getElementById("btnLoadAllSummaries");
    if (loadAllBtn) {
      loadAllBtn.addEventListener("click", function () {
        loadAllSummariesForPage();
      });
    }
    // 窗口尺寸变化会影响列宽/换行, 需重新评估是否显示「展开」按钮(防抖)
    let _sumResizeTimer = null;
    window.addEventListener("resize", () => {
      clearTimeout(_sumResizeTimer);
      _sumResizeTimer = setTimeout(refreshAllSummaryToggles, 150);
    });

    // 不自动选中第一个 Contact，避免进入总览面板就触发事件明细请求
    // 用户从 Dashboard 点击「日志」按钮时再按需加载
    // 探测翻译能力，成功后重绘侧栏以显示翻译按钮
    loadTranslateConfig().then(() => {
      refreshOfflineUi();
      renderSidebar(searchBox.value);
      if (cdContactId) updateCdTranslateBtn(cdContactId);
    }).catch(() => {});

    // 切换实例时由 auth.js 调用，传入新实例的摘要列表，重建数据并刷新所有视图
    window.__CONNECT_SWITCH_INSTANCE__ = function (instanceName, summaries, nextToken, totalContacts) {
      for (const k of Object.keys(escOverride)) delete escOverride[k];
      rebuildFromSummaries(summaries);
      activeCid = null;
      dashPage = 1;
      dashNextToken = nextToken || null;
      dashHasMore = !!nextToken;
      dashTotalContacts = totalContacts != null ? totalContacts : null;
      dashPageCache = { 1: { items: summaries, nextToken: nextToken || null } };
      // 切换实例时重置 channel 过滤（DOM 由 auth.js buildSwitcher 重建，自然为空）
      dashChannelFilter = "";
      selectedTitle.textContent = "";
      timelineEl.innerHTML = "";
      searchBox.value = "";
      renderFilters();
      renderSidebar("");
      renderDashboard();
      updateGlobalMeta();
      showView("dashboard");
    };

    // 默认展示 Dashboard
    showView("dashboard");
  }

  init();
})();
