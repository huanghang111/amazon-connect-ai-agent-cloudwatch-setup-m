#!/usr/bin/env python3
"""
process_events_logs.py

用途:
  S3 事件触发的 Lambda（函数名 connect-ai-logs-<account_id>-process-events，
  桶事件通知 Id process-events-logs）。监听日志桶
  events/<instanceName>/<contactId>.json.gz 的 ObjectCreated 事件，对每个新写入的
  事件明细文件：
    1) 读取并解压（gzip JSON 数组，每项 {timestamp, message, source, contactId}）
    2) 按 source 拆分 connect(AI Agent) / gateway(AgentCore Gateway) 日志
    3) 用本文件内置的会话解析逻辑解析 AI Agent 会话与 faqSearch 调用；再补充
       TRANSCRIPT_AI_AGENT_TRACE 的 execute_tool(faqSearch) span 分析与
       gateway 日志中的 faqSearch 记录
    4) 调用 Connect DescribeContact / GetContactAttributes 获取渠道与联系人属性
    5) 每个文件输出一行结构化 JSON 到本函数的 CloudWatch 日志

环境变量:
  INSTANCES_META  JSON: {instanceName(InstanceAlias): {"instanceId","region","configKey"}}
                  由 setup_cloudfront.py 写入；用于定位 Connect 实例与 region
  LOGS_BUCKET     日志桶名（仅作信息记录）
  MAX_LOG_BYTES   单条日志行最大字节数，默认 200000（CloudWatch 单事件上限 256 KB）

日志行类型(type 字段):
  process-events-logs.result  每个 events 文件一条解析结果
  process-events-logs.chunk   结果过大时分片输出（按 part 顺序拼接 data 即为完整 JSON）
  process-events-logs.skip    非 events/*.json.gz 的对象被跳过
  process-events-logs.error   处理单条 S3 记录时出现意外异常

result 记录主要字段:
  bucket / key / eTag / versionId / sequencer / size / eventName / eventTime   S3 事件信息
  instanceName / instanceId / connectRegion / contactId                        定位信息
  contact + contactError         DescribeContact 的渠道、时间戳、队列等（失败时写 error）
  attributes + attributesError   GetContactAttributes 的全部属性（失败时写 error）
  analysis
    counts                       各类日志行计数
    transcript                   inputEventType + sessions（对话 turns 与 faq_events）
    faqSearch                    transcriptCalls/Hits、toolSpans、gateway
    logSummary                   与 DynamoDB 摘要同口径的统计（失败时为 logSummaryError）
  processingError                读取/解析 events 文件失败的原因
  object                         S3 对象元数据（读取成功时才有）
  truncated                      长字段被截断时为 true

文件结构（自上而下）:
  常量与配置 → 通用工具 → S3 输入读取 → 会话解析 → faqSearch 分析(span/gateway)
  → 日志行汇总 → Connect 联系人补充 → 日志输出(截断/分片) → 入口
"""
import dataclasses
import datetime
import gzip
import json
import os
import re
import traceback
import urllib.parse

import boto3
from botocore.config import Config

# contact_summary.py 由 setup_cloudfront.py 从 lib/lambda/ 一起打包进部署包
# （与解析 Lambda handler.py 共用，用于 trace span 解析和日志摘要）
from contact_summary import (
    _parse_span,
    _short_tool_name,
    _tool_name_from_span,
    _tool_output_text,
    _tool_succeeded,
    build_contact_summary,
)


# ---------------------------------------------------------------------------
# 常量与配置
# ---------------------------------------------------------------------------

LOG_TYPE = "process-events-logs"
DEFAULT_MAX_LOG_BYTES = 200000
MIN_LOG_BYTES = 1000            # MAX_LOG_BYTES 配得过小时的下限
TRUNCATE_FIELD_CHARS = 2000     # 超限时每个字符串字段保留的字符数
PREVIEW_CHARS = 1000            # 工具输出 / gateway 日志预览的字符数
CHUNK_OVERHEAD_SLACK = 32       # 分片行除 data 外的预留字节
MAX_BYTES_PER_CHAR = 4          # 一个字符序列化后最多 4 字节（UTF-8 4 字节字符 / 转义）

# faqSearch 工具名在不同实例前缀不同（uat___faqSearch /
# agentcore_mcp_..._gateway___faqSearch），统一用子串匹配。
# 注意两处匹配的大小写口径不同（保持历史行为）：
#   会话解析中的 toolUse.name 区分大小写（FAQ_TOOL_SUBSTR in name）；
#   trace span 工具名与 gateway 日志文本不区分大小写（_FAQ_RE）。
FAQ_TOOL_SUBSTR = "faqSearch"
_FAQ_RE = re.compile(re.escape(FAQ_TOOL_SUBSTR), re.IGNORECASE)

_KEY_RE = re.compile(r"^events/([^/]+)/([^/]+)\.json\.gz$")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_GZIP_MAGIC = b"\x1f\x8b"
_GATEWAY_ERROR_RE = re.compile(r"\b(ERROR|Exception|Traceback|FATAL)\b")
_GATEWAY_LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\b")
_GATEWAY_LEVEL_KEYS = ("level", "levelname", "log_level", "severity_text", "severityText", "severity")

# 会话解析的输入事件类型（按优先级）与 trace 事件类型
EVENT_AGENTIC_MESSAGE = "TRANSCRIPT_AGENTIC_MESSAGE"
EVENT_LLM_INVOCATION = "TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION"
EVENT_AI_AGENT_TRACE = "TRANSCRIPT_AI_AGENT_TRACE"

# DescribeContact 中保留的字段（输出时首字母小写）
_CONTACT_FIELDS = (
    "Channel", "InitiationMethod", "InitiationTimestamp", "DisconnectTimestamp",
    "LastUpdateTimestamp", "DisconnectReason", "InitialContactId",
    "PreviousContactId", "QueueInfo", "AgentInfo", "SystemEndpoint",
)
# build_contact_summary 结果中输出到 analysis.logSummary 的字段
_LOG_SUMMARY_FIELDS = (
    "count", "minTs", "maxTs", "mcpToolCalls", "faqAsk", "faqSend",
    "errors", "escalated", "escalationReason", "locale",
)
SKIP_REASON = "not events/<instanceName>/<contactId>.json.gz"
NO_INSTANCE_REASON = "instance not in INSTANCES_META"
NOT_CONTACT_ID_REASON = "not a contact id"

_BOTO_CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"})
# 客户端在 Lambda 容器内复用（测试会重置这两个缓存）
_s3_client = None
_connect_clients = {}


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", config=_BOTO_CFG)
    return _s3_client


def _connect(region: str):
    """Connect 实例可能分布在不同 region，按 region 缓存客户端。"""
    if region not in _connect_clients:
        _connect_clients[region] = boto3.client(
            "connect", region_name=region, config=_BOTO_CFG
        )
    return _connect_clients[region]


def _max_log_bytes() -> int:
    try:
        return max(MIN_LOG_BYTES, int(os.environ.get("MAX_LOG_BYTES", DEFAULT_MAX_LOG_BYTES)))
    except (TypeError, ValueError):
        return DEFAULT_MAX_LOG_BYTES


def _instances_meta() -> dict:
    try:
        meta = json.loads(os.environ.get("INSTANCES_META") or "{}")
    except (TypeError, ValueError):
        return {}
    return meta if isinstance(meta, dict) else {}


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------

def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _print_line(value) -> None:
    """输出一行 JSON 到 CloudWatch（flush 保证 Lambda 结束前写出）。"""
    print(_dumps(value), flush=True)


def _jsonable(value):
    """datetime -> ISO 字符串（递归处理 dict/list）。"""
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _truncate_text(text: str, limit: int) -> str:
    if len(text) > limit:
        return text[:limit] + f"…[truncated {len(text) - limit} chars]"
    return text


def _preview(value, limit: int = PREVIEW_CHARS) -> str:
    """把任意值转成截断后的文本预览（非字符串先序列化为 JSON）。"""
    if value is None:
        return ""
    text = value if isinstance(value, str) else _dumps(value)
    return _truncate_text(text, limit)


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _lower_first(name: str) -> str:
    return name[:1].lower() + name[1:]


def _client_error_text(error: Exception) -> str:
    """ClientError -> "Code: Message"；其他异常用类型名作 Code。"""
    response = getattr(error, "response", None) or {}
    err = response.get("Error") or {}
    code = err.get("Code") or type(error).__name__
    message = err.get("Message") or str(error)
    return f"{code}: {message}"


def _json_loads_or_none(text):
    """解析 JSON；失败返回 None（会话解析中 "解析失败" 与 "null" 的处理结果相同）。"""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


# ---------------------------------------------------------------------------
# S3 输入读取
# ---------------------------------------------------------------------------

def _s3_object(s3_record: dict) -> dict:
    return (s3_record.get("s3") or {}).get("object") or {}


def _object_key(s3_object: dict) -> str:
    """S3 事件中的 key 是 URL 编码的（空格为 +），需先解码。"""
    return urllib.parse.unquote_plus(s3_object.get("key", ""))


def _parse_key(key: str):
    """events/<instanceName>/<contactId>.json.gz -> (instanceName, contactId)；否则 None。"""
    match = _KEY_RE.match(key or "")
    if not match:
        return None
    return match.group(1), match.group(2)


def _load_rows(bucket: str, key: str) -> tuple:
    """读取 events 文件，返回 (rows, meta)。

    splitter 写入时带 ContentEncoding=gzip，boto3 get_object 不会自动解压，
    Body 是 gzip 字节；若已是明文 JSON 也兼容。
    """
    response = _s3().get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    if body[:2] == _GZIP_MAGIC:
        body = gzip.decompress(body)
    rows = json.loads(body.decode("utf-8"))
    if not isinstance(rows, list):
        raise ValueError("events file is not a JSON array")
    meta = {
        "contentLength": response.get("ContentLength"),
        "lastModified": _jsonable(response.get("LastModified")),
    }
    return rows, meta


# ---------------------------------------------------------------------------
# 会话解析: AI Agent prompt -> 对话 turns + faqSearch 事件
#
# 输入是 TRANSCRIPT_AGENTIC_MESSAGE（或同为 Converse 结构的
# LARGE_LANGUAGE_MODEL_INVOCATION）的 message JSON，含字段:
#   session_id、event_timestamp、prompt（JSON 字符串），prompt 反序列化后:
#     messages: [{role: user|assistant, content: [item ...]}]，item 可能是
#       {"text": "..."}                                user=客户输入;
#                                                      assistant=<message>..</message><thinking>..</thinking>
#       {"guardContent": {"text": {"text": "..."}}}    包裹后的文本
#       {"toolUse": {"toolUseId", "name", "input"}}   工具调用（faqSearch 等）
#       {"toolResult": {"toolUseId", "content": [{"text": "..."}]}}   工具结果
#     system: [{"text": "...system prompt..."}, {"cachePoint": ...}]
#       system prompt 末尾的 <system-variables> 块含 contactId / sessionId /
#       instanceId / responseLanguage 等。
# 同一 session 会产生多条消息，每条 messages 都是累积快照，
# 因此每个 session 只取时间戳最大的一条即可得到最完整的会话。
# ---------------------------------------------------------------------------

_MESSAGE_RE = re.compile(r"<message>(.*?)</message>", re.DOTALL)
_THINKING_RE = re.compile(r"<thinking>.*?</thinking>", re.DOTALL)
_SYSVAR_RE = re.compile(r"<system-variables>(.*?)</system-variables>", re.DOTALL)


def _extract_bot_message(text: str) -> str:
    """从 assistant 文本中取 <message> 内容（BOT 对客户的真实回复），排除 <thinking>。

    若不含 <message> 标签，则去掉 <thinking> 后返回剩余纯文本。
    """
    if not text:
        return ""
    messages = _MESSAGE_RE.findall(text)
    if messages:
        return "\n".join(m.strip() for m in messages).strip()
    return _THINKING_RE.sub("", text).strip()


def _content_text(item: dict):
    """从一个 content item 中取纯文本（text 或 guardContent.text.text）；没有返回 None。"""
    if "text" in item and isinstance(item["text"], str):
        return item["text"]
    guard = item.get("guardContent")
    if isinstance(guard, dict):
        inner = guard.get("text")
        if isinstance(inner, dict) and isinstance(inner.get("text"), str):
            return inner["text"]
    return None


def _parse_system_variables(prompt_obj: dict) -> dict:
    """从 prompt.system 中解析 <system-variables> 块（"- key: value" 行），返回 kv。"""
    result = {}
    for block in prompt_obj.get("system") or []:
        if not isinstance(block, dict):
            continue
        text = block.get("text")
        if not isinstance(text, str):
            continue
        match = _SYSVAR_RE.search(text)
        if not match:
            continue
        for line in match.group(1).splitlines():
            line = line.strip().lstrip("-").strip()
            if ":" in line:
                key, _, value = line.partition(":")
                result[key.strip()] = value.strip()
    return result


# --- faqSearch 工具结果 -----------------------------------------------------

def _parse_faq_result(tool_result_text: str) -> list:
    """解析 faqSearch 的 toolResult 文本，返回命中的 FAQ 条目列表。

    toolResult 内层文本可能是：
      - 直接的 JSON 数组: '[{"question":..,"answer":..}, ...]'
      - 包裹的对象: '{"status":"success","output":"[...]"}'（output 可能再嵌套一层）
      - MCP 风格: '{"content":[{"text":"[...]"}]}'
    按 原文 → output → content[].text 的顺序尝试，第一个解析出数组的候选生效；
    空数组或无法解析视为未命中（返回 []）。
    """
    if not tool_result_text:
        return []
    text = tool_result_text.strip()
    top = _json_loads_or_none(text)
    if isinstance(top, list):
        return _normalize_faq_list(top)

    candidates = [text]
    if isinstance(top, dict):
        if isinstance(top.get("output"), str):
            candidates.append(top["output"])
        content = top.get("content")
        if isinstance(content, list):
            candidates.extend(
                c["text"] for c in content if isinstance(c, dict) and isinstance(c.get("text"), str)
            )

    for candidate in candidates:
        parsed = _json_loads_or_none(candidate.strip())
        if isinstance(parsed, dict) and isinstance(parsed.get("output"), str):
            parsed = _json_loads_or_none(parsed["output"])  # {"output": "[...]"} 再解一层
        if isinstance(parsed, list):
            return _normalize_faq_list(parsed)
    return []


def _normalize_faq_list(items: list) -> list:
    """标准化 FAQ 条目列表，保留 question/answer，并透传 confidence（若有）。"""
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        question = item.get("question") or item.get("Question") or ""
        answer = item.get("answer") or item.get("Answer") or ""
        entry = {"question": str(question), "answer": str(answer)}
        confidence = _confidence_value(item)
        if confidence is not None:
            entry["confidence"] = confidence
        if question or answer or confidence is not None:
            out.append(entry)
    return out


def _confidence_value(item: dict):
    """取 FAQ 条目的 confidence，兼容常见字段名。

    以第一个存在且非 None 的字段为准；该值无法转成数值时返回 None（不再看后面的字段）。
    """
    for key in ("confidence", "Confidence", "score", "Score"):
        if key in item and item[key] is not None:
            try:
                return float(item[key])
            except (TypeError, ValueError):
                return None
    return None


def _best_confidence(faq_items: list):
    """置信度 = FAQ 返回条目里 confidence 的最大值（代表最佳匹配）；都没有则为 None。"""
    confidences = [item["confidence"] for item in faq_items if "confidence" in item]
    return max(confidences) if confidences else None


# --- 是否命中（机器人是否采用了 FAQ 答案） -----------------------------------

# 机器人“未采用 FAQ 答案”的兜底/失败话术特征（多语言）。命中这些说明没采用。
_NON_ADOPTION_PATTERNS = [
    # 未找到 / 无相关信息
    "no encontr", "no pude encontrar", "no hay informaci", "sin informaci",
    "not find", "couldn't find", "could not find", "no information", "no relevant",
    "没有找到", "未找到", "没有相关", "抱歉",
    "não encontr", "sem informa",
    # 转人工 / 无法帮助
    "human agent", "agente humano", "connect you with", "conectarte con",
    "transferir", "转人工", "人工客服", "no puedo ayudar", "unable to help",
    "no he podido", "lo siento",
]
# 短于此长度、单行且以问号结尾的回复视为“纯澄清提问”
_CLARIFY_QUESTION_MAX_CHARS = 60


def _bot_adopted_answer(faq_items: list, bot_reply: str) -> bool:
    """判断机器人是否采用了 faqSearch 返回的答案（即 hit）。

    依次判定为“未采用”：FAQ 结果为空；机器人无实质回复；回复命中兜底/转人工话术；
    回复是较短的单行问句（多条 FAQ 待澄清时机器人只反问）。其余视为采用。

    FAQ 结果常为西/中文，机器人按 responseLanguage 翻译后回复，
    所以不做原文包含匹配，而是看“是否给出了实质回答且不是兜底话术”。
    """
    if not faq_items:
        return False
    reply = (bot_reply or "").strip()
    if not reply:
        return False
    lowered = reply.lower()
    if any(pattern in lowered for pattern in _NON_ADOPTION_PATTERNS):
        return False
    is_question = reply.endswith("?") or reply.endswith("？")
    if is_question and len(reply) < _CLARIFY_QUESTION_MAX_CHARS and "\n" not in reply:
        return False
    return True


# --- 是否追问（当前提问是否承接上一个 faqSearch 的提问） ----------------------

# 指代/延续性词汇（西/英/中/葡）：用词边界匹配，避免误匹配到词内部
_FOLLOWUP_CUES = [
    "eso", "ese", "esa", "esto", "también", "entonces", "mismo",
    "that", "this", "same", "also", "then",
    "还有", "那个", "然后", "另外",
    "isso", "esse",
]
_FOLLOWUP_CUE_RE = re.compile(
    r"(?<!\w)(?:" + "|".join(re.escape(cue) for cue in _FOLLOWUP_CUES) + r")(?!\w)"
)
# 以这些连词/短语开头，通常是承接上一问的追问
_FOLLOWUP_PREFIXES = (
    "y ", "y?", "and ", "what about", "y del", "y de", "y la", "y el",
    "e ", "e o", "e a", "那", "这", "还有", "以及", "呢",
)
_FOLLOWUP_MAX_WORDS = 4         # 不超过这么多词的短问视为追问
_CONTENT_WORD_MIN_LEN = 4       # 长度不小于此值的词才算“实词”
_WHITESPACE_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^\wáéíóúñü]+")


def _content_words(text: str) -> set:
    return {word for word in _NON_WORD_RE.split(text) if len(word) >= _CONTENT_WORD_MIN_LEN}


def _is_followup_question(prev_q: str, curr_q: str) -> bool:
    """判断 curr_q 是否是 prev_q 的延伸追问（而非全新问题）。

    无模型可用时的启发式近似，满足任一即视为追问：
      1) 当前问题很短（<= 4 个词）
      2) 以承接连词开头（y.../and.../那.../呢 等）
      3) 含指代/延续性提示词（eso/that/还有 等，词边界匹配）
      4) 与上一问有实词重叠（同主题延伸）
    """
    if not prev_q or not curr_q:
        return False
    curr = curr_q.strip().lower()
    prev = prev_q.strip().lower()

    word_count = len([w for w in _WHITESPACE_RE.split(curr) if w])
    if 0 < word_count <= _FOLLOWUP_MAX_WORDS:
        return True
    if curr.startswith(_FOLLOWUP_PREFIXES):
        return True
    if _FOLLOWUP_CUE_RE.search(curr):
        return True
    return bool(_content_words(prev) & _content_words(curr))


# --- 单条消息解析 -------------------------------------------------------------

def _tool_result_text(tool_result: dict) -> str:
    """toolResult.content 是 [{"text": ..}] 列表（拼接各段文本），或直接是 value 字符串。"""
    content = tool_result.get("content")
    if isinstance(content, list):
        return "\n".join(
            c["text"] for c in content if isinstance(c, dict) and isinstance(c.get("text"), str)
        )
    if isinstance(tool_result.get("value"), str):
        return tool_result["value"]
    return ""


def _flatten_messages(messages: list) -> tuple:
    """按出现顺序展开 prompt.messages，返回 (turns, stream, tool_results)。

    turns         干净的对话记录 [{"role": "CUSTOMER"|"BOT", "text"}]（不含 <thinking>）
    stream        [("customer", text) | ("bot", text) | ("tooluse", toolUse dict)]，
                  用于给每个 faqSearch 找前后最近的客户提问 / 机器人回复
    tool_results  {toolUseId: 结果文本}
    非 dict 的 message 会抛 AttributeError，由调用方丢弃整条消息（保持历史行为）。
    """
    turns, stream, tool_results = [], [], {}
    for msg in messages:
        role = msg.get("role")
        for item in msg.get("content") or []:
            if not isinstance(item, dict):
                continue
            if "toolUse" in item and isinstance(item["toolUse"], dict):
                stream.append(("tooluse", item["toolUse"]))
                continue
            if "toolResult" in item and isinstance(item["toolResult"], dict):
                tool_result = item["toolResult"]
                tool_use_id = tool_result.get("toolUseId", "")
                text = _tool_result_text(tool_result)
                if tool_use_id:
                    tool_results[tool_use_id] = text
                continue
            text = _content_text(item)
            if text is None:
                continue
            if role == "user":
                text = text.strip()
                if text:
                    turns.append({"role": "CUSTOMER", "text": text})
                    stream.append(("customer", text))
            elif role == "assistant":
                bot_message = _extract_bot_message(text)
                if bot_message:  # 空 <message> 占位不计入对话
                    turns.append({"role": "BOT", "text": bot_message})
                    stream.append(("bot", bot_message))
    return turns, stream, tool_results


def _nearest_text(stream: list, start: int, kind: str, step: int) -> str:
    """从 stream[start] 起按 step 方向找第一条 kind 类型的文本；没有返回 ""。"""
    index = start
    while 0 <= index < len(stream):
        if stream[index][0] == kind:
            return stream[index][1]
        index += step
    return ""


def _faq_tool_input(tool: dict) -> tuple:
    """toolUse.input（dict 或 JSON 字符串）-> (query, kb_key, productCategory)。"""
    tool_input = tool.get("input") or {}
    if isinstance(tool_input, str):
        tool_input = _json_loads_or_none(tool_input)
    if not isinstance(tool_input, dict):
        return "", "", None
    return tool_input.get("query", ""), tool_input.get("kb_key", ""), tool_input.get("productCategory")


def _faq_events_from_stream(stream: list, tool_results: dict) -> list:
    """为 stream 中的每个 faqSearch 调用生成一条 faq_event。"""
    faq_events = []
    prev_question = None  # 上一个 faqSearch 的客户提问，用于追问判定；第一个不算追问
    for index, (kind, value) in enumerate(stream):
        if kind != "tooluse":
            continue
        # name 非 str 时 `in` 抛 TypeError，由调用方丢弃整条消息（保持历史行为）
        if FAQ_TOOL_SUBSTR not in (value.get("name", "") or ""):
            continue
        query, kb_key, product_category = _faq_tool_input(value)
        original_question = _nearest_text(stream, index - 1, "customer", -1)
        answer = _nearest_text(stream, index + 1, "bot", 1)
        faq_matches = _parse_faq_result(tool_results.get(value.get("toolUseId", ""), ""))
        question = original_question or query

        faq_events.append({
            "query": query,
            "original_question": question,
            "answer": answer,
            "hit": _bot_adopted_answer(faq_matches, answer),
            "confidence": _best_confidence(faq_matches),
            "is_followup": prev_question is not None and _is_followup_question(prev_question, question),
            "kb_key": kb_key,
            "product_category": product_category,
            "faq_matches": faq_matches,
        })
        prev_question = question
    return faq_events


def parse_agentic_message(message_obj: dict):
    """解析单条 AGENTIC_MESSAGE / LLM_INVOCATION 的 message，返回结构化会话；无有效 prompt 返回 None。

    返回:
      {
        "session_id", "contact_id", "instance_id", "bot_language", "event_timestamp",
        "turns": [{"role": "CUSTOMER"|"BOT", "text"}],   # 干净对话（无思考）
        "faq_events": [{
            "query":             faqSearch 的 query 参数
            "original_question": faqSearch 前最近一条客户提问（没有则为 query）
            "answer":            faqSearch 后机器人的首个回复
            "hit":               机器人是否采用了 FAQ 答案
            "confidence":        FAQ 返回条目中 confidence 的最大值，没有则 None
            "is_followup":       是否是上一个 faqSearch 提问的延伸追问
            "kb_key", "product_category",
            "faq_matches":       [{question, answer[, confidence]}]
        }]
      }
    结构异常（如 prompt 不是对象、message 不是 dict）会抛异常，由调用方丢弃该消息。
    """
    prompt_raw = message_obj.get("prompt")
    if not prompt_raw:
        return None
    try:
        prompt_obj = json.loads(prompt_raw) if isinstance(prompt_raw, str) else prompt_raw
    except (json.JSONDecodeError, TypeError):
        return None

    sysvars = _parse_system_variables(prompt_obj)
    turns, stream, tool_results = _flatten_messages(prompt_obj.get("messages") or [])
    return {
        "session_id": message_obj.get("session_id") or sysvars.get("sessionId", ""),
        "contact_id": sysvars.get("contactId", ""),
        "instance_id": sysvars.get("instanceId", ""),
        "bot_language": sysvars.get("responseLanguage", ""),
        "event_timestamp": message_obj.get("event_timestamp"),
        "turns": turns,
        "faq_events": _faq_events_from_stream(stream, tool_results),
    }


def dedupe_latest_by_session(parsed_list: list) -> list:
    """同一 session 只保留 event_timestamp 最大的一条（累积最全）。

    以 session_id（其次 contact_id）分组；时间戳相同取后出现的一条；
    结果按各组首次出现的顺序排列。
    """
    latest = {}
    for parsed in parsed_list:
        if not parsed:
            continue
        key = parsed.get("session_id") or parsed.get("contact_id") or id(parsed)
        current = latest.get(key)
        if current is None or (parsed.get("event_timestamp") or 0) >= (current.get("event_timestamp") or 0):
            latest[key] = parsed
    return list(latest.values())


# ---------------------------------------------------------------------------
# faqSearch 分析: trace 中的 execute_tool span 与 gateway 日志
# ---------------------------------------------------------------------------

def _tool_use_from_span(span: dict) -> dict:
    """从 execute_tool span 的 input_messages 中取 toolUse dict。"""
    raw = span.get("input_messages", "")
    try:
        messages = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return {}
    if not isinstance(messages, list):
        return {}
    for message in messages:
        values = message.get("values") if isinstance(message, dict) else None
        for value in values if isinstance(values, list) else []:
            tool_use = value.get("toolUse") if isinstance(value, dict) else None
            if isinstance(tool_use, dict):
                return tool_use
    return {}


def _faq_tool_span(row: dict, obj: dict, span: dict) -> dict:
    """一个 faqSearch execute_tool span -> 输入、状态、成功与否、耗时、输出预览。"""
    tool_name = _tool_name_from_span(span)
    tool_use = _tool_use_from_span(span)
    tool_input = tool_use.get("input")
    if isinstance(tool_input, str):
        try:
            tool_input = json.loads(tool_input)
        except (TypeError, ValueError):
            pass
    start = _int_or_none(span.get("start_timestamp"))
    end = _int_or_none(span.get("end_timestamp"))
    return {
        "timestamp": row.get("timestamp"),
        "sessionId": obj.get("session_id", ""),
        "toolName": tool_name,
        "shortToolName": _short_tool_name(tool_name),
        "toolUseId": tool_use.get("toolUseId", ""),
        "input": tool_input,
        "status": span.get("status", ""),
        "success": bool(_tool_succeeded(span)),
        "startTimestamp": start,
        "endTimestamp": end,
        "latencyMs": (end - start) if start is not None and end is not None else None,
        "outputPreview": _preview(_tool_output_text(span)),
    }


def _gateway_level(message: str) -> str:
    """日志级别：优先取 JSON 日志中的 level 类字段，否则在文本中找级别关键字。"""
    try:
        obj = json.loads(message)
    except (TypeError, ValueError):
        obj = None
    if isinstance(obj, dict):
        for key in _GATEWAY_LEVEL_KEYS:
            if isinstance(obj.get(key), str) and obj[key]:
                return obj[key].upper()
    match = _GATEWAY_LEVEL_RE.search(message or "")
    return match.group(1) if match else ""


def _gateway_faq_entry(row: dict, message: str):
    """提到 faqSearch 的 gateway 日志 -> 条目；其他 gateway 日志返回 None。"""
    if not _FAQ_RE.search(message):
        return None
    return {
        "timestamp": row.get("timestamp"),
        "level": _gateway_level(message),
        "isError": bool(_GATEWAY_ERROR_RE.search(message)),
        "message": _preview(message),
    }


# ---------------------------------------------------------------------------
# 日志行汇总: 一个 events 文件 -> analysis
# ---------------------------------------------------------------------------

def _new_counts() -> dict:
    return {
        "total": 0, "connect": 0, "gateway": 0, "agenticMessages": 0,
        "llmInvocations": 0, "aiAgentTraces": 0, "executeToolSpans": 0,
    }


@dataclasses.dataclass
class _RowScan:
    """单次遍历日志行收集到的中间结果。"""
    counts: dict = dataclasses.field(default_factory=_new_counts)
    valid_rows: list = dataclasses.field(default_factory=list)       # 交给 build_contact_summary
    agentic: list = dataclasses.field(default_factory=list)          # [(row, obj)] 有 prompt 的 AGENTIC_MESSAGE
    llm: list = dataclasses.field(default_factory=list)              # [(row, obj)] 有 prompt 的 LLM_INVOCATION
    tool_spans: list = dataclasses.field(default_factory=list)       # faqSearch execute_tool span
    gateway_entries: list = dataclasses.field(default_factory=list)  # 提到 faqSearch 的 gateway 日志


def _is_valid_row(row) -> bool:
    return isinstance(row, dict) and row.get("timestamp") is not None and row.get("message") is not None


def _scan_connect_event(scan: _RowScan, row: dict, message: str) -> None:
    """统计一条 connect(AI Agent) 日志：按 event_type 计数、收集会话解析输入与 faqSearch span。"""
    try:
        obj = json.loads(message)
    except (TypeError, ValueError):
        return
    if not isinstance(obj, dict):
        return
    event_type = obj.get("event_type") or ""
    if event_type == EVENT_AGENTIC_MESSAGE:
        scan.counts["agenticMessages"] += 1
        if obj.get("prompt"):
            scan.agentic.append((row, obj))
    elif event_type == EVENT_LLM_INVOCATION:
        scan.counts["llmInvocations"] += 1
        if obj.get("prompt"):
            scan.llm.append((row, obj))
    elif event_type == EVENT_AI_AGENT_TRACE:
        scan.counts["aiAgentTraces"] += 1

    # span 不限 event_type：任何带 execute_tool span 的事件都统计
    span_str = obj.get("span")
    if not span_str:
        return
    span = _parse_span(span_str)
    if span.get("span_name") != "execute_tool":
        return
    scan.counts["executeToolSpans"] += 1
    if _FAQ_RE.search(_tool_name_from_span(span) or ""):
        scan.tool_spans.append(_faq_tool_span(row, obj, span))


def _scan_rows(rows: list) -> _RowScan:
    scan = _RowScan()
    for row in rows:
        if not _is_valid_row(row):
            continue
        scan.valid_rows.append(row)
        scan.counts["total"] += 1
        message = row["message"] if isinstance(row["message"], str) else json.dumps(row["message"])
        if row.get("source") == "gateway":
            scan.counts["gateway"] += 1
            entry = _gateway_faq_entry(row, message)
            if entry:
                scan.gateway_entries.append(entry)
        else:
            scan.counts["connect"] += 1
            _scan_connect_event(scan, row, message)
    return scan


def _parse_sessions(agentic: list, llm: list) -> tuple:
    """返回 (inputEventType, sessions)。

    优先解析 TRANSCRIPT_AGENTIC_MESSAGE；没有时回退到同为 Converse prompt 结构的
    LARGE_LANGUAGE_MODEL_INVOCATION。单条消息解析异常只丢弃该条，不影响其他消息。
    """
    if agentic:
        input_type, candidates = EVENT_AGENTIC_MESSAGE, agentic
    elif llm:
        input_type, candidates = EVENT_LLM_INVOCATION, llm
    else:
        input_type, candidates = None, []
    parsed = []
    for row, obj in candidates:
        message_obj = dict(obj)
        message_obj.setdefault("event_timestamp", row.get("timestamp"))
        try:
            result = parse_agentic_message(message_obj)
        except Exception:  # noqa: BLE001 - 单条消息异常不影响其他消息
            result = None
        if result:
            parsed.append(result)
    return input_type, dedupe_latest_by_session(parsed)


def _faq_search_summary(sessions: list, tool_spans: list, gateway_entries: list) -> dict:
    faq_events = [ev for s in sessions for ev in (s.get("faq_events") or [])]
    return {
        "transcriptCalls": len(faq_events),
        "transcriptHits": sum(1 for ev in faq_events if ev.get("hit")),
        "toolSpans": tool_spans,
        "toolSpanCount": len(tool_spans),
        "toolSpanSuccess": sum(1 for s in tool_spans if s["success"]),
        "gateway": {
            "count": len(gateway_entries),
            "errors": sum(1 for e in gateway_entries if e["isError"]),
            "entries": gateway_entries,
        },
    }


def _add_log_summary(analysis: dict, valid_rows: list) -> None:
    """附加与 DynamoDB 摘要同口径的统计；摘要失败只记录错误，不影响其余结果。"""
    if not valid_rows:
        return
    try:
        summary = build_contact_summary(valid_rows)
        analysis["logSummary"] = {k: summary.get(k) for k in _LOG_SUMMARY_FIELDS}
    except Exception as error:  # noqa: BLE001
        analysis["logSummaryError"] = str(error)


def analyze_rows(rows: list) -> dict:
    """对一个 events 文件的行做解析，返回 analysis dict。"""
    scan = _scan_rows(rows)
    input_type, sessions = _parse_sessions(scan.agentic, scan.llm)
    analysis = {
        "counts": scan.counts,
        "transcript": {"inputEventType": input_type, "sessions": sessions},
        "faqSearch": _faq_search_summary(sessions, scan.tool_spans, scan.gateway_entries),
    }
    _add_log_summary(analysis, scan.valid_rows)
    return analysis


# ---------------------------------------------------------------------------
# Connect 联系人补充: 渠道 + 属性
# ---------------------------------------------------------------------------

def _describe_contact(region: str, instance_id: str, contact_id: str) -> tuple:
    """返回 (contact dict, error str)；只保留 _CONTACT_FIELDS 中非空的字段。"""
    try:
        response = _connect(region).describe_contact(
            InstanceId=instance_id, ContactId=contact_id
        )
    except Exception as error:  # noqa: BLE001
        return None, _client_error_text(error)
    raw = response.get("Contact") or {}
    contact = {
        _lower_first(field): _jsonable(raw[field])
        for field in _CONTACT_FIELDS if raw.get(field) not in (None, "", {})
    }
    return contact, None


def _get_attributes(region: str, instance_id: str, initial_contact_id: str) -> tuple:
    """返回 (attributes dict, error str)。"""
    try:
        response = _connect(region).get_contact_attributes(
            InstanceId=instance_id, InitialContactId=initial_contact_id
        )
    except Exception as error:  # noqa: BLE001
        return None, _client_error_text(error)
    return response.get("Attributes") or {}, None


def _enrich_with_contact(record: dict, instance_name: str, contact_id: str) -> None:
    """按 INSTANCES_META 定位实例，补充 contact / attributes（失败只写 error 字段）。"""
    inst = _instances_meta().get(instance_name) or {}
    instance_id, region = inst.get("instanceId"), inst.get("region")
    record["instanceId"], record["connectRegion"] = instance_id, region
    if not instance_id or not region:
        record["contactError"] = record["attributesError"] = NO_INSTANCE_REASON
        return
    if not _UUID_RE.match(contact_id):
        # 如 unknown / unattached-gateway-logs 等非 Contact ID 文件名
        record["contactError"] = record["attributesError"] = NOT_CONTACT_ID_REASON
        return
    contact, record["contactError"] = _describe_contact(region, instance_id, contact_id)
    record["contact"] = contact
    # 属性挂在初始联系人上（转接后的 contact 需用 InitialContactId 查询）
    initial_id = (contact or {}).get("initialContactId") or contact_id
    record["attributes"], record["attributesError"] = _get_attributes(
        region, instance_id, initial_id
    )


# ---------------------------------------------------------------------------
# 日志输出: 超过 MAX_LOG_BYTES 时先截断长字段，仍超限则分片
# ---------------------------------------------------------------------------

def _truncate_strings(value, limit: int = TRUNCATE_FIELD_CHARS):
    """递归截断所有超过 limit 的字符串。"""
    if isinstance(value, str):
        return _truncate_text(value, limit)
    if isinstance(value, dict):
        return {k: _truncate_strings(v, limit) for k, v in value.items()}
    if isinstance(value, list):
        return [_truncate_strings(v, limit) for v in value]
    return value


def _fits(line: str, max_bytes: int) -> bool:
    return len(line.encode("utf-8")) <= max_bytes


def _emit_chunks(record: dict, line: str, max_bytes: int) -> None:
    """把 line 切片输出；按 part 顺序拼接各行的 data 即为完整（已截断）JSON。"""
    header = {
        "type": f"{LOG_TYPE}.chunk", "key": record.get("key"),
        "eTag": record.get("eTag"), "part": 0, "parts": 0, "data": "",
    }
    overhead = len(_dumps(header).encode("utf-8")) + CHUNK_OVERHEAD_SLACK
    # 按最坏情况每字符 4 字节估算每片字符数，保证每行不超限
    step = max(1, (max_bytes - overhead) // MAX_BYTES_PER_CHAR)
    pieces = [line[i:i + step] for i in range(0, len(line), step)]
    for index, piece in enumerate(pieces, 1):
        _print_line(dict(header, part=index, parts=len(pieces), data=piece))


def _emit(record: dict) -> None:
    """输出一行 JSON；超过 MAX_LOG_BYTES 时先截断长字段（并标记 truncated），仍超限则分片输出。"""
    max_bytes = _max_log_bytes()
    line = _dumps(record)
    if _fits(line, max_bytes):
        print(line, flush=True)
        return
    record = _truncate_strings(record)
    record["truncated"] = True
    line = _dumps(record)
    if _fits(line, max_bytes):
        print(line, flush=True)
        return
    _emit_chunks(record, line, max_bytes)


# ---------------------------------------------------------------------------
# Lambda 入口
# ---------------------------------------------------------------------------

def process_record(s3_record: dict) -> dict:
    """处理一条 S3 事件记录，返回结果 dict（读取/解析失败写入 processingError，不抛异常）。"""
    s3_info = s3_record.get("s3") or {}
    bucket = (s3_info.get("bucket") or {}).get("name", "")
    obj = _s3_object(s3_record)
    key = _object_key(obj)
    instance_name, contact_id = _parse_key(key) or ("", "")

    record = {
        "type": f"{LOG_TYPE}.result",
        "bucket": bucket,
        "key": key,
        "eTag": obj.get("eTag"),
        "versionId": obj.get("versionId"),
        "sequencer": obj.get("sequencer"),
        "size": obj.get("size"),
        "eventName": s3_record.get("eventName"),
        "eventTime": s3_record.get("eventTime"),
        "instanceName": instance_name,
        "instanceId": None,
        "connectRegion": None,
        "contactId": contact_id,
        "contact": None,
        "contactError": None,
        "attributes": None,
        "attributesError": None,
        "analysis": None,
        "processingError": None,
    }
    try:
        rows, meta = _load_rows(bucket, key)
        record["object"] = meta
        record["analysis"] = analyze_rows(rows)
    except Exception as error:  # noqa: BLE001
        record["processingError"] = (
            f"{type(error).__name__}: {error} | "
            + traceback.format_exc(limit=-3)  # 取最内层 3 帧，保留真正抛错的位置.replace("\n", " | ")
        )
    _enrich_with_contact(record, instance_name, contact_id)
    return record


def handler(event, context):
    processed = skipped = errors = 0
    for s3_record in (event or {}).get("Records") or []:
        try:
            key = _object_key(_s3_object(s3_record))
            if not _parse_key(key):
                skipped += 1
                _print_line({"type": f"{LOG_TYPE}.skip", "key": key, "reason": SKIP_REASON})
                continue
            record = process_record(s3_record)
            _emit(record)
            processed += 1
            if record.get("processingError"):
                errors += 1
        except Exception as error:  # noqa: BLE001 - 不让单条记录影响其余记录
            errors += 1
            _print_line({"type": f"{LOG_TYPE}.error", "error": f"{type(error).__name__}: {error}",
                         "traceback": traceback.format_exc()})
    return {"ok": True, "processed": processed, "skipped": skipped, "errors": errors}
