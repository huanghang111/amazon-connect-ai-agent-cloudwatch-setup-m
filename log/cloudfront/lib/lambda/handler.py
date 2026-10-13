#!/usr/bin/env python3
"""
handler.py — S3 事件触发的 Lambda: 在云端完成日志解析并写入 DynamoDB / S3

触发方式:
  日志桶 ai-agent-logs-<acct> 配置了 S3 事件通知: 当 "trigger/" 前缀下出现 .json
  触发对象(由部署脚本在原始数据上传完成后写入)时，调用本函数。

存储设计:
  Contact 摘要表(环境变量 CONTACTS_TABLE, DynamoDB):
    分区键:  instanceName (S)   ← Connect 实例名，用于多实例数据隔离
    排序键:  contactId    (S)
    属性:    minTs, maxTs, count, sourceCounts, generatedAt

  事件明细(S3, 环境变量 LOGS_BUCKET):
    路径: events/{instanceName}/{contactId}.json.gz
    内容: gzip 压缩的 JSON 数组，每个元素为
          {"timestamp": <epoch_ms>, "message": "...", "source": "connect"|"gateway",
           "contactId": "..."}

  写入策略:
    以 (instanceName, contactId) 为唯一键，直接 put_item 覆盖写入。
    每次触发均重新解析并覆盖 DynamoDB 摘要与 S3 事件明细。

触发 JSON 格式(由 setup_cloudfront.py 写入 trigger/ 前缀):
  {
    "instanceName": "<Connect InstanceAlias>",
    "connect": "raw/<prefix>/connect.ndjson",
    "gateway": "raw/<prefix>/gateway.ndjson",   # 可选
    "prefix":  "<prefix>/"                      # S3 key 前缀，用于写 index.json
  }

依赖: Lambda 运行时自带 boto3；connect_parser.py 随部署包一起打包。
      需要环境变量: CONTACTS_TABLE、LOGS_BUCKET(由 setup_cloudfront.py 注入)。
      instanceName 从触发 JSON 的 instanceName 字段读取，不再依赖环境变量。
"""
import concurrent.futures
import datetime
import gzip
import json
import os
import re as _re
import traceback
import urllib.parse

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config

import connect_parser as cp

UNATTACHED_GATEWAY = "unattached-gateway-logs"
DOUBLE_UUID_SESSION = "double-uuid-session"  # Q in Connect 双 UUID session，无对应 Connect contact，跳过入库

# ---------------------------------------------------------------------------
# Guardrail 辅助函数（与前端 app.js 逻辑对齐）
# ---------------------------------------------------------------------------

def _slice_balanced(s, start):
    """从 start 位置开始，用括号配对截取完整的 JSON 数组或对象。"""
    depth = 0
    for i in range(start, len(s)):
        c = s[i]
        if c in ("[", "{"):
            depth += 1
        elif c in ("]", "}"):
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return ""


def _extract_guard_assessments(raw_msg):
    """从单条日志 message 中提取结构化 guardrail_assessments 列表。
    返回对象列表（可能为空列表）；若消息中完全没有 guardrail_assessments 则返回 None。
    """
    # 先尝试 JSON 解析，取 span.guardrail_assessments
    texts = []
    try:
        obj = json.loads(raw_msg)
        span_str = obj.get("span", "")
        if span_str and "guardrail_assessments" in str(span_str):
            texts.append(str(span_str))
    except (ValueError, TypeError):
        pass
    if not texts and "guardrail_assessments" in raw_msg:
        texts.append(raw_msg)
    if not texts:
        return None

    out = []
    for text in texts:
        # 还原 JSON 转义引号
        s = _re.sub(r'\\+"', '"', text)
        arrays = []
        if "guardrail_assessments" not in s:
            idx = s.find("[")
            if idx != -1:
                seg = _slice_balanced(s, idx)
                if seg:
                    arrays.append(seg)
        else:
            pos = 0
            while True:
                k = s.find("guardrail_assessments", pos)
                if k == -1:
                    break
                idx = s.find("[", k)
                if idx == -1:
                    break
                seg = _slice_balanced(s, idx)
                if not seg:
                    break
                arrays.append(seg)
                pos = idx + len(seg)
        for a in arrays:
            try:
                parsed = json.loads(a)
                if isinstance(parsed, list):
                    out.extend(item for item in parsed if isinstance(item, dict))
            except (ValueError, TypeError):
                pass
    return out  # 空列表也返回（说明有 guardrail_assessments 字段但解析失败或为空）


def _guardrail_detail_text(d):
    """将 guardrail policy details 字段（字符串/对象/列表）统一转成可读文本。"""
    if d is None:
        return ""
    if isinstance(d, str):
        return d.strip()
    if isinstance(d, list):
        return " / ".join(filter(None, (_guardrail_detail_text(x) for x in d)))
    if isinstance(d, dict):
        for key in ("name", "topic", "type", "filterType", "match", "detail"):
            if isinstance(d.get(key), str) and d[key]:
                return d[key]
        try:
            return json.dumps(d, ensure_ascii=False)
        except Exception:
            return ""
    return str(d)

# 环境变量(由部署脚本注入)
CONTACTS_TABLE = os.environ.get("CONTACTS_TABLE", "")
LOGS_BUCKET    = os.environ.get("LOGS_BUCKET", "")

# 并发写 S3 的线程数(可用环境变量覆盖)
UPLOAD_WORKERS = max(1, int(os.environ.get("UPLOAD_WORKERS", "16")))

s3 = boto3.client("s3", config=Config(
    max_pool_connections=UPLOAD_WORKERS + 4,
    retries={"max_attempts": 5, "mode": "adaptive"},
))
dynamo_res = boto3.resource("dynamodb", config=Config(
    max_pool_connections=UPLOAD_WORKERS + 4,
    retries={"max_attempts": 5, "mode": "adaptive"},
))

_serializer = TypeSerializer()

# Connect 客户端缓存(按 region 区分，线程安全：dict 赋值是原子操作)
_connect_clients: dict = {}

def _connect_client(region: str):
    if region not in _connect_clients:
        _connect_clients[region] = boto3.client(
            "connect", region_name=region,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _connect_clients[region]


# ---------------------------------------------------------------------------
# S3 读取
# ---------------------------------------------------------------------------

def _read_ndjson(bucket, key, source):
    """流式读取 S3 上的 NDJSON(每行一个事件)，归一化为 rows。"""
    rows = []
    resp = s3.get_object(Bucket=bucket, Key=key)
    for raw in resp["Body"].iter_lines():
        if not raw:
            continue
        try:
            e = json.loads(raw.decode("utf-8", "ignore"))
        except (ValueError, TypeError):
            continue
        ts, msg = e.get("timestamp"), e.get("message")
        if ts is None or msg is None:
            continue
        rows.append({"timestamp": int(ts),
                     "message": cp.sanitize_message(msg),
                     "source": source})
    return rows


# ---------------------------------------------------------------------------
# Contact ID 解析
# ---------------------------------------------------------------------------

def _resolve_contact_id(row):
    cid = row.get("contactId") or ""
    if cid:
        # Q in Connect 某些版本 session_name 为双 UUID 格式（73字符），
        # 这类 session 没有对应的 Connect contact，跳过入库
        if len(cid) > 36 and cid[36] == "-":
            return DOUBLE_UUID_SESSION
        return cid
    if row.get("source") == "gateway":
        return UNATTACHED_GATEWAY
    _, sid = cp._extract_connect_ids(row.get("message", ""))
    return sid or "unknown"


# ---------------------------------------------------------------------------
# 指标提取辅助函数（与前端 app.js computeMetrics 保持一致）
# ---------------------------------------------------------------------------

def _parse_span(span_str):
    """解析 OpenTelemetry 风格的非 JSON span 字符串 {k=v, k=v, nested=[...]}。
    容错浅解析，仅取顶层 key=value，嵌套结构保留为原始字符串。
    """
    out = {}
    if not isinstance(span_str, str):
        return out
    s = span_str.strip()
    if s.startswith("{"):
        s = s[1:]
    if s.endswith("}"):
        s = s[:-1]
    parts = []
    depth = 0
    cur = ""
    for ch in s:
        if ch in ("[", "{"):
            depth += 1
        elif ch in ("]", "}"):
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    for p in parts:
        eq = p.find("=")
        if eq == -1:
            continue
        k = p[:eq].strip()
        v = p[eq + 1:].strip()
        out[k] = v
    return out


def _tool_name_from_span(sp):
    """从 execute_tool span 里取工具名（与前端 toolNameFromSpan 一致）。"""
    im_raw = sp.get("input_messages", "")
    try:
        im = json.loads(im_raw) if isinstance(im_raw, str) else im_raw
        if isinstance(im, list):
            for m in im:
                for v in (m.get("values") or []):
                    if isinstance(v, dict) and v.get("toolUse", {}).get("name"):
                        return v["toolUse"]["name"]
    except (ValueError, TypeError):
        pass
    return sp.get("tool_name") or sp.get("tool") or ""


def _short_tool_name(name):
    """去掉 MCP 工具名的命名空间前缀: connect_repair_mcp___faqSearch -> faqSearch。"""
    if not name:
        return ""
    idx = name.rfind("___")
    return name[idx + 3:] if idx >= 0 else name


def _tool_output_text(sp):
    """取 execute_tool span 的输出正文（用于判断工具调用是否成功）。"""
    om_raw = sp.get("output_messages", "")
    txt = ""
    try:
        om = json.loads(om_raw) if isinstance(om_raw, str) else om_raw
        if isinstance(om, list):
            for m in om:
                for v in (m.get("values") or []):
                    if isinstance(v, dict) and v.get("toolResult"):
                        for c in (v["toolResult"].get("content") or []):
                            txt += c.get("text") or json.dumps(c)
    except (ValueError, TypeError):
        pass
    if not txt and isinstance(om_raw, str):
        txt = om_raw
    return txt


def _tool_succeeded(sp):
    """判断一次工具调用是否成功（status OK 且输出无 error / HTTP 4xx-5xx）。"""
    if sp.get("status") and sp["status"] != "OK":
        return False
    txt = _tool_output_text(sp)
    if _re.search(r'"error"', txt):
        return False
    if _re.search(r'HTTP\s*[45]\d\d', txt):
        return False
    return True


def _repair_ticket_returned(sp):
    """判断 requestRepair 工具是否成功创建了 SR Draft（输出里有非空 ticketNumber）。"""
    txt = _tool_output_text(sp)
    if not txt:
        return False
    m = _re.search(r'ticketNumber\\*["\']?\s*:\s*\\*["\']?\s*([^"\'\\,}\s]+)', txt, _re.IGNORECASE)
    if not m:
        return False
    val = (m.group(1) or "").strip()
    return bool(val) and val.lower() not in ("null", "none")


# ---------------------------------------------------------------------------
# 单 Contact 写入逻辑(线程池并发调用)
# ---------------------------------------------------------------------------

def _write_contact(bucket, instance_name, cid, rows, instance_id="", connect_region="", csat_attr_key="botevaluation"):
    """处理单个 Contact: 无论是否存在，直接覆盖写入 S3 明细(gzip) + DDB 摘要。

    返回 (summary_entry, status)，status 固定为 "created"。

    DDB 存储所有可从事件日志派生的字段，以及通过 DescribeContact 获取的
    Channel / InitiationMethod / DisconnectReason / DID / CSAT，
    无需前端动态加载。

    该函数被线程池并发调用；不访问任何共享可变状态，并发安全。
    """
    contacts_table = dynamo_res.Table(CONTACTS_TABLE)
    events_key = f"events/{instance_name}/{cid}.json.gz"

    # 规范化排序
    rows = sorted(rows, key=lambda r: (r["timestamp"], r.get("source", "connect"), r["message"]))
    min_ts = rows[0]["timestamp"]
    max_ts = rows[-1]["timestamp"]
    source_counts = {}
    for r in rows:
        src = r.get("source", "connect")
        source_counts[src] = source_counts.get(src, 0) + 1

    # ---- 提取所有日志可派生指标（与前端 computeMetrics 对齐）----
    locale = ""
    bu = ""
    sr_number = ""
    mcp_tool_calls = 0
    faq_ask = 0
    faq_send = 0
    sr_intent = 0
    sr_success = 0
    first_repair_ts = None
    guard_blocks = 0
    guard_reasons = set()
    _guard_assessed_events = []   # [(blocked_assessments_list, raw_msg), ...]
    _guard_fallback_events = []   # [raw_msg, ...]
    lat_sum = 0
    lat_n = 0
    agentic = 0
    llm = 0
    errors = 0

    for r in rows:
        msg = r.get("message", "")
        src = r.get("source", "connect")
        ts  = r.get("timestamp", 0)

        # locale / BU / srNumber（所有行都扫，取首次命中）
        if not locale:
            m = (_re.search(r'language locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', msg) or
                 _re.search(r'configured locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', msg) or
                 _re.search(r'\blocale\b[^A-Za-z]{0,12}([a-z]{2}_[A-Z]{2})', msg) or
                 _re.search(r'responseLanguage[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', msg))
            if m:
                locale = m.group(1)
        if not bu:
            m = _re.search(r'-\s*BU:\s*([^\\"\n<}]+)', msg)
            if m:
                bu = m.group(1).strip()
        if not sr_number:
            m = _re.search(r'srNumber\\*["\']?\s*:\s*\\*["\']?\s*([^"\'\\,}\s]+)', msg, _re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                if val and val.lower() not in ("null", "none"):
                    sr_number = val

        # guardrail 拦截 — 结构化解析 guardrail_assessments（与前端逻辑对齐）
        # 同一次拦截在日志里会出现两次：
        #   a) inference span 的 guardrail_assessments（结构化，优先使用）
        #   b) ORCHESTRATION_MESSAGE 的 guardrail_blocked=true（兜底）
        # 策略：有结构化评估的事件优先，收集完所有行后再统一去重计数。
        _guard_raw = _extract_guard_assessments(msg)
        if _guard_raw is not None:
            # 结构化路径：assessments 是解析出的对象列表
            _blocked = [a for a in _guard_raw if str(a.get("action", "")).upper() != "NONE"]
            if _blocked:
                _guard_assessed_events.append((_blocked, msg))
        elif ('"guardrail_blocked": true' in msg or "'guardrail_blocked': True" in msg):
            # 兜底路径：只有 guardrail_blocked 标记，无结构化评估
            _guard_fallback_events.append(msg)

        if src == "gateway":
            if _re.search(r'\b(ERROR|Exception|Traceback|FATAL)\b', msg):
                errors += 1
            continue

        # 解析 connect span
        try:
            obj = json.loads(msg)
        except (ValueError, TypeError):
            continue

        event_type = (obj.get("event_type") or "").replace("TRANSCRIPT_", "")
        if event_type == "AGENTIC_MESSAGE":
            agentic += 1
        elif event_type == "LARGE_LANGUAGE_MODEL_INVOCATION":
            llm += 1

        span_str = obj.get("span", "")
        if not span_str:
            continue
        sp = _parse_span(span_str)

        if sp.get("status") and sp["status"] != "OK":
            errors += 1

        span_name = sp.get("span_name", "")

        if span_name == "inference":
            try:
                lat_sum += int(sp["end_timestamp"]) - int(sp["start_timestamp"])
                lat_n += 1
            except (KeyError, ValueError, TypeError):
                pass

        if span_name == "execute_tool":
            mcp_tool_calls += 1
            tool_name = _short_tool_name(_tool_name_from_span(sp))
            ok = _tool_succeeded(sp)
            if _re.search(r'faqSearch', tool_name, _re.IGNORECASE):
                faq_ask += 1
                if ok:
                    faq_send += 1
            if _re.search(r'requestRepair', tool_name, _re.IGNORECASE):
                sr_intent += 1
                if _repair_ticket_returned(sp):
                    sr_success += 1
                if first_repair_ts is None:
                    first_repair_ts = ts

    # SR 信息轮次: 首次 requestRepair 前客户 UTTERANCE 数
    sr_rounds = None
    if sr_intent > 0:
        rounds = 0
        for r in rows:
            if r.get("source") == "gateway":
                continue
            try:
                obj = json.loads(r["message"])
            except (ValueError, TypeError):
                continue
            if (obj.get("event_type") or "").replace("TRANSCRIPT_", "") == "UTTERANCE":
                utt = obj.get("utterance", "")
                if utt.startswith("[CUSTOMER]"):
                    if first_repair_ts is None or r["timestamp"] <= first_repair_ts:
                        rounds += 1
        sr_rounds = rounds

    # 转人工检测
    escalated = False
    escalation_reason = ""
    for r in rows:
        if r.get("source") == "gateway":
            continue
        try:
            obj = json.loads(r["message"])
        except (ValueError, TypeError):
            continue
        span_str = obj.get("span", "")
        if span_str:
            sp = _parse_span(span_str)
            if sp.get("span_name") == "escalate_agent":
                escalated = True
            if sp.get("span_name") == "execute_tool":
                if _re.search(r'Escalate', _tool_name_from_span(sp) or "", _re.IGNORECASE):
                    escalated = True
        prompt_raw = obj.get("prompt", "")
        if prompt_raw:
            try:
                prompt = json.loads(prompt_raw) if isinstance(prompt_raw, str) else prompt_raw
                for pmsg in (prompt.get("messages") or []):
                    for c in (pmsg.get("content") or []):
                        if isinstance(c, dict) and c.get("toolUse"):
                            tu = c["toolUse"]
                            if _re.search(r'Escalate', tu.get("name", ""), _re.IGNORECASE):
                                escalated = True
                                if tu.get("input", {}).get("reason"):
                                    escalation_reason = tu["input"]["reason"]
            except (ValueError, TypeError):
                pass

    ai_agent_calls = max(agentic, llm)
    response_latency = round(lat_sum / lat_n) if lat_n else None

    # guardrail 统计: 有结构化评估优先(防 ORCHESTRATION+inference 双计)，否则退回兜底
    _use_guard = _guard_assessed_events if _guard_assessed_events else []
    _reason_counts = {}  # text -> count
    for _assessed, _raw_msg in _use_guard:
        for _a in _assessed:
            _name = (_a.get("guardrailName") or _a.get("guardrailId") or "guardrail").strip()
            _policies = [p for p in (_a.get("policies") or []) if p]
            _hit = [p for p in _policies if str(p.get("action", "")).upper() != "NONE"]
            _use_p = _hit if _hit else _policies
            if not _use_p:
                _key = _name + " · " + str(_a.get("action", "BLOCKED"))
                _reason_counts[_key] = _reason_counts.get(_key, 0) + 1
            else:
                for _p in _use_p:
                    _parts = [_name]
                    if _p.get("policyType"):
                        _parts.append(str(_p["policyType"]))
                    _detail = _guardrail_detail_text(_p.get("details"))
                    if _detail:
                        _parts.append(_detail)
                    _key = " · ".join(_parts)
                    _reason_counts[_key] = _reason_counts.get(_key, 0) + 1
    if _guard_assessed_events:
        guard_blocks = sum(_reason_counts.values())
        guard_reasons_list = sorted(
            [{"text": t, "count": c} for t, c in _reason_counts.items()],
            key=lambda x: (-x["count"], x["text"])
        )
    else:
        # 兜底路径: 只有 guardrail_blocked 标记，无结构化评估
        guard_blocks = len(_guard_fallback_events)
        _fb_reasons = {}
        for _raw_msg in _guard_fallback_events:
            for _m2 in _re.finditer(r'guardrailName[\\"\'\\s]*[:=][\\"\'\\s]*([^\\"\'\\,}\\]]+)', _raw_msg):
                _n = _m2.group(1).strip()
                if _n:
                    _fb_reasons[_n] = _fb_reasons.get(_n, 0) + 1
        guard_reasons_list = sorted(
            [{"text": t, "count": c} for t, c in _fb_reasons.items()],
            key=lambda x: (-x["count"], x["text"])
        )

    # 写明细到 S3: events/{instanceName}/{contactId}.json.gz
    event_items = [
        {
            "timestamp": r["timestamp"],
            "message":   r["message"],
            "source":    r.get("source", "connect"),
            "contactId": cid,
        }
        for r in rows
    ]
    gz_body = gzip.compress(
        json.dumps(event_items, ensure_ascii=False).encode("utf-8"),
        compresslevel=6,
    )
    s3.put_object(
        Bucket=bucket,
        Key=events_key,
        Body=gz_body,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    # 写最小摘要到 DDB
    generated_at = (datetime.datetime.now(datetime.timezone.utc)
                    .isoformat().replace("+00:00", "Z"))

    # DescribeContact: 获取 Channel / InitiationMethod / DisconnectReason / DID / CSAT
    channel           = ""
    initiation_method = ""
    disconnect_reason = ""
    did               = ""
    csat              = ""
    if instance_id and connect_region:
        try:
            resp = _connect_client(connect_region).describe_contact(
                InstanceId=instance_id, ContactId=cid
            )
            contact = resp.get("Contact", {})
            channel           = contact.get("Channel", "")
            initiation_method = contact.get("InitiationMethod", "")
            disconnect_reason = contact.get("DisconnectReason", "")
            sys_ep = contact.get("SystemEndpoint") or {}
            did    = sys_ep.get("Address", "")
            attrs  = contact.get("Attributes") or {}
            csat   = attrs.get(csat_attr_key, "")
        except Exception as _dc_err:  # noqa: BLE001
            print(f"DescribeContact failed for {cid}: {_dc_err}")

    ddb_item = {
        "instanceName":      instance_name,
        "contactId":         cid,
        "count":             len(rows),
        "minTs":             min_ts,
        "maxTs":             max_ts,
        "sourceCounts":      json.dumps(source_counts),
        "generatedAt":       generated_at,
        # 日志可派生指标
        "locale":            locale,
        "bu":                bu,
        "srNumber":          sr_number,
        "mcpToolCalls":      mcp_tool_calls,
        "faqAsk":            faq_ask,
        "faqSend":           faq_send,
        "srIntent":          sr_intent,
        "srSuccess":         sr_success,
        "guardBlocks":       guard_blocks,
        "guardReasons":      guard_reasons_list,
        "errors":            errors,
        "escalated":         escalated,
        "escalationReason":  escalation_reason,
        "aiAgentCalls":      ai_agent_calls,
        # DescribeContact 补充字段
        "channel":           channel,
        "instChannel":       f'{instance_name}#{channel}',
        "initiationMethod":  initiation_method,
        "disconnectReason":  disconnect_reason,
        "did":               did,
        "csat":              csat,
    }
    if sr_rounds is not None:
        ddb_item["srRounds"] = sr_rounds
    if response_latency is not None:
        ddb_item["responseLatency"] = response_latency

    contacts_table.put_item(Item=ddb_item)

    summary = {
        "contactId":         cid,
        "count":             len(rows),
        "minTs":             min_ts,
        "maxTs":             max_ts,
        "sourceCounts":      source_counts,
        # 日志可派生指标（前端 computeMetrics 兜底路径直接读）
        "locale":            locale,
        "bu":                bu,
        "srNumber":          sr_number,
        "mcpToolCalls":      mcp_tool_calls,
        "faqAsk":            faq_ask,
        "faqSend":           faq_send,
        "srIntent":          sr_intent,
        "srSuccess":         sr_success,
        "guardBlocks":       guard_blocks,
        "guardReasons":      guard_reasons_list,
        "errors":            errors,
        "escalated":         escalated,
        "escalationReason":  escalation_reason,
        "aiAgentCalls":      ai_agent_calls,
        # DescribeContact 补充字段
        "channel":           channel,
        "instChannel":       f'{instance_name}#{channel}',
        "initiationMethod":  initiation_method,
        "disconnectReason":  disconnect_reason,
        "did":               did,
        "csat":              csat,
        "_summaryOnly":      True,
    }
    if sr_rounds is not None:
        summary["srRounds"] = sr_rounds
    if response_latency is not None:
        summary["responseLatency"] = response_latency

    return summary, "created"


# ---------------------------------------------------------------------------
# 主处理流程
# ---------------------------------------------------------------------------

def _process(bucket, connect_key, gateway_key, prefix, instance_name,
             instance_id="", connect_region="", csat_attr_key="botevaluation"):
    connect_rows = _read_ndjson(bucket, connect_key, "connect")
    gateway_rows = _read_ndjson(bucket, gateway_key, "gateway") if gateway_key else []

    cp.correlate(connect_rows, gateway_rows)

    connect_rows.extend(gateway_rows)
    gateway_rows = None
    all_rows = connect_rows
    connect_rows = None
    all_rows.sort(key=lambda r: r["timestamp"])
    event_count = len(all_rows)

    # 按 Contact 分组（丢弃无法关联真实 Contact 的行）
    groups = {}
    for r in all_rows:
        cid = _resolve_contact_id(r)
        if cid in (UNATTACHED_GATEWAY, DOUBLE_UUID_SESSION):
            continue
        groups.setdefault(cid, []).append(r)
    all_rows = None

    # 并发写 S3 + DDB
    contacts_summary = []
    workers = min(UPLOAD_WORKERS, len(groups)) or 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_write_contact, bucket, instance_name, cid, rows,
                            instance_id, connect_region, csat_attr_key): cid
                for cid, rows in groups.items()}
        for fut in concurrent.futures.as_completed(futs):
            summary, _ = fut.result()
            contacts_summary.append(summary)

    contacts_summary.sort(key=lambda e: e["contactId"])

    # 写 S3 index.json(仅供部署脚本轮询判断 Lambda 是否完成)
    index = {
        "generatedAt":  datetime.datetime.now(datetime.timezone.utc)
                        .isoformat().replace("+00:00", "Z"),
        "instanceName": instance_name,
        "contactCount": len(contacts_summary),
        "eventCount":   event_count,
        "storage":      "s3",
    }
    s3.put_object(Bucket=bucket, Key=prefix + "index.json",
                  Body=json.dumps(index, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")

    print("processed: instance=%s contacts=%d events=%d workers=%d"
          % (instance_name, len(contacts_summary), event_count, workers))
    return index


# ---------------------------------------------------------------------------
# 错误标记
# ---------------------------------------------------------------------------

def _write_error_index(bucket, prefix, err, instance_name):
    try:
        s3.put_object(
            Bucket=bucket, Key=prefix + "index.json",
            Body=json.dumps({
                "generatedAt":  datetime.datetime.now(datetime.timezone.utc)
                                .isoformat().replace("+00:00", "Z"),
                "instanceName": instance_name,
                "error":        str(err),
                "contactCount": 0,
                "eventCount":   0,
                "storage":      "s3",
            }, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json")
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Lambda 入口
# ---------------------------------------------------------------------------

def handler(event, context):
    if not CONTACTS_TABLE or not LOGS_BUCKET:
        raise RuntimeError(
            "环境变量 CONTACTS_TABLE、LOGS_BUCKET 未完整设置。"
            "请通过 setup_cloudfront.py 重新部署以注入正确的值。"
        )
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key    = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        trig   = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        prefix = trig.get("prefix", "")
        instance_name = trig.get("instanceName", "")
        instance_id   = trig.get("instanceId", "")
        connect_region = trig.get("connectRegion", "")
        csat_attr_key  = trig.get("csatAttrKey", os.environ.get("CSAT_ATTRIBUTE_KEY", "botevaluation"))
        if not instance_name:
            raise RuntimeError(
                f"触发 JSON {key} 中缺少 instanceName 字段，无法确定数据归属实例。"
            )
        try:
            _process(bucket, trig["connect"], trig.get("gateway"), prefix, instance_name,
                     instance_id, connect_region, csat_attr_key)
        except Exception as err:  # noqa: BLE001
            traceback.print_exc()
            _write_error_index(bucket, prefix, err, instance_name)
            raise
    return {"ok": True}
