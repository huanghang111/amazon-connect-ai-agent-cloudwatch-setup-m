#!/usr/bin/env python3
"""Pure event-to-summary calculation shared by the splitter and backfill CLI."""
import json
import re

# Fields whose values are derived only from persisted event rows and may be backfilled.
BACKFILLABLE_FIELDS = (
    "count", "minTs", "maxTs", "sourceCounts", "locale", "bu", "srNumber",
    "mcpToolCalls", "faqAsk", "faqSend", "srIntent", "srSuccess", "srRounds",
    "guardBlocks", "guardReasons", "errors", "escalated", "escalationReason",
    "aiAgentCalls", "responseLatency",
)


def _slice_balanced(value, start):
    depth = 0
    for index in range(start, len(value)):
        char = value[index]
        if char in ("[", "{"):
            depth += 1
        elif char in ("]", "}"):
            depth -= 1
            if depth == 0:
                return value[start:index + 1]
    return ""


def _extract_guard_assessments(raw_msg):
    texts = []
    try:
        obj = json.loads(raw_msg)
        span_str = obj.get("span", "")
        if span_str and "guardrail_assessments" in str(span_str):
            texts.append(str(span_str))
    except (ValueError, TypeError, AttributeError):
        pass
    if not texts and "guardrail_assessments" in raw_msg:
        texts.append(raw_msg)
    if not texts:
        return None

    out = []
    for text in texts:
        value = re.sub(r'\\+"', '"', text)
        arrays = []
        if "guardrail_assessments" not in value:
            index = value.find("[")
            segment = _slice_balanced(value, index) if index != -1 else ""
            if segment:
                arrays.append(segment)
        else:
            position = 0
            while True:
                key_index = value.find("guardrail_assessments", position)
                if key_index == -1:
                    break
                index = value.find("[", key_index)
                if index == -1:
                    break
                segment = _slice_balanced(value, index)
                if not segment:
                    break
                arrays.append(segment)
                position = index + len(segment)
        for array in arrays:
            try:
                parsed = json.loads(array)
                if isinstance(parsed, list):
                    out.extend(item for item in parsed if isinstance(item, dict))
            except (ValueError, TypeError):
                pass
    return out


def _guardrail_detail_text(detail):
    if detail is None:
        return ""
    if isinstance(detail, str):
        return detail.strip()
    if isinstance(detail, list):
        return " / ".join(filter(None, (_guardrail_detail_text(item) for item in detail)))
    if isinstance(detail, dict):
        for key in ("name", "topic", "type", "filterType", "match", "detail"):
            if isinstance(detail.get(key), str) and detail[key]:
                return detail[key]
        try:
            return json.dumps(detail, ensure_ascii=False)
        except Exception:
            return ""
    return str(detail)


def _parse_span(span_str):
    out = {}
    if not isinstance(span_str, str):
        return out
    value = span_str.strip()
    if value.startswith("{"):
        value = value[1:]
    if value.endswith("}"):
        value = value[:-1]
    parts = []
    depth = 0
    current = ""
    for char in value:
        if char in ("[", "{"):
            depth += 1
        elif char in ("]", "}"):
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    if current.strip():
        parts.append(current)
    for part in parts:
        equals = part.find("=")
        if equals != -1:
            out[part[:equals].strip()] = part[equals + 1:].strip()
    return out


def _tool_name_from_span(span):
    raw = span.get("input_messages", "")
    try:
        messages = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(messages, list):
            for message in messages:
                for value in (message.get("values") or []):
                    if isinstance(value, dict) and value.get("toolUse", {}).get("name"):
                        return value["toolUse"]["name"]
    except (ValueError, TypeError, AttributeError):
        pass
    return span.get("tool_name") or span.get("tool") or ""


def _short_tool_name(name):
    if not name:
        return ""
    index = name.rfind("___")
    return name[index + 3:] if index >= 0 else name


def _tool_output_text(span):
    raw = span.get("output_messages", "")
    text = ""
    try:
        messages = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(messages, list):
            for message in messages:
                for value in (message.get("values") or []):
                    if isinstance(value, dict) and value.get("toolResult"):
                        for content in (value["toolResult"].get("content") or []):
                            text += content.get("text") or json.dumps(content)
    except (ValueError, TypeError, AttributeError):
        pass
    return text or (raw if isinstance(raw, str) else "")


def _tool_succeeded(span):
    if span.get("status") and span["status"] != "OK":
        return False
    text = _tool_output_text(span)
    return not re.search(r'"error"', text) and not re.search(r'HTTP\s*[45]\d\d', text)


def _repair_ticket_returned(span):
    match = re.search(
        r'ticketNumber\\*["\']?\s*:\s*\\*["\']?\s*([^"\'\\,}\s]+)',
        _tool_output_text(span), re.IGNORECASE,
    )
    if not match:
        return False
    value = (match.group(1) or "").strip()
    return bool(value) and value.lower() not in ("null", "none")


def _try_parse(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return None


def _is_escalate_tool(name):
    return isinstance(name, str) and bool(re.search(r"Escalate", name, re.IGNORECASE))


def _unescape_quotes(value):
    return re.sub(r'\\+(["\'\\])', r'\1', value)


def _reason_from_text(value):
    if not isinstance(value, str):
        return ""
    match = re.search(r'\bEscalate\s*\(\s*reason\s*=\s*(\\*)(["\'])([\s\S]*?)\1\2\s*\)', value)
    if match:
        return _unescape_quotes(match.group(3))
    match = re.search(r'\breason\\*["\']?\s*[:=]\s*(\\*)(["\'])([\s\S]*?)\1\2', value)
    return _unescape_quotes(match.group(3)) if match else ""


def _reason_from_tool_input(value):
    if isinstance(value, dict):
        reason = value.get("reason")
        return reason if isinstance(reason, str) else ""
    if isinstance(value, str):
        parsed = _try_parse(value)
        if isinstance(parsed, dict) and isinstance(parsed.get("reason"), str):
            return parsed["reason"]
        return _reason_from_text(value)
    return ""


def _reasons_from_tool_use_blocks(value):
    out = []
    for match in re.finditer(r'Name=([^,\s)]+),\s*Input="((?:[^"\\]|\\.)*)"', value):
        if not _is_escalate_tool(match.group(1)):
            continue
        try:
            tool_input = json.loads('"' + match.group(2) + '"')
        except (ValueError, TypeError):
            tool_input = match.group(2)
        out.append(_reason_from_tool_input(tool_input))
    return out


def _detect_escalation(rows):
    state = {"flag": False}
    reasons = []

    def add(reason):
        value = " ".join(str(reason or "").split())
        if value:
            state["flag"] = True
            if value not in reasons:
                reasons.append(value)

    def from_span(obj):
        span_str = obj.get("span")
        if not span_str:
            return
        span = _parse_span(span_str)
        name = span.get("span_name")
        if name == "escalate_agent":
            state["flag"] = True
        if name != "execute_tool":
            return
        if _is_escalate_tool(_tool_name_from_span(span)):
            state["flag"] = True
        for raw in (span.get("input_messages"), span.get("output_messages")):
            if not raw:
                continue
            messages = _try_parse(raw)
            if isinstance(messages, list):
                for message in messages:
                    values = message.get("values") if isinstance(message, dict) else None
                    for value in values if isinstance(values, list) else []:
                        tool_use = value.get("toolUse") if isinstance(value, dict) else None
                        if isinstance(tool_use, dict) and _is_escalate_tool(tool_use.get("name")):
                            add(_reason_from_tool_input(tool_use.get("input")))
            elif isinstance(raw, str) and re.search(r"Escalate", raw, re.IGNORECASE):
                add(_reason_from_text(raw))

    def from_completion(obj):
        completion = obj.get("completion")
        if not completion:
            return
        parsed = _try_parse(completion)
        tool_uses = parsed.get("toolUseList") if isinstance(parsed, dict) else None
        for tool_use in tool_uses if isinstance(tool_uses, list) else []:
            if isinstance(tool_use, dict) and _is_escalate_tool(tool_use.get("toolName")):
                add(_reason_from_tool_input(tool_use.get("toolInput")))
        if (isinstance(completion, str) and not isinstance(parsed, (dict, list)) and
                re.search(r"Escalate", completion, re.IGNORECASE)):
            add(_reason_from_text(completion))

    def from_parsed_response(obj):
        value = obj.get("parsed_response")
        if not isinstance(value, str):
            return
        found = _reasons_from_tool_use_blocks(value)
        for reason in found:
            add(reason)
        if not found and re.search(r"\bEscalate\s*\(", value):
            add(_reason_from_text(value))

    def from_prompt(obj):
        prompt = _try_parse(obj.get("prompt")) if obj.get("prompt") else None
        if not isinstance(prompt, dict):
            return
        for message in (prompt.get("messages") or []):
            for content in (message.get("content") or []) if isinstance(message, dict) else []:
                tool_use = content.get("toolUse") if isinstance(content, dict) else None
                if isinstance(tool_use, dict) and _is_escalate_tool(tool_use.get("name")):
                    state["flag"] = True
                    add(_reason_from_tool_input(tool_use.get("input")))

    for row in rows:
        try:
            if row.get("source") == "gateway":
                continue
            obj = json.loads(row["message"])
            if not isinstance(obj, dict):
                continue
        except Exception:
            continue
        for scanner in (from_span, from_completion, from_parsed_response, from_prompt):
            try:
                scanner(obj)
            except Exception:
                continue
    return state["flag"], "; ".join(reasons)


def build_contact_summary(rows):
    """Return all log-derived DynamoDB fields without mutating event rows."""
    if not rows:
        raise ValueError("events array must not be empty")
    ordered = sorted(rows, key=lambda row: (
        row["timestamp"], row.get("source", "connect"), row["message"]
    ))
    source_counts = {}
    for row in ordered:
        source = row.get("source", "connect")
        source_counts[source] = source_counts.get(source, 0) + 1

    locale = bu = sr_number = ""
    mcp_tool_calls = faq_ask = faq_send = sr_intent = sr_success = 0
    first_repair_ts = None
    assessed_events = []
    fallback_events = []
    latency_sum = latency_count = agentic = llm = errors = 0

    for row in ordered:
        message = row.get("message", "")
        source = row.get("source", "connect")
        timestamp = row.get("timestamp", 0)
        if not locale:
            match = (re.search(r'language locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', message) or
                     re.search(r'configured locale[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', message) or
                     re.search(r'\blocale\b[^A-Za-z]{0,12}([a-z]{2}_[A-Z]{2})', message) or
                     re.search(r'responseLanguage[^A-Za-z]{0,12}([a-z]{2}[_-][A-Z]{2})', message))
            if match:
                locale = match.group(1)
        if not bu:
            match = re.search(r'-\s*BU:\s*([^\\"\n<}]+)', message)
            if match:
                bu = match.group(1).strip()
        if not sr_number:
            match = re.search(r'srNumber\\*["\']?\s*:\s*\\*["\']?\s*([^"\'\\,}\s]+)', message, re.IGNORECASE)
            if match and match.group(1).strip().lower() not in ("null", "none"):
                sr_number = match.group(1).strip()

        assessments = _extract_guard_assessments(message)
        if assessments is not None:
            blocked = [item for item in assessments if str(item.get("action", "")).upper() != "NONE"]
            if blocked:
                assessed_events.append(blocked)
        elif '"guardrail_blocked": true' in message or "'guardrail_blocked': True" in message:
            fallback_events.append(message)

        if source == "gateway":
            if re.search(r"\b(ERROR|Exception|Traceback|FATAL)\b", message):
                errors += 1
            continue
        try:
            obj = json.loads(message)
        except (ValueError, TypeError):
            continue
        event_type = (obj.get("event_type") or "").replace("TRANSCRIPT_", "")
        agentic += event_type == "AGENTIC_MESSAGE"
        llm += event_type == "LARGE_LANGUAGE_MODEL_INVOCATION"
        span_str = obj.get("span", "")
        if not span_str:
            continue
        span = _parse_span(span_str)
        if span.get("status") and span["status"] != "OK":
            errors += 1
        span_name = span.get("span_name", "")
        if span_name == "inference":
            try:
                latency_sum += int(span["end_timestamp"]) - int(span["start_timestamp"])
                latency_count += 1
            except (KeyError, ValueError, TypeError):
                pass
        if span_name == "execute_tool":
            mcp_tool_calls += 1
            tool_name = _short_tool_name(_tool_name_from_span(span))
            if re.search(r"faqSearch", tool_name, re.IGNORECASE):
                faq_ask += 1
                faq_send += _tool_succeeded(span)
            if re.search(r"requestRepair", tool_name, re.IGNORECASE):
                sr_intent += 1
                sr_success += _repair_ticket_returned(span)
                if first_repair_ts is None:
                    first_repair_ts = timestamp

    sr_rounds = None
    if sr_intent:
        sr_rounds = 0
        for row in ordered:
            if row.get("source") == "gateway":
                continue
            try:
                obj = json.loads(row["message"])
            except (ValueError, TypeError):
                continue
            if ((obj.get("event_type") or "").replace("TRANSCRIPT_", "") == "UTTERANCE" and
                    obj.get("utterance", "").startswith("[CUSTOMER]") and
                    (first_repair_ts is None or row["timestamp"] <= first_repair_ts)):
                sr_rounds += 1

    escalated, escalation_reason = _detect_escalation(ordered)
    reason_counts = {}
    for assessments in assessed_events:
        for assessment in assessments:
            name = (assessment.get("guardrailName") or assessment.get("guardrailId") or "guardrail").strip()
            policies = [policy for policy in (assessment.get("policies") or []) if policy]
            blocked = [policy for policy in policies if str(policy.get("action", "")).upper() != "NONE"]
            selected = blocked if blocked else policies
            if not selected:
                key = name + " · " + str(assessment.get("action", "BLOCKED"))
                reason_counts[key] = reason_counts.get(key, 0) + 1
            for policy in selected:
                parts = [name]
                if policy.get("policyType"):
                    parts.append(str(policy["policyType"]))
                detail = _guardrail_detail_text(policy.get("details"))
                if detail:
                    parts.append(detail)
                key = " · ".join(parts)
                reason_counts[key] = reason_counts.get(key, 0) + 1
    if assessed_events:
        guard_blocks = sum(reason_counts.values())
    else:
        guard_blocks = len(fallback_events)
        for message in fallback_events:
            for match in re.finditer(r'guardrailName[\\"\'\\s]*[:=][\\"\'\\s]*([^\\"\'\\,}\]]+)', message):
                name = match.group(1).strip()
                if name:
                    reason_counts[name] = reason_counts.get(name, 0) + 1
    guard_reasons = sorted(
        ({"text": text, "count": count} for text, count in reason_counts.items()),
        key=lambda item: (-item["count"], item["text"]),
    )

    result = {
        "count": len(ordered), "minTs": ordered[0]["timestamp"],
        "maxTs": ordered[-1]["timestamp"], "sourceCounts": json.dumps(source_counts),
        "locale": locale, "bu": bu, "srNumber": sr_number,
        "mcpToolCalls": mcp_tool_calls, "faqAsk": faq_ask, "faqSend": faq_send,
        "srIntent": sr_intent, "srSuccess": sr_success,
        "guardBlocks": guard_blocks, "guardReasons": guard_reasons, "errors": errors,
        "escalated": escalated, "escalationReason": escalation_reason,
        "aiAgentCalls": max(agentic, llm),
        "srRounds": sr_rounds,
        "responseLatency": round(latency_sum / latency_count) if latency_count else None,
    }
    return result
