"""Convert Amazon Connect AI Agent logs to OTEL GenAI spans for AgentCore Evaluation.

Input : Connect AI Agent log records (CloudWatch export - CSV or raw JSON events)
Output: per-session sessionSpans list (spans + events) compatible with
        bedrock-agentcore Evaluate API / OTEL gen_ai semantic conventions.

Two record formats are understood, picked per session:

  * TRACE   - TRANSCRIPT_AI_AGENT_TRACE events carry the complete span tree
              (invoke_agent / inference / execute_tool) with messages and token
              usage. Preferred whenever a session has them.
  * LEGACY  - sessions logged before Connect emitted trace events only have
              TRANSCRIPT_AGENTIC_MESSAGE / TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION
              (one per model call: the full Converse-style prompt and the
              completion). The span tree is rebuilt from those: one invoke_agent
              per customer turn, one chat span per model call, one execute_tool
              per tool round trip found in the following prompt.

Three properties of Connect's message lists have to be undone before the result
is a conversation (all three verified against the matching
TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION record, which shows the prompt the
model actually received):

  * `input_messages` is REVERSE chronological; `output_messages` is forward. Every
    MESSAGE carries its own `timestamp`, so both are sorted by it.
  * one assistant turn is split across several MESSAGE objects: the visible reply,
    the reasoning, then one per tool call. The reasoning one has no text block, so
    taken literally the turn ended on an empty message and quality evaluators
    scored it as "the assistant did not answer".
  * some agents add a running-total customer message holding every utterance so
    far joined with spaces, which reads as a second copy of the user side.

Span shapes differ between agent types, all seen in real accounts:

  * self-service orchestrators: UUID span ids, `operation_name` set, messages as
    MESSAGE objects with text / toolUse / toolResult values.
  * agent-assist / analytics agents (e.g. AmazonConnectRAPIDCore): 16-hex span
    ids, NO `operation_name` on inference spans, tool spans whose
    `operation_name` is the TOOL NAME (span_name is what says execute_tool), and
    messages nested one level deeper - a single text value holding a JSON list of
    {participant, timestamp, text}. Tool arguments and results are plain text.
  * manual-search agents: invoke_agent spans with no messages at all.

So the span kind is read from `span_name` first, values are stripped, nested
message lists are expanded, and a span with nothing to evaluate is dropped or
given a placeholder event (see build_session_spans).

Known deviation: when a turn issues several tool calls, they are grouped as
`assistant[use, use, use]` + one `tool` message per result, whereas the model's
own prompt interleaves them use/result. Same calls, same ids, same order.
"""

import csv
import hashlib
import json
import re
import sys
from collections import defaultdict

SCOPE = {"name": "strands.telemetry.tracer", "version": ""}
SERVICE_NAME = "connect-ai-agent"

TRACE_EVENT = "TRANSCRIPT_AI_AGENT_TRACE"
# legacy per-model-call records; both carry the same prompt/completion for one
# generation, so they are deduplicated by generation id
LEGACY_EVENTS = ("TRANSCRIPT_AGENTIC_MESSAGE",
                 "TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION")
# canonical span kinds Connect emits; anything else in operation_name is a
# tool name (agent-assist agents) or an internal marker
KNOWN_OPS = {"invoke_agent", "inference", "execute_tool", "escalate_agent",
             "barge_in"}


# ---------------------------------------------------------------- parsing

def _fix_newlines(s: str) -> str:
    # CloudWatch CSV export renders "\n" inside JSON strings as backslash+LF
    return s.replace("\\\n", "\\n")


_DECODER = json.JSONDecoder(strict=False)


def _loads(raw, default=None):
    """Lenient JSON: Connect embeds raw newlines/control chars in strings."""
    if raw is None or raw == "":
        return default
    if not isinstance(raw, str):
        return raw
    try:
        return _DECODER.decode(raw)
    except json.JSONDecodeError:
        return default


def parse_csv(path):
    """Yield parsed Connect log message dicts from a CloudWatch CSV export."""
    with open(path) as f:
        for row in csv.DictReader(f):
            yield _DECODER.decode(_fix_newlines(row["message"]))


def parse_span_field(s: str) -> dict:
    """Parse Connect's `{k=v, k=v, ...}` span serialization (values may contain
    nested JSON arrays/objects), returning a flat dict of raw string values.

    Values are stripped: some agents log `ai_agent_name=Foo  ,` with trailing
    blanks, which otherwise leak into span names and attributes."""
    body = s.strip()[1:-1]
    parts, depth, cur, inq, prev = [], 0, "", False, ""
    for ch in body:
        if inq:
            cur += ch
            if ch == '"' and prev != "\\":
                inq = False
        else:
            if ch == '"':
                inq, cur = True, cur + ch
            elif ch in "[{(":
                depth, cur = depth + 1, cur + ch
            elif ch in "]})":
                depth, cur = depth - 1, cur + ch
            elif ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        prev = ch
    if cur.strip():
        parts.append(cur)
    out = {}
    for p in parts:
        k, _, v = p.strip().partition("=")
        v = v.strip()
        if v in ("", "null", "None"):
            continue
        out[k.strip()] = v
    return out


def span_kind(s) -> str:
    """Canonical span kind: invoke_agent / inference / execute_tool / ...

    `span_name` is authoritative. `operation_name` is only a fallback: agent-assist
    agents omit it on inference spans and put the tool's name in it on tool spans.
    """
    name = s.get("span_name")
    if name in KNOWN_OPS:
        return name
    op = s.get("operation_name")
    if op in KNOWN_OPS:
        return op
    return name or op or "unknown"


# ---------------------------------------------------------------- id mapping

def otel_span_id(uuid_str: str, salt: str = "") -> str:
    return hashlib.md5(("span:" + salt + uuid_str).encode()).hexdigest()[:16]


def otel_trace_id(root_uuid: str, salt: str = "") -> str:
    return hashlib.md5(("trace:" + salt + root_uuid).encode()).hexdigest()


# ---------------------------------------------------------------- messages

def expand_nested_messages(msgs):
    """Undo the agent-assist agents' extra nesting.

    Those agents log ONE MESSAGE whose single text value is itself a JSON list of
    {"messageId", "participant", "timestamp", "text"} objects. Read literally the
    whole conversation is one opaque string on the wrong participant; expanded, it
    is the same MESSAGE list every other agent logs.
    """
    out = []
    for m in msgs:
        if not isinstance(m, dict):
            continue
        values = m.get("values") or []
        inner = None
        if len(values) == 1 and isinstance(values[0], dict) and "text" in values[0]:
            txt = (values[0]["text"] or {}).get("value", "")
            if isinstance(txt, str) and txt.lstrip().startswith("[{"):
                cand = _loads(txt)
                if isinstance(cand, list) and cand and all(
                        isinstance(x, dict) and "participant" in x and "text" in x
                        for x in cand):
                    inner = cand
        if inner is None:
            out.append(m)
            continue
        for x in inner:
            out.append({"messageId": x.get("messageId"),
                        "participant": x.get("participant"),
                        "timestamp": x.get("timestamp") or m.get("timestamp"),
                        "values": [{"text": {"value": x.get("text") or ""}}]})
    return out


def message_blocks(m):
    """One Connect MESSAGE -> (content blocks, has_tool, is_tool_result).

    Only the values that are part of the conversation become blocks. A "reasoning"
    value is the model's internal chain of thought, which Connect logs as its OWN
    MESSAGE next to the visible reply; it must not become a message of its own
    (see conversation_messages) and must not be graded as if it were the answer.
    """
    blocks, has_tool, is_tool_result = [], False, False
    for v in m.get("values", []):
        if not isinstance(v, dict):
            continue
        if "text" in v:
            txt = ((v["text"] or {}).get("value") or "").strip()
            if txt:
                blocks.append({"text": txt})
        elif "toolUse" in v:
            tu = v["toolUse"] or {}
            inp = tu.get("input")
            if isinstance(inp, str):
                inp = _loads(inp, inp)
            blocks.append({"toolUse": {"toolUseId": tu.get("toolUseId"),
                                       "name": tu.get("name"), "input": inp}})
            has_tool = True
        elif "toolResult" in v:
            tr_ = v["toolResult"] or {}
            # Connect uses either "content" (blocks) or "value" (string)
            blocks.append({"toolResult": {"toolUseId": tr_.get("toolUseId"),
                                          "status": "success",
                                          "content": tool_result_blocks(tr_)}})
            has_tool, is_tool_result = True, True
    return blocks, has_tool, is_tool_result


# Who is talking to the AI agent. Self-service agents talk to the CUSTOMER;
# agent-assist agents (manual search, RAPID analytics) talk to the human AGENT,
# who is the user of the assistant in that use case. Everything the AI says is
# logged as BOT.
BOT_PARTICIPANTS = {"BOT", "ASSISTANT", "SYSTEM", "AI_AGENT"}


def is_user_message(m):
    return str(m.get("participant") or "").upper() not in BOT_PARTICIPANTS


def _user_text(m):
    return " ".join(" ".join((v["text"] or {}).get("value", "")
                             for v in m.get("values", [])
                             if isinstance(v, dict) and "text" in v).split())


def drop_aggregate_user_message(msgs):
    """Remove Connect's running-total customer message, if present.

    Alongside the real turns, Connect adds one extra CUSTOMER message per
    inference span whose text is every customer utterance so far joined with
    spaces ("hi, I have some device defect 7646 it's an oven ..."). It carries the
    oldest timestamp, so it sorts to the front of the conversation and reads as a
    second copy of the whole user side - which is what showed up as those
    ever-growing strings in the Insights user-intent clusters.

    Identified by content, not position: the message whose text equals the other
    customer texts joined in order. At most one is dropped, so a genuine repeated
    utterance survives (with two identical utterances either one may go, and the
    conversation is the same afterwards).
    """
    users = [m for m in msgs if is_user_message(m)]
    if len(users) < 2:
        return msgs
    for cand in users:
        others = [_user_text(u) for u in users if u is not cand]
        if all(others) and _user_text(cand) == " ".join(others):
            return [m for m in msgs if m is not cand]
    return msgs


def conversation_messages(msgs):
    """Connect MESSAGE list -> one logical message per conversation turn.

    Connect splits a single assistant turn across several MESSAGE objects - the
    visible reply, the reasoning, then one per tool call - and logs
    input_messages in REVERSE chronological order (output_messages is forward).
    Taken literally, that produced a conversation that ran backwards and ended on
    an empty "<NO_RESPONSE>" assistant message (the reasoning one, which has no
    text block), so every quality evaluator scored the turn as "the assistant did
    not answer" even though it had. Hence: sort by the per-message timestamp,
    drop the running-total user message, drop what is not conversation, and merge
    what belongs to one turn back together.
    """
    out = []
    msgs = expand_nested_messages(msgs)
    for m in drop_aggregate_user_message(sorted(msgs, key=lambda x: x.get("timestamp") or 0)):
        blocks, has_tool, is_tool_result = message_blocks(m)
        if not blocks:
            continue                       # reasoning-only: not a turn of its own
        role = ("tool" if is_tool_result
                else "user" if is_user_message(m) else "assistant")
        prev = out[-1] if out else None
        # reply + tool calls are one assistant turn; a toolResult always stands alone
        if prev and prev["role"] == role == "assistant" and not is_tool_result \
                and not prev["is_tool_result"]:
            prev["blocks"] += blocks
            prev["has_tool"] = prev["has_tool"] or has_tool
        elif prev and prev["role"] == role and prev["blocks"] == blocks:
            continue                       # same turn logged twice
        else:
            out.append({"role": role, "blocks": blocks, "has_tool": has_tool,
                        "is_tool_result": is_tool_result})
    return out


def turns_to_otel(turns, direction):
    """Logical turns -> Strands-style gen_ai messages.

    direction: 'input' | 'output' (controls content shape). Strands-native content
    blocks: Recommendation's tool matching only recognizes toolUse blocks in this
    exact shape (Evaluate is laxer).
    """
    out = []
    for t in turns:
        blocks, role, has_tool = t["blocks"], t["role"], t["has_tool"]
        text = "\n".join(b["text"] for b in blocks if "text" in b).strip()
        # mirror the shapes emitted by strands.telemetry.tracer
        if direction == "output" and role == "assistant":
            content = {"message": json.dumps(blocks, ensure_ascii=False) if has_tool else text,
                       "finish_reason": "tool_use" if has_tool else "end_turn"}
        elif role in ("user", "tool") or has_tool:
            content = {"content": json.dumps(blocks, ensure_ascii=False)}
        else:
            content = text
        out.append({"role": role, "content": content})
    return out


def _placeholder_turn(direction):
    """The evaluator needs *something* on both sides of a span it maps."""
    return {"role": "assistant" if direction == "output" else "user",
            "blocks": [{"text": "<NO_RESPONSE>" if direction == "output"
                        else "<EMPTY_USER_INPUT>"}],
            "has_tool": False, "is_tool_result": False}


def connect_messages_to_otel(raw: str, direction: str):
    """Connect input_messages/output_messages JSON -> Strands-style gen_ai
    messages. direction: 'input' | 'output' (controls content shape)."""
    msgs = _loads(raw)
    if not isinstance(msgs, list) or not msgs:
        return []
    turns = conversation_messages(msgs)
    if not turns:
        # nothing conversational survived (e.g. a reasoning-only span)
        turns = [_placeholder_turn(direction)]
    return turns_to_otel(turns, direction)


def trim_to_user_query(msgs):
    """Cut an agent-span conversation off after its last user message.

    An AGENT span represents one user turn, and the evaluator reads that turn's
    user query from the END of input.messages: leave a tool result or an assistant
    reply there and the whole session fails with

        AgentSpanMappingException: Failed to parse user_query from agent-span
        with spanId: <id> and scope: strands.telemetry.tracer

    The turn's tool round trips are not lost - they are evaluated on the child
    chat / execute_tool spans, which is also where Strands puts them.

    (While input_messages was being read in Connect's reverse order this happened
    to hold by accident: the oldest messages sorted last, and the oldest message
    is a customer utterance.)

    A turn the agent started on its own - a voice agent's greeting or "are you
    still there?" - has no customer utterance to end on, so it gets the same
    explicit placeholder the rest of the converter uses for that case.
    """
    for i in range(len(msgs) - 1, -1, -1):
        if msgs[i]["role"] == "user":
            return msgs[:i + 1]
    return msgs + [{"role": "user", "content": {"content": json.dumps(
        [{"text": "<EMPTY_USER_INPUT>"}], ensure_ascii=False)}}]


def system_prompt_text(raw):
    """Extract the system prompt from a span's system_instructions field.

    Connect serializes it as a list of MESSAGE objects, i.e.
    [{"messageId", "participant", "timestamp", "values": [{"text": {"value": ...}}]}]
    - the prompt text sits two levels down, not directly on the list items.
    """
    parsed = _loads(raw)
    if parsed is None:
        return raw.strip() if isinstance(raw, str) and raw and not raw.startswith("[") else ""
    if isinstance(parsed, str):
        return parsed
    chunks = []
    for m in parsed if isinstance(parsed, list) else [parsed]:
        if not isinstance(m, dict):
            chunks.append(str(m))
            continue
        for v in m.get("values", []):
            if isinstance(v, dict) and "text" in v:
                chunks.append((v["text"] or {}).get("value", ""))
        if "value" in m:  # tolerate a flatter shape too
            chunks.append(str(m["value"]))
        if "text" in m and isinstance(m["text"], str):  # Converse system block
            chunks.append(m["text"])
    return "\n".join(c for c in chunks if c).strip()


def tool_result_text(tr):
    """Tool output as a non-empty string, across both shapes Connect emits.

    Two shapes seen in the wild:
      * {"toolUseId", "value": "<json string>"}  - MCP-backed tools
      * {"toolUseId", "content": [...]}           - content-block form
    Returning "" here is not an option: the evaluator raises
    ToolSpanMappingException ("Failed to parse tool_output from tool-span")
    when the output is an empty JSON string, which fails the whole session.
    """
    if not isinstance(tr, dict):
        return ""
    if tr.get("value") not in (None, ""):
        v = tr["value"]
        return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    if tr.get("content") not in (None, "", []):
        return json.dumps(tr["content"], ensure_ascii=False)
    return ""


def tool_result_blocks(tr):
    """Tool output as a content-block LIST, ready to be JSON-serialized.

    The evaluator json-parses tool_output and raises ToolSpanMappingException if
    that fails. Connect's `value` string cannot be forwarded as-is: MCP tools
    embed inner JSON inside it without escaping, e.g.
        {"status": "success", "output": "{"woNumber": "9926972953"}"}
    which is not valid JSON. So a raw string is always wrapped in a text block,
    which is also the shape that evaluated successfully end to end.
    """
    if isinstance(tr, dict) and isinstance(tr.get("content"), list) and tr["content"]:
        return tr["content"]
    txt = tool_result_text(tr)
    return [{"text": txt}] if txt else [{"text": "OK"}]


def _all_text(raw):
    """Every text value in a Connect MESSAGE list, joined. For agent-assist tool
    spans, whose arguments and results are logged as plain text."""
    msgs = _loads(raw)
    if not isinstance(msgs, list):
        return ""
    parts = []
    for m in expand_nested_messages(msgs):
        for v in m.get("values", []):
            if isinstance(v, dict) and "text" in v:
                t = ((v["text"] or {}).get("value") or "").strip()
                if t:
                    parts.append(t)
    return "\n".join(parts)


def tool_event_messages(s):
    """Build execute_tool event body messages in the Strands tool shape:
    input = tool arguments, output = tool result.

    Self-service orchestrators log a toolUse (and sometimes a toolResult) in
    input_messages. Agent-assist agents log the arguments as plain text in
    input_messages and the result as plain text in output_messages, with the tool
    name in operation_name; that is the fallback path.
    -> (input msgs, output msgs, tool name)"""
    tool_use, tool_result = None, None
    for field in ("input_messages", "output_messages"):
        msgs = _loads(s.get(field))
        if not isinstance(msgs, list):
            continue
        for m in msgs:
            for v in (m.get("values") or []) if isinstance(m, dict) else []:
                if not isinstance(v, dict):
                    continue
                if "toolUse" in v and tool_use is None:
                    tool_use = v["toolUse"]
                elif "toolResult" in v and tool_result is None:
                    tool_result = v["toolResult"]
    if tool_use is not None:
        tid = tool_use.get("toolUseId", "")
        name = tool_use.get("name", "")
        args = tool_use.get("input", "{}")
    else:
        args = _all_text(s.get("input_messages")) or "{}"
        out_txt = _all_text(s.get("output_messages"))
        if not args.strip("{} \n") and not out_txt:
            return [], [], ""
        op = s.get("operation_name")
        name = op if op and op not in KNOWN_OPS else ""
        tid = "tooluse_" + otel_span_id(s.get("span_id", ""))
        tool_result = {"value": out_txt} if out_txt else None
    in_msgs = [{"role": "tool", "content": {
        "content": args, "role": "tool", "id": tid}}]
    # always valid JSON, never empty: see tool_result_blocks
    result_txt = json.dumps(tool_result_blocks(tool_result), ensure_ascii=False)
    out_msgs = [{"role": "assistant", "content": {
        "message": result_txt, "id": tid}}]
    return in_msgs, out_msgs, name


# ---------------------------------------------------------------- conversion

SESSION_SALT_SEP = "--run-"


def eval_session_id(session_id, salt=""):
    """The session.id written into Observability, unique per run.

    Batch evaluation collects a session's spans by session.id out of the shared
    runtime log group, so a Connect session that is ingested twice would be
    evaluated over BOTH copies - including any spans an older, buggier converter
    produced. Scoping the id to the run keeps each run's evaluation self-contained.
    Use connect_session_id() to get the original id back for reporting.
    """
    return f"{session_id}{SESSION_SALT_SEP}{salt}" if salt else session_id


def connect_session_id(eval_id):
    """Inverse of eval_session_id: the Connect sessionId, for reports/ledger."""
    return (eval_id or "").split(SESSION_SALT_SEP)[0]


RESOURCE_ATTRS = {
    "service.name": SERVICE_NAME,
    "aws.local.service": SERVICE_NAME,
    "telemetry.sdk.name": "opentelemetry",
    "telemetry.sdk.language": "python",
    "aws.service.type": "gen_ai_agent",
    "cloud.provider": "aws",
}


def _span(span_id, trace_id, name, start_ns, end_ns, attrs, status="OK",
          status_desc="", parent=None):
    end_ns = max(end_ns, start_ns + 1_000_000)
    st = {"code": "OK" if status == "OK" else "ERROR" if status == "ERROR" else "UNSET"}
    if status_desc:
        st["message"] = status_desc
    out = {
        "spanId": span_id, "traceId": trace_id, "name": name, "kind": "INTERNAL",
        "startTimeUnixNano": start_ns, "endTimeUnixNano": end_ns,
        "durationNano": end_ns - start_ns, "scope": SCOPE, "attributes": attrs,
        "status": st, "resource": {"attributes": RESOURCE_ATTRS},
    }
    if parent:
        out["parentSpanId"] = parent
    return out


def _event(span_id, trace_id, session_id, in_msgs, out_msgs, time_ns):
    return {
        "spanId": span_id, "traceId": trace_id, "scope": SCOPE,
        "body": {"output": {"messages": out_msgs}, "input": {"messages": in_msgs}},
        "attributes": {"event.name": SCOPE["name"], "session.id": session_id},
        "resource": {"attributes": RESOURCE_ATTRS},
        "flags": 1, "severityNumber": 9, "severityText": "",
        "timeUnixNano": time_ns, "observedTimeUnixNano": time_ns,
    }


def _empty_user_msgs():
    return [{"role": "user", "content": {"content": json.dumps(
        [{"text": "<EMPTY_USER_INPUT>"}], ensure_ascii=False)}}]


def build_session_spans(trace_events, session_id, salt=""):
    """Convert one session's TRANSCRIPT_AI_AGENT_TRACE records into a
    sessionSpans list (OTEL spans + matching log events).

    `salt` makes the derived span/trace/session ids unique per run. Without it the
    ids are a pure function of the Connect UUIDs, so re-ingesting a session adds a
    second set of spans under the SAME ids to the shared runtime log group, and
    batch evaluation then scores this run's session over stale spans too - which
    makes converter fixes look like they had no effect.
    """
    session_id = eval_session_id(session_id, salt)
    raw_spans = []
    for e in trace_events:
        s = parse_span_field(e.get("span") or "{}")
        if s.get("span_id") and s.get("start_timestamp") and s.get("end_timestamp"):
            raw_spans.append(s)
    by_id = {s["span_id"]: s for s in raw_spans}

    def find_root(s):
        seen = set()
        while s.get("parent_span_id") and s["parent_span_id"] in by_id \
                and s["span_id"] not in seen:
            seen.add(s["span_id"])
            s = by_id[s["parent_span_id"]]
        return s

    def agent_name(s):
        # agent-assist agents only name the root span; children inherit it
        return (s.get("ai_agent_name") or find_root(s).get("ai_agent_name")
                or "connect-agent")

    # a span that never gets a log event cannot be evaluated: batch evaluation
    # raises LogEventMissingException for it, and it never appears in the runtime
    # log group's index, so the indexing gate waits for it forever. Connect emits
    # such spans for non-conversational internals (barge_in, for example). A leaf
    # one carries nothing evaluable and is dropped; one with children has to stay
    # to keep the parent chain intact, and gets a placeholder event instead.
    parents = {s.get("parent_span_id") for s in raw_spans if s.get("parent_span_id")}

    out = []
    for s in sorted(raw_spans, key=lambda x: int(x["start_timestamp"])):
        root = find_root(s)
        trace_id = otel_trace_id(root["span_id"], salt)
        span_id = otel_span_id(s["span_id"], salt)
        parent = s.get("parent_span_id")
        start_ns = int(s["start_timestamp"]) * 1_000_000
        end_ns = int(s["end_timestamp"]) * 1_000_000
        kind = span_kind(s)
        name = agent_name(s)

        attrs = {
            "session.id": session_id,
            "gen_ai.operation.name": kind,
            "gen_ai.system": "strands-agents",
            "gen_ai.provider.name": "strands-agents",
            "gen_ai.agent.name": name,
        }
        if s.get("ai_agent_type"):
            attrs["connect.ai_agent.type"] = s["ai_agent_type"]
        if s.get("error_type"):
            attrs["error.type"] = s["error_type"]

        in_msgs, out_msgs, tool_name = [], [], ""
        if kind == "inference":
            attrs["gen_ai.operation.name"] = "chat"
            attrs["aws.genai.span_kind"] = "LLM"
            attrs["gen_ai.request.model"] = s.get("request_model", "")
            for k_src, k_dst in [("usage_input_tokens", "gen_ai.usage.input_tokens"),
                                 ("usage_output_tokens", "gen_ai.usage.output_tokens"),
                                 ("usage_total_tokens", "gen_ai.usage.total_tokens")]:
                if s.get(k_src, "").isdigit():
                    attrs[k_dst] = int(s[k_src])
            in_msgs = connect_messages_to_otel(s.get("input_messages", ""), "input")
            out_msgs = connect_messages_to_otel(s.get("output_messages", ""), "output")
            # a model call the agent made on its own (a voice agent's greeting)
            # has no input messages; the evaluator's span mapping raises on an
            # empty array, so it gets the same explicit placeholder as elsewhere
            if out_msgs and not in_msgs:
                in_msgs = _empty_user_msgs()
        elif kind == "execute_tool":
            in_msgs, out_msgs, tool_name = tool_event_messages(s)
            attrs["gen_ai.tool.name"] = tool_name
            attrs["aws.genai.span_kind"] = "TOOL"
        elif kind == "invoke_agent":
            attrs["aws.genai.span_kind"] = "AGENT"
            attrs["gen_ai.request.model"] = s.get("request_model", "")
            in_msgs = connect_messages_to_otel(s.get("input_messages", ""), "input")
            out_msgs = connect_messages_to_otel(s.get("output_messages", ""), "output")
            sys_txt = system_prompt_text(s.get("system_instructions"))
            # self-service orchestrators log the turn on the child inference span
            # only; synthesize the turn-level query / answer from the last one
            if not in_msgs:
                children = [c for c in raw_spans
                            if c.get("parent_span_id") == s["span_id"]
                            and span_kind(c) == "inference"]
                if children:
                    c = children[-1]
                    in_msgs = connect_messages_to_otel(c.get("input_messages", ""), "input")
                    out_msgs = out_msgs or connect_messages_to_otel(
                        c.get("output_messages", ""), "output")
                    sys_txt = sys_txt or system_prompt_text(c.get("system_instructions"))
            if in_msgs:
                in_msgs = trim_to_user_query(in_msgs)
                if sys_txt:
                    in_msgs = [{"role": "system", "content": sys_txt[:4000]}] + in_msgs
        else:
            in_msgs = connect_messages_to_otel(s.get("input_messages", ""), "input")
            out_msgs = connect_messages_to_otel(s.get("output_messages", ""), "output")

        span_name = {"invoke_agent": "invoke_agent " + name,
                     "inference": "chat " + s.get("request_model", ""),
                     "execute_tool": "execute_tool " + tool_name}.get(kind, kind).strip()

        # an aborted call (barge-in, timeout) has ERROR status and no answer; the
        # evaluator rejects it as "error status with insufficient data", so it is
        # treated like any other span with nothing to evaluate
        if s.get("status") == "ERROR" and not out_msgs:
            in_msgs, out_msgs = [], []
        if not in_msgs and not out_msgs:
            if s["span_id"] not in parents:
                continue                        # leaf, nothing to evaluate
            in_msgs = _empty_user_msgs()        # keep the chain, matchable

        out.append(_span(span_id, trace_id, span_name, start_ns, end_ns, attrs,
                         status=s.get("status", ""),
                         status_desc=s.get("status_description", ""),
                         parent=otel_span_id(parent, salt)
                         if parent and parent in by_id else None))
        out.append(_event(span_id, trace_id, session_id, in_msgs, out_msgs,
                          out[-1]["endTimeUnixNano"]))
    return out


# ---------------------------------------------------------------- legacy format

def _ts_ms(r):
    try:
        return int(r.get("event_timestamp") or 0)
    except (TypeError, ValueError):
        return 0


def legacy_records(records):
    """The per-model-call records of a session, one per generation, in order.

    TRANSCRIPT_AGENTIC_MESSAGE and TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION
    describe the same call (same prompt, same completion, ids linked by
    session_message_id == generation_id), so only one of each pair is kept.
    Only ORCHESTRATION prompts are conversations; KB answer generation, email
    reformulation etc. are single-shot helper calls and are skipped.
    """
    seen, out = set(), []
    for r in sorted(records, key=_ts_ms):
        if r.get("event_type") not in LEGACY_EVENTS:
            continue
        if (r.get("prompt_type") or "ORCHESTRATION") != "ORCHESTRATION":
            continue
        gid = r.get("session_message_id") or r.get("generation_id") or id(r)
        if gid in seen:
            continue
        seen.add(gid)
        out.append(r)
    return out


def converse_messages_to_turns(msgs):
    """Bedrock Converse messages (as logged in a legacy prompt) -> logical turns
    in the same shape conversation_messages() produces."""
    turns = []
    for m in msgs or []:
        blocks, has_tool, is_tool_result = [], False, False
        for b in m.get("content") or []:
            if not isinstance(b, dict):
                continue
            if "text" in b:
                t = (b["text"] or "").strip()
                if t and t not in ("<message>",):   # prefill marker, not content
                    blocks.append({"text": t})
            elif "toolUse" in b:
                tu = b["toolUse"] or {}
                blocks.append({"toolUse": {"toolUseId": tu.get("toolUseId"),
                                           "name": tu.get("name"),
                                           "input": tu.get("input")}})
                has_tool = True
            elif "toolResult" in b:
                tr_ = b["toolResult"] or {}
                blocks.append({"toolResult": {"toolUseId": tr_.get("toolUseId"),
                                              "status": tr_.get("status", "success"),
                                              "content": tool_result_blocks(tr_)}})
                has_tool, is_tool_result = True, True
        if not blocks:
            continue
        role = "tool" if is_tool_result else ("user" if m.get("role") == "user" else "assistant")
        prev = turns[-1] if turns else None
        if prev and prev["role"] == role == "assistant" and not is_tool_result \
                and not prev["is_tool_result"]:
            prev["blocks"] += blocks
            prev["has_tool"] = prev["has_tool"] or has_tool
        else:
            turns.append({"role": role, "blocks": blocks, "has_tool": has_tool,
                          "is_tool_result": is_tool_result})
    return turns


def completion_blocks(raw):
    """A legacy completion -> (assistant content blocks, [toolUse dicts]).

    Shapes seen: {"textContent", ...}; {"toolUseList": [{toolName, toolUseId,
    toolInput}]}; Converse {"output": {"message": {...}}}; KB {"output": {"text"}}.
    """
    c = _loads(raw, raw)
    blocks, uses = [], []
    if isinstance(c, str):
        if c.strip():
            blocks.append({"text": c.strip()})
        return blocks, uses
    if not isinstance(c, dict):
        return blocks, uses
    txt = (c.get("textContent") or "").strip()
    if txt:
        blocks.append({"text": txt})
    for tu in c.get("toolUseList") or []:
        inp = tu.get("toolInput")
        inp = _loads(inp, inp) if isinstance(inp, str) else inp
        use = {"toolUseId": tu.get("toolUseId"), "name": tu.get("toolName"), "input": inp}
        blocks.append({"toolUse": use})
        uses.append(use)
    msg = (c.get("output") or {}).get("message") if isinstance(c.get("output"), dict) else None
    if isinstance(msg, dict):
        for b in msg.get("content") or []:
            if "text" in b and (b["text"] or "").strip():
                blocks.append({"text": b["text"].strip()})
            elif "toolUse" in b:
                blocks.append({"toolUse": b["toolUse"]})
                uses.append(b["toolUse"])
    elif isinstance(c.get("output"), dict) and (c["output"].get("text") or "").strip():
        blocks.append({"text": c["output"]["text"].strip()})
    return blocks, uses


def _user_key(turns):
    """Identity of the customer turn a prompt answers: how many customer
    utterances so far and the text of the last one."""
    users = [t for t in turns if t["role"] == "user"]
    last = " ".join(b.get("text", "") for b in users[-1]["blocks"]) if users else ""
    return (len(users), last)


def build_legacy_session_spans(records, session_id, salt=""):
    """Rebuild a span tree for a session that has no trace events.

    One invoke_agent per customer turn (consecutive model calls that answer the
    same last customer utterance), one chat span per model call, and one
    execute_tool span per tool call whose result shows up in a later prompt.
    """
    calls = legacy_records(records)
    if not calls:
        return []
    eval_sid = eval_session_id(session_id, salt)
    out = []

    parsed = []
    for r in calls:
        p = _loads(r.get("prompt"), {}) or {}
        turns = converse_messages_to_turns(p.get("messages") if isinstance(p, dict) else None)
        blocks, uses = completion_blocks(r.get("completion"))
        parsed.append({"r": r, "prompt": p if isinstance(p, dict) else {},
                       "turns": turns, "blocks": blocks, "uses": uses,
                       "ts": _ts_ms(r)})

    # index of tool results: toolUseId -> (content blocks, prompt timestamp)
    results = {}
    for pc in parsed:
        for t in pc["turns"]:
            for b in t["blocks"]:
                tr_ = b.get("toolResult")
                if tr_ and tr_.get("toolUseId") and tr_["toolUseId"] not in results:
                    results[tr_["toolUseId"]] = (tr_["content"], pc["ts"])

    # group model calls into customer turns
    groups, prev_key = [], None
    for pc in parsed:
        key = _user_key(pc["turns"])
        if groups and key == prev_key:
            groups[-1].append(pc)
        else:
            groups.append([pc])
        prev_key = key

    agent = "connect-agent"
    for g_idx, group in enumerate(groups):
        first, last = group[0], group[-1]
        r0 = first["r"]
        agent = ("ai-agent-" + str(r0.get("ai_agent_id"))[:8]) if r0.get("ai_agent_id") else agent
        model = first["prompt"].get("modelId") or r0.get("model_id") or ""
        turn_uid = f"{session_id}:turn:{g_idx}"
        trace_id = otel_trace_id(turn_uid, salt)
        agent_span_id = otel_span_id(turn_uid, salt)
        prev_ts = groups[g_idx - 1][-1]["ts"] if g_idx else None
        start_ms = prev_ts + 1 if prev_ts else first["ts"] - 1000
        end_ms = last["ts"]
        sys_txt = ""
        for blk in first["prompt"].get("system") or []:
            if isinstance(blk, dict) and blk.get("text"):
                sys_txt += blk["text"]

        # chat spans + tool spans
        call_start = start_ms
        chat_ids = []
        for pc in group:
            r = pc["r"]
            cid = otel_span_id(f"{session_id}:call:{r.get('session_message_id') or r.get('generation_id') or pc['ts']}", salt)
            chat_ids.append(cid)
            attrs = {"session.id": eval_sid, "gen_ai.operation.name": "chat",
                     "aws.genai.span_kind": "LLM", "gen_ai.system": "strands-agents",
                     "gen_ai.provider.name": "strands-agents",
                     "gen_ai.agent.name": agent, "gen_ai.request.model": model}
            in_turns = pc["turns"] or [_placeholder_turn("input")]
            in_msgs = turns_to_otel(in_turns, "input")
            out_turn = {"role": "assistant", "blocks": pc["blocks"] or [{"text": "<NO_RESPONSE>"}],
                        "has_tool": bool(pc["uses"]), "is_tool_result": False}
            out_msgs = turns_to_otel([out_turn], "output")
            out.append(_span(cid, trace_id, "chat " + model, call_start * 1_000_000,
                             pc["ts"] * 1_000_000, attrs, parent=agent_span_id))
            out.append(_event(cid, trace_id, eval_sid, in_msgs, out_msgs,
                              out[-1]["endTimeUnixNano"]))
            for use in pc["uses"]:
                tid = use.get("toolUseId") or ""
                res = results.get(tid)
                tsid = otel_span_id(f"{session_id}:tool:{tid or pc['ts']}", salt)
                t_end = res[1] if res else pc["ts"] + 1
                tattrs = {"session.id": eval_sid, "gen_ai.operation.name": "execute_tool",
                          "aws.genai.span_kind": "TOOL", "gen_ai.system": "strands-agents",
                          "gen_ai.provider.name": "strands-agents",
                          "gen_ai.agent.name": agent, "gen_ai.tool.name": use.get("name", "")}
                args = use.get("input")
                args = json.dumps(args, ensure_ascii=False) if not isinstance(args, str) else args
                t_in = [{"role": "tool", "content": {"content": args or "{}", "role": "tool", "id": tid}}]
                t_out = [{"role": "assistant", "content": {
                    "message": json.dumps(res[0] if res else [{"text": "OK"}], ensure_ascii=False),
                    "id": tid}}]
                out.append(_span(tsid, trace_id, "execute_tool " + (use.get("name") or ""),
                                 pc["ts"] * 1_000_000, t_end * 1_000_000, tattrs,
                                 parent=agent_span_id))
                out.append(_event(tsid, trace_id, eval_sid, t_in, t_out,
                                  out[-1]["endTimeUnixNano"]))
            call_start = pc["ts"] + 1

        # the turn-level agent span: user query = last prompt, answer = last text
        attrs = {"session.id": eval_sid, "gen_ai.operation.name": "invoke_agent",
                 "aws.genai.span_kind": "AGENT", "gen_ai.system": "strands-agents",
                 "gen_ai.provider.name": "strands-agents",
                 "gen_ai.agent.name": agent, "gen_ai.request.model": model}
        in_msgs = trim_to_user_query(turns_to_otel(last["turns"] or [_placeholder_turn("input")], "input"))
        if sys_txt:
            in_msgs = [{"role": "system", "content": sys_txt[:4000]}] + in_msgs
        final_blocks = [b for pc in group for b in pc["blocks"]]
        out_turn = {"role": "assistant",
                    "blocks": final_blocks or [{"text": "<NO_RESPONSE>"}],
                    "has_tool": any(pc["uses"] for pc in group), "is_tool_result": False}
        out_msgs = turns_to_otel([out_turn], "output")
        out.append(_span(agent_span_id, trace_id, "invoke_agent " + agent,
                         start_ms * 1_000_000, end_ms * 1_000_000, attrs))
        out.append(_event(agent_span_id, trace_id, eval_sid, in_msgs, out_msgs,
                          out[-1]["endTimeUnixNano"]))
    return out


# ---------------------------------------------------------------- per-record helpers

def system_prompt_of_record(r):
    """Full system prompt text carried by one log record, any format."""
    if r.get("event_type") == TRACE_EVENT:
        return system_prompt_text(parse_span_field(r.get("span") or "{}")
                                  .get("system_instructions"))
    if r.get("event_type") in LEGACY_EVENTS:
        p = _loads(r.get("prompt"), {})
        if isinstance(p, dict):
            return "\n".join(b.get("text", "") for b in p.get("system") or []
                             if isinstance(b, dict)).strip()
    return ""


def tool_descriptions_of_record(r):
    """{tool name: description} as the model saw them, any format.

    Trace-format agents embed <toolConfiguration> XML in the system prompt (see
    recommend.py); legacy prompts carry a Converse toolConfig with toolSpecs.
    """
    out = {}
    if r.get("event_type") in LEGACY_EVENTS:
        p = _loads(r.get("prompt"), {})
        if isinstance(p, dict):
            for t in ((p.get("toolConfig") or {}).get("tools") or []):
                spec = t.get("toolSpec") or {}
                if spec.get("name") and spec.get("description"):
                    out[spec["name"]] = spec["description"]
    txt = system_prompt_of_record(r)
    for block in re.findall(r"<toolConfiguration>(.*?)</toolConfiguration>", txt, re.S):
        name_m = re.search(r"<toolName>(.*?)</toolName>", block, re.S)
        if not name_m:
            continue
        instrs = re.findall(r"<instruction>\s*([^<]+?)\s*</instruction>", block, re.S)
        desc = " ".join(i.strip() for i in instrs if i.strip())
        if desc:
            out[name_m.group(1).strip()] = desc
    return out


def record_agent_name(r):
    """The AI agent a single log record belongs to, or None if it does not say.

    Trace records name the agent on the span (children of agent-assist agents do
    not; the session-level view comes from agent_names_of_spans). Legacy records
    only carry the agent id, so the same derived name build_legacy_session_spans
    uses is returned."""
    if r.get("event_type") == TRACE_EVENT:
        return parse_span_field(r.get("span") or "{}").get("ai_agent_name") or None
    if r.get("event_type") in LEGACY_EVENTS and r.get("ai_agent_id"):
        return "ai-agent-" + str(r["ai_agent_id"])[:8]
    return None


def agent_names_of_spans(spans):
    return sorted({x["attributes"].get("gen_ai.agent.name") for x in spans
                   if "name" in x and x["attributes"].get("gen_ai.agent.name")})


# ---------------------------------------------------------------- driver

def classify_sessions(records):
    """{session_id: "trace" | "legacy" | "unsupported"} for every session seen.

    "unsupported" sessions have log records but nothing this converter can turn
    into a conversation (e.g. only TRANSCRIPT_CREATE_SESSION, only the
    pre-orchestration SELF_SERVICE_MESSAGE format, or only single-shot helper
    calls such as knowledge-base answer generation)."""
    records = _rekey_null_session(list(records))
    kinds = defaultdict(set)
    for r in records:
        sid = r.get("session_id")
        if sid:
            kinds[sid].add(r.get("event_type"))
    conversational = set()
    for r in records:
        if r.get("event_type") in LEGACY_EVENTS and \
                (r.get("prompt_type") or "ORCHESTRATION") == "ORCHESTRATION":
            conversational.add(r.get("session_id"))
    out = {}
    for sid, ks in kinds.items():
        if TRACE_EVENT in ks:
            out[sid] = "trace"
        elif sid in conversational:
            out[sid] = "legacy"
        else:
            out[sid] = "unsupported"
    return out


NULL_SESSION = "00000000-0000-0000-0000-000000000000"


def _rekey_null_session(records):
    """Give every contact logged under Connect's all-zero session id its own id.

    Agent-assist queries (manual search, analytics) that are not part of a Q in
    Connect session are all logged with session_id 00000000-...-000000000000. Left
    as one "session" they span weeks of unrelated contacts, which both scores
    them as one conversation and breaks ingestion (PutLogEvents cannot take
    events more than 14 days apart). The span carries the contact_id, so the
    contact is used as the session instead; records without one keep the id.
    """
    null = [(i, parse_span_field(r.get("span") or "{}")) for i, r in enumerate(records)
            if r.get("session_id") == NULL_SESSION and r.get("event_type") == TRACE_EVENT]
    if not null:
        return records
    by_id = {s["span_id"]: s for _, s in null if s.get("span_id")}

    def root(s):
        seen = set()
        while s.get("parent_span_id") in by_id and s.get("span_id") not in seen:
            seen.add(s.get("span_id"))
            s = by_id[s["parent_span_id"]]
        return s

    out = list(records)
    for i, s in null:
        r = root(s)
        # contact when the span says which one; otherwise the request that
        # invoked the agent, which for one-shot agent-assist queries IS the unit
        key = r.get("contact_id") or r.get("request_id") or r.get("origin_request_id") \
            or r.get("span_id")
        if key:
            prefix = "contact-" if r.get("contact_id") else "request-"
            out[i] = dict(records[i], session_id=prefix + key)
    return out


rekey_records = _rekey_null_session


def convert(records, salt=""):
    """records: iterable of parsed Connect log dicts -> {session_id: sessionSpans}

    Pass a per-run `salt` (e.g. the runId) when the output will be ingested into a
    shared log group; see build_session_spans. Sessions with trace events use the
    trace path; sessions with only legacy model-call records are rebuilt from
    those; anything else is omitted (see classify_sessions for the reason).
    """
    records = _rekey_null_session(list(records))
    by_sid = defaultdict(list)
    for r in records:
        if r.get("session_id"):
            by_sid[r["session_id"]].append(r)
    out = {}
    for sid, fmt in classify_sessions(records).items():
        if fmt == "trace":
            spans = build_session_spans(
                [r for r in by_sid[sid] if r.get("event_type") == TRACE_EVENT], sid, salt)
        elif fmt == "legacy":
            spans = build_legacy_session_spans(by_sid[sid], sid, salt)
        else:
            continue
        if spans:
            out[sid] = spans
    return out


if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "connect_ai_agent_log.csv"
    sessions = convert(parse_csv(src))
    for sid, spans in sessions.items():
        fn = f"out/session_{sid}.json"
        with open(fn, "w") as f:
            json.dump(spans, f, ensure_ascii=False, indent=1)
        n_spans = sum(1 for x in spans if "name" in x)
        n_events = len(spans) - n_spans
        print(f"{sid}: {n_spans} spans + {n_events} events -> {fn}")
