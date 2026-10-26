"""Step 1: collect finished Connect AI Agent sessions from CloudWatch Logs.

Reads the Connect delivery log group (default /aws/connect/ai-agent-logs) over a
lookback window, converts each session with converter.py, and hands the ones that
are ready for evaluation to the next step.

Three correctness rules make a daily 24h schedule safe to run unattended:

  1. OVERLAP  - the window is (hours + OVERLAP_MINUTES) wide, so a session that
                straddles midnight, or whose last events were still being
                delivered when the previous run's window closed, is picked up by
                the next run instead of being silently truncated or lost.
  2. SETTLED  - a session is only evaluated once its newest event is older than
                SETTLE_MINUTES. An in-flight call would otherwise be scored on
                half a transcript, which produces a wrong score, not a missing one.
  3. LEDGER   - every evaluated sessionId is recorded in DynamoDB with a TTL.
                Overlap and manual re-runs therefore cost nothing: already-done
                sessions are skipped. Set FORCE=true to re-evaluate anyway.

Two guards keep a busy account from turning one run into a surprise bill:

  * Logs Insights returns at most 10,000 rows per query. The window is split in
    half, recursively, whenever a query fills up, so a chatty day is read
    completely instead of silently truncated to its newest 10,000 events.
  * MAX_SESSIONS_PER_RUN (0 = unlimited) caps how many sessions one run
    evaluates. Evaluation is one LLM call per evaluator per trace, so this is the
    knob that bounds cost. Newest sessions win; the rest are reported under
    skipped.deferred and, since they are not written to the ledger, are picked up
    by the next run as long as it still covers them.

Validation input: an `archive` of `s3://bucket/prefix` in the event reads the
same Connect records from NDJSON files ({"timestamp", "message"} per line, as
written by the daily collector of the parent project) instead of the log group.
It exists to replay historical data through the pipeline; the default, and the
scheduled path, is the log group.

Output (to S3, so later steps and the customer both read the same artifacts):
  runs/<runId>/sessions/<sessionId>.json          converted sessionSpans
  runs/<runId>/agents/<agent>/system_prompt.txt   full system prompt per AI agent
  runs/<runId>/agents/<agent>/tool_descriptions.json  {tool: description} per agent
  runs/<runId>/collect.json                       manifest consumed by the next step

One Connect log group usually carries several AI agents (a voice agent, a chat
agent, agent-assist search...), each with its own system prompt and tools, so
the prompt/tool artifacts for the Recommendation step are captured per agent.
"""

import json
import os
import re
import time

import boto3

from converter import (convert, classify_sessions, eval_session_id,
                       system_prompt_of_record, tool_descriptions_of_record,
                       agent_names_of_spans, record_agent_name, rekey_records,
                       _fix_newlines, _DECODER)

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["RESULTS_BUCKET"]
LEDGER_TABLE = os.environ["LEDGER_TABLE"]
CONNECT_LOG_GROUP = os.environ.get("CONNECT_LOG_GROUP", "/aws/connect/ai-agent-logs")

# window widening / readiness thresholds (see module docstring)
OVERLAP_MINUTES = int(os.environ.get("OVERLAP_MINUTES", "30"))
SETTLE_MINUTES = int(os.environ.get("SETTLE_MINUTES", "10"))
# put_log_events rejects events older than 14 days. Raising this is only safe
# because ingest.py shifts such sessions forward into the window (SHIFT_TIMESTAMPS);
# with shifting off, anything over 14 days can never be ingested. Useful for
# backfilling or for validating a deployment against archived logs.
MAX_AGE_DAYS = int(os.environ.get("MAX_AGE_DAYS", "14"))
LEDGER_TTL_DAYS = int(os.environ.get("LEDGER_TTL_DAYS", "30"))
QUERY_LIMIT = int(os.environ.get("QUERY_LIMIT", "10000"))
# never split a window below this; a 5-minute slice that still fills 10,000 rows
# is not a Connect log group any more
MIN_SLICE_SECONDS = int(os.environ.get("MIN_SLICE_SECONDS", "300"))
MAX_SESSIONS_PER_RUN = int(os.environ.get("MAX_SESSIONS_PER_RUN", "0") or 0)

logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)

# trace events for current agents, model-call records for sessions logged before
# Connect emitted traces (converter.py picks the format per session)
EVENT_TYPES = ["TRANSCRIPT_AI_AGENT_TRACE", "TRANSCRIPT_CREATE_SESSION",
               "TRANSCRIPT_AGENTIC_MESSAGE",
               "TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION"]
QUERY = """fields @message
| filter event_type in [{types}]
| sort @timestamp asc
| limit {limit}"""


def _decode(msg):
    try:
        return _DECODER.decode(_fix_newlines(msg))
    except json.JSONDecodeError:
        return None  # a truncated record must not abort the whole run


def _query_once(start, end):
    qid = logs.start_query(
        logGroupName=CONNECT_LOG_GROUP, startTime=start, endTime=end,
        queryString=QUERY.format(
            types=", ".join(f'"{t}"' for t in EVENT_TYPES), limit=QUERY_LIMIT),
    )["queryId"]
    while True:
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(2)
    if r["status"] != "Complete":
        raise RuntimeError(f"Logs Insights query {r['status']} for {CONNECT_LOG_GROUP}")
    msgs = [f["value"] for row in r["results"] for f in row if f["field"] == "@message"]
    return msgs, r.get("statistics", {})


def run_query(start, end, stats=None, depth=0):
    """Pull raw Connect AI Agent log messages via Logs Insights, splitting the
    window until no slice hits the row limit. -> (records, stats)"""
    stats = stats if stats is not None else {"recordsScanned": 0.0, "queries": 0,
                                             "slicesAtLimit": 0}
    msgs, st = _query_once(start, end)
    stats["queries"] += 1
    stats["recordsScanned"] += float(st.get("recordsScanned") or 0)
    if len(msgs) >= QUERY_LIMIT and end - start > MIN_SLICE_SECONDS:
        mid = (start + end) // 2
        left, _ = run_query(start, mid, stats, depth + 1)
        right, _ = run_query(mid, end, stats, depth + 1)
        return left + right, stats
    if len(msgs) >= QUERY_LIMIT:
        stats["slicesAtLimit"] += 1
    out = [r for r in (_decode(m) for m in msgs) if r]
    return out, stats


def read_archive(uri, start_epoch, end_epoch):
    """Connect records from NDJSON archive objects under s3://bucket/prefix.

    Every object under the prefix whose key ends in .ndjson is read; each line is
    {"timestamp": <ms>, "message": <Connect record JSON>} (the daily collector's
    format). Records outside [start, end] are dropped so the window still means
    the same thing as for the log group."""
    assert uri.startswith("s3://"), uri
    bucket, _, prefix = uri[5:].partition("/")
    out, objects = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".ndjson"):
                continue
            objects += 1
            body = s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
            for line in body.decode("utf-8", errors="replace").splitlines():
                outer = _decode(line)
                if not outer:
                    continue
                ts = int(outer.get("timestamp") or 0) // 1000
                if ts and not (start_epoch <= ts <= end_epoch):
                    continue
                rec = _decode(outer.get("message") or "")
                if rec:
                    out.append(rec)
    return out, {"archiveObjects": objects, "archiveRecords": len(out)}


def already_evaluated(session_id):
    r = ddb.get_item(TableName=LEDGER_TABLE,
                     Key={"sessionId": {"S": session_id}},
                     ConsistentRead=True)
    return "Item" in r


def session_end_ns(spans):
    return max((s.get("endTimeUnixNano") or s.get("timeUnixNano") or 0)
               for s in spans)


def longest_system_prompt(records):
    """The full system prompt, taken from the raw records.

    It cannot be read back from the converted spans: converter.py truncates the
    system message to 4000 chars for the event body, which cuts off the
    <tools><toolConfigurationList> section that tool-description recommendation
    needs. So it is captured here, at full length, and stored alongside the run.
    """
    best = ""
    for r in records:
        txt = system_prompt_of_record(r)
        if len(txt) > len(best):
            best = txt
    return best


def tool_descriptions(records):
    """{tool: description} across all records; later records win on conflicts."""
    out = {}
    for r in records:
        out.update(tool_descriptions_of_record(r))
    return out


def handler(event, context):
    # the scheduler sends no runId; derive a sortable one so every run has a prefix
    run_id = event.get("runId") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    hours = int(event.get("hours") or os.environ.get("LOOKBACK_HOURS", "24"))
    force = str(event.get("force", os.environ.get("FORCE", "false"))).lower() == "true"
    max_sessions = int(event.get("maxSessions") or MAX_SESSIONS_PER_RUN or 0)
    archive = event.get("archive") or ""

    now = int(time.time())
    # rule 1: widen the window backwards so nothing falls between two runs
    start = now - hours * 3600 - OVERLAP_MINUTES * 60
    if archive:
        records, stats = read_archive(archive, start, now)
    else:
        records, stats = run_query(start, now)
    # contacts logged under Connect's all-zero session id become one session
    # per contact/request here, so the manifest, the per-agent artifacts and the
    # converted spans all use the same ids (see converter.rekey_records)
    records = rekey_records(records)
    formats = classify_sessions(records)
    # salt the derived span ids with the runId: the runtime log group is shared
    # across runs, and a repeated spanId lets batch evaluation read a previous
    # run's event instead of this one's (see converter.build_session_spans)
    sessions = convert(records, salt=run_id)

    settle_ns = (now - SETTLE_MINUTES * 60) * 1_000_000_000
    oldest_ns = (now - MAX_AGE_DAYS * 86400) * 1_000_000_000

    candidates = []
    skipped = {"unsettled": [], "already_evaluated": [], "too_old": [],
               "deferred": [],
               "unsupported": sorted(s for s, f in formats.items() if f == "unsupported")}
    for sid, spans in sessions.items():
        end_ns = session_end_ns(spans)
        if end_ns > settle_ns:                        # rule 2: still in flight
            skipped["unsettled"].append(sid)
            continue
        if end_ns < oldest_ns:                        # cannot be ingested at all
            skipped["too_old"].append(sid)
            continue
        if not force and already_evaluated(sid):      # rule 3: seen before
            skipped["already_evaluated"].append(sid)
            continue
        candidates.append((end_ns, sid, spans))

    # cost cap: newest first, the rest wait for the next run
    candidates.sort(key=lambda c: -c[0])
    if max_sessions and len(candidates) > max_sessions:
        skipped["deferred"] = [sid for _, sid, _ in candidates[max_sessions:]]
        candidates = candidates[:max_sessions]

    selected = []
    for end_ns, sid, spans in candidates:
        key = f"runs/{run_id}/sessions/{sid}.json"
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps(spans, ensure_ascii=False).encode(),
                      ContentType="application/json")
        selected.append({"sessionId": sid,
                         # what the spans actually carry, and therefore what the
                         # evaluation job must filter on
                         "evalSessionId": eval_session_id(sid, run_id),
                         "key": key, "items": len(spans),
                         "spans": sum(1 for x in spans if "name" in x),
                         "format": formats.get(sid),
                         "agents": agent_names_of_spans(spans),
                         "endTimeUnixNano": end_ns})

    # captured at full length for the Recommendation step, per AI agent and from
    # the selected sessions only, so each prompt matches the traces it is judged on
    selected_ids = {s["sessionId"] for s in selected}
    # modern sessions log BOTH trace events and the older per-model-call records;
    # only the records of the format the session was converted from count,
    # otherwise every trace-format agent shows up a second time under the
    # id-derived name the legacy records carry
    fmt_of = {s["sessionId"]: s["format"] for s in selected}
    by_agent = {}
    for r in records:
        fmt = fmt_of.get(r.get("session_id"))
        if not fmt:
            continue
        if (fmt == "trace") != (r.get("event_type") == "TRANSCRIPT_AI_AGENT_TRACE"):
            continue
        name = record_agent_name(r)
        if name:
            by_agent.setdefault(name, []).append(r)
    agents = {}
    for name, recs in sorted(by_agent.items()):
        slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:80]
        info = {"sessions": sorted({r["session_id"] for r in recs}),
                "systemPromptKey": None, "toolDescriptionsKey": None}
        prompt = longest_system_prompt(recs)
        if prompt:
            info["systemPromptKey"] = f"runs/{run_id}/agents/{slug}/system_prompt.txt"
            info["systemPromptChars"] = len(prompt)
            s3.put_object(Bucket=BUCKET, Key=info["systemPromptKey"],
                          Body=prompt.encode(), ContentType="text/plain")
        tools = tool_descriptions(recs)
        if tools:
            info["toolDescriptionsKey"] = f"runs/{run_id}/agents/{slug}/tool_descriptions.json"
            s3.put_object(Bucket=BUCKET, Key=info["toolDescriptionsKey"],
                          Body=json.dumps(tools, ensure_ascii=False, indent=1).encode(),
                          ContentType="application/json")
        agents[name] = info

    manifest = {
        "runId": run_id,
        "region": REGION,
        "source": archive or CONNECT_LOG_GROUP,
        "connectLogGroup": CONNECT_LOG_GROUP,
        "agents": agents,
        "window": {"startEpoch": start, "endEpoch": now, "hours": hours,
                   "overlapMinutes": OVERLAP_MINUTES},
        "query": stats,
        "recordsRead": len(records),
        "sessionsFound": len(formats),
        "sessionFormats": {f: sum(1 for x in formats.values() if x == f)
                           for f in ("trace", "legacy", "unsupported")},
        "sessionsSelected": len(selected),
        "maxSessionsPerRun": max_sessions,
        "sessions": selected,
        "skipped": {k: {"count": len(v), "sessionIds": v} for k, v in skipped.items()},
        "queryLimitHit": bool(stats.get("slicesAtLimit")),
    }
    s3.put_object(Bucket=BUCKET, Key=f"runs/{run_id}/collect.json",
                  Body=json.dumps(manifest, ensure_ascii=False, indent=1).encode(),
                  ContentType="application/json")

    # never let a truncated query masquerade as "nothing more to evaluate"
    if manifest["queryLimitHit"]:
        print(f"WARNING: a {MIN_SLICE_SECONDS}s slice still hit QUERY_LIMIT="
              f"{QUERY_LIMIT}; some events in the window were not read.")
    print(json.dumps({k: v for k, v in manifest.items() if k != "sessions"}))

    return {"runId": run_id,
            "sessionCount": len(selected),
            "sessionIds": [s["sessionId"] for s in selected],
            "manifestKey": f"runs/{run_id}/collect.json",
            "ledgerTtlDays": LEDGER_TTL_DAYS}
