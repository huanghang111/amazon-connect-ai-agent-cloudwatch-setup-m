"""Step 3: block until the ingested data is visible to Logs Insights.

Batch evaluation reads BOTH halves of the ingested data through the Logs Insights
index, and each half has its own lag. Starting a job before either one is
queryable fails every session in it, so both are gated here:

Both halves are indexed under the SESSION's own clock, not the ingest time: a
span document lands at the span's startTimeUnixNano, and a log event lands at
whatever timestamp PutLogEvents was given, which is that same session time
(ingest.py only shifts a session when it is close to the 14-day PutLogEvents
limit). Evaluating yesterday's calls therefore indexes yesterday, and a query
window that starts "now" finds neither half. ingest.py records the range as
`spanTimeRange`, and both queries use it.

  * span documents in "aws/spans" (written via the X-Ray OTLP endpoint). If these
    are not indexed yet, the job fails with
        ValidationException: Provided input contains N log event(s) but no span
        documents. Log events alone cannot be evaluated
  * log events in the runtime log group (written via PutLogEvents). If these are
    not indexed yet, the job fails per session with
        LogEventMissingException: Span with ID <id> ... is missing a
        corresponding log event
    The FIRST batch written to a freshly created stream can stay invisible for
    well over 15 minutes even though GetLogEvents returns it immediately
    (recordsScanned: 0.0). Re-writing the identical messages under fresh
    timestamps reliably makes them queryable in ~90s.

So the gate is: poll both indexes -> if the events are still missing when the
budget runs out, re-write them with current timestamps -> let Step Functions
retry -> only start evaluation once both sides are visible.

Failing loudly here is much cheaper than a job that reports 0 completed sessions.
The one thing to get right is that the gate must fail only when the data really
is missing: both presence tests read the raw message instead of a discovered
field (see PARSE_QUERY), because a query that can never match turns this gate
into a deadlock. REQUIRE_SPAN_INDEX=false skips the aws/spans half if a region
still refuses to cooperate.
"""

import json
import os
import time

import boto3

from logbatch import put_events

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["RESULTS_BUCKET"]
RUNTIME_NAME = os.environ["OBSERVABILITY_RUNTIME_NAME"]
LOG_GROUP = f"/aws/bedrock-agentcore/runtimes/{RUNTIME_NAME}-DEFAULT"
EVENT_STREAM = "otel-rt-logs"
SPANS_LOG_GROUP = "aws/spans"

POLL_SECONDS = int(os.environ.get("INDEX_POLL_SECONDS", "20"))
# one Lambda invocation stays well inside its own timeout; Step Functions retries
BUDGET_SECONDS = int(os.environ.get("INDEX_BUDGET_SECONDS", "180"))
# escape hatch for a region where the "aws/spans" side of the gate cannot be made
# to work at all: skip that half and let evaluation report the truth. Only the
# gate is skipped, nothing about the data changes.
REQUIRE_SPAN_INDEX = os.environ.get("REQUIRE_SPAN_INDEX", "true").lower() == "true"

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

# The gate must NOT depend on Logs Insights automatic JSON field discovery.
#
# A deployment in eu-central-1 (customer report, 2026-09) had the span documents
# physically present in "aws/spans" - `fields @message` returned the full span
# JSON and `filter @message like /spanId/` matched all 12 records - while
# `filter ispresent(spanId)` matched 0 records, run after run (recordsScanned 12,
# recordsMatched 0). Whatever the reason - spanId was simply not queryable as a
# field there - `missing_spans` stayed at 100%, all 5 attempts burned, and every
# run died in WaitIndexed with LogEventsNotIndexed while the data it was waiting
# for was already there. It is region- or account-specific, not universal: the
# same query works on aws/spans in us-east-1, so it cannot be relied on either
# way.
#
# `parse` reads the raw message text, so it is independent of discovery, and this
# pattern covers both serializations the gate has to read:
#   aws/spans, written by X-Ray (compact)  ->  "spanId":"3d58446444af52bb"
#   runtime log group, our own json.dumps  ->  "spanId": "3d58446444af52bb"
# Checked against real data in us-east-1, where discovery does work, and it
# returns exactly what the old query returned: 5/5 span documents in aws/spans,
# 213/213 log events in a runtime log group. spanIds are md5[:16] (converter.py),
# hence the fixed 16 hex digits.
PARSE_QUERY = ('parse @message /"spanId":\\s*"(?<sid>[0-9a-fA-F]{16})"/'
               " | filter ispresent(sid) | stats count(*) by sid | limit 10000")
# Safety net for the opposite failure - a log group where the raw text does not
# carry that shape but the field is discovered. Only run when parse finds nothing,
# so the normal path stays at one query per log group per poll.
DISCOVERED_QUERY = "fields spanId | filter ispresent(spanId) | limit 10000"


def query_span_ids(log_group, query, field, start_epoch, end_epoch):
    """Values of `field` returned by `query` against `log_group` over [start, end]."""
    qid = logs.start_query(
        logGroupName=log_group, startTime=start_epoch, endTime=end_epoch,
        queryString=query,
    )["queryId"]
    while True:
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(2)
    if r["status"] != "Complete":
        print(f"query on {log_group} ended {r['status']}: {query}")
        return set()
    return {f["value"] for row in r["results"] for f in row if f["field"] == field}


def indexed_span_ids(log_group, start_epoch, end_epoch):
    """spanIds visible to Logs Insights in `log_group` over [start, end]."""
    found = query_span_ids(log_group, PARSE_QUERY, "sid", start_epoch, end_epoch)
    if not found:
        found = query_span_ids(log_group, DISCOVERED_QUERY, "spanId",
                               start_epoch, end_epoch)
    return found


def rewrite_events(missing, start_epoch, end_epoch):
    """Re-emit only the still-unindexed events under current timestamps.

    This is the documented workaround for the stuck-first-batch case: identical
    message bodies, new timestamps, which reliably become queryable in ~90s.

    Only the spans in `missing` are re-emitted. The stream is shared by every run,
    and a rewrite is itself part of the stream, so re-emitting everything found
    would double the stream on each attempt (and re-send events that are already
    indexed). Deduplicating by spanId keeps a retry the same size as the gap.

    Reads with FilterLogEvents rather than GetLogEvents. GetLogEvents pages are
    documented to be "partially full, or even empty" without that meaning the end
    of the stream - a break on the first empty page silently found nothing to
    rewrite ("rewrote 0 events") on a stream large enough to produce one, which is
    exactly when the rewrite is needed. FilterLogEvents ends when it stops
    returning a nextToken, and takes the time window, so a shared stream that has
    accumulated many runs is not re-read from the beginning every attempt.
    """
    seen, events, token, scanned = set(), [], None, 0
    while True:
        kw = {"logGroupName": LOG_GROUP, "logStreamNames": [EVENT_STREAM],
              "startTime": start_epoch * 1000, "endTime": end_epoch * 1000}
        if token:
            kw["nextToken"] = token
        r = logs.filter_log_events(**kw)
        for e in r.get("events", []):
            scanned += 1
            msg = e["message"]
            try:
                span_id = json.loads(msg).get("spanId")
            except (json.JSONDecodeError, AttributeError):
                continue
            if span_id in missing and span_id not in seen:
                seen.add(span_id)
                events.append(msg)
        token = r.get("nextToken")
        if not token:
            break
    if not events:
        print(f"nothing to rewrite: scanned {scanned} event(s) in {EVENT_STREAM} "
              f"over [{start_epoch}, {end_epoch}], none of them among the "
              f"{len(missing)} missing spanIds")
        return 0
    # event bodies carry whole transcripts, so a gap of any size can exceed the
    # 1 MB PutLogEvents limit; logbatch splits it into legal calls
    base = int(time.time() * 1000)
    sent, oversized = put_events(
        logs, LOG_GROUP, EVENT_STREAM,
        [{"timestamp": base + i, "message": m} for i, m in enumerate(events)])
    if oversized:
        print(f"WARNING: {len(oversized)} event(s) exceed the 256 KB single-event "
              "limit and could not be rewritten")
    return sent


def handler(event, context):
    run_id = event["runId"]
    ingest = json.loads(s3.get_object(
        Bucket=BUCKET, Key=f"runs/{run_id}/ingest.json")["Body"].read())
    wanted = set(ingest.get("spanIds", []))
    attempt = int(event.get("indexAttempt", 0))

    if not wanted:
        return {"runId": run_id, "indexed": True, "missing": 0,
                "note": "no spans ingested"}

    # Two different clocks, so two different query windows (see docstring):
    #   * log events were written by PutLogEvents just now, so they sit at
    #     ingest time
    #   * span documents are indexed by X-Ray under each span's OWN start time,
    #     which for replayed or unshifted sessions is hours or days in the past.
    #     Querying "the last 2 hours" then finds none of them and the gate fails
    #     with every span missing even though all of them arrived.
    rng = ingest.get("spanTimeRange") or {}
    ingested_at = rng.get("ingestedAtEpoch") or int(time.time())
    span_start = min(rng.get("startEpoch") or ingested_at, ingested_at) - 600
    # The log events carry the SESSION's timestamps, not the ingest time:
    # PutLogEvents keeps whatever timestamp it is given, and ingest.py only shifts
    # a session when it is close to the 14-day limit. A session evaluated two days
    # after the call therefore indexes two days in the past, so a window that
    # starts at ingest time cannot see it, and the gate reported every log event
    # missing on every run - masked until now because rewriting them under current
    # timestamps DID land inside that window. Same clock as the span side.
    event_start = span_start

    deadline = time.time() + BUDGET_SECONDS
    missing_events = wanted
    missing_spans = wanted if REQUIRE_SPAN_INDEX else set()
    while time.time() < deadline:
        now = int(time.time())
        # both sides must be queryable; see module docstring for each failure mode
        missing_events = wanted - indexed_span_ids(LOG_GROUP, event_start, now + 600)
        missing_spans = set()
        if REQUIRE_SPAN_INDEX:
            missing_spans = wanted - indexed_span_ids(
                SPANS_LOG_GROUP, span_start, now + 600)
        if not missing_events and not missing_spans:
            print(f"all {len(wanted)} spans indexed in both {SPANS_LOG_GROUP} "
                  f"and {LOG_GROUP}")
            return {"runId": run_id, "indexed": True, "missing": 0,
                    "indexAttempt": attempt}
        span_state = (f"{len(missing_spans)}/{len(wanted)} span documents"
                      if REQUIRE_SPAN_INDEX else "span gate disabled")
        print(f"waiting to index: {span_state}, "
              f"{len(missing_events)}/{len(wanted)} log events")
        time.sleep(POLL_SECONDS)

    # Only the PutLogEvents side can be nudged by rewriting; span documents are
    # owned by X-Ray, so for those the only option is to wait and retry.
    rewritten = (rewrite_events(missing_events, event_start, int(time.time()) + 600)
                 if missing_events else 0)
    print(f"index budget exhausted on attempt {attempt}; "
          f"{len(missing_spans)} span documents and {len(missing_events)} log "
          f"events still missing; rewrote {rewritten} events")
    # Which ones, not just how many: with the ids in hand the same three queries
    # that diagnose-index-gate.sh runs can be pointed at a span that is actually
    # missing. Sampling an id out of ingest.json hits a span that already landed
    # and proves nothing. Also print the window queried, because "absent" and
    # "outside the window" look identical from the counts alone.
    if missing_spans:
        print(f"missing spanIds (span documents, up to 10): "
              f"{sorted(missing_spans)[:10]} queried {SPANS_LOG_GROUP} over "
              f"[{span_start}, {int(time.time()) + 600}]")
    if missing_events:
        print(f"missing spanIds (log events, up to 10): "
              f"{sorted(missing_events)[:10]} queried {LOG_GROUP} over "
              f"[{event_start}, {int(time.time()) + 600}]")
    # a span the OTLP endpoint never kept can never be indexed, so waiting for it
    # is hopeless - say so here rather than let five attempts look like slowness
    if missing_spans and ingest.get("rejectedSpans"):
        print(f"NOTE: ingest reported {ingest['rejectedSpans']} span(s) rejected by "
              f"the X-Ray OTLP endpoint {ingest.get('rejectedSpanErrors')} - those "
              "will never appear in aws/spans, so this gate cannot pass for them")
    return {"runId": run_id, "indexed": False,
            "missing": len(missing_events | missing_spans),
            "missingSpanDocuments": len(missing_spans),
            "missingLogEvents": len(missing_events),
            "indexAttempt": attempt + 1, "rewrittenEvents": rewritten}
