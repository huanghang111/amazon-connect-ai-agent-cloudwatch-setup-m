"""Chunked PutLogEvents, because the API limits are easy to blow past silently.

A single Connect session converts to log events whose bodies carry the whole
transcript, so a busy day can produce one event of tens of KB and a stream of
hundreds. PutLogEvents then rejects the whole call with

    InvalidParameterException: Upload too large: 1049044 bytes exceeds limit
    of 1048576

which fails the entire pipeline run. The limits enforced here are the documented
CloudWatch Logs ones:

  * 1 MB per PutLogEvents call, counting 26 bytes of overhead per event
  * 10,000 events per call
  * 256 KB for a single event (such an event cannot be sent at all, so it is
    reported back to the caller rather than silently dropped)
"""

MAX_BATCH_BYTES = 1_000_000       # under the 1,048,576 limit, leaving headroom
MAX_BATCH_EVENTS = 10_000
EVENT_OVERHEAD_BYTES = 26         # per-event overhead counted against the limit
MAX_EVENT_BYTES = 256 * 1024


def event_size(event):
    return len(event["message"].encode("utf-8")) + EVENT_OVERHEAD_BYTES


def chunk_events(events):
    """-> (batches, oversized): events split into API-legal PutLogEvents calls.

    `oversized` holds events that exceed the 256 KB single-event limit; they are
    returned instead of dropped so the caller can fail loudly about them.
    """
    batches, oversized = [], []
    current, current_bytes = [], 0
    for e in events:
        size = event_size(e)
        if size > MAX_EVENT_BYTES:
            oversized.append(e)
            continue
        if current and (current_bytes + size > MAX_BATCH_BYTES
                        or len(current) >= MAX_BATCH_EVENTS):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(e)
        current_bytes += size
    if current:
        batches.append(current)
    return batches, oversized


def put_events(logs, log_group, log_stream, events):
    """PutLogEvents in API-legal chunks. -> (sent, oversized)

    Events must be sorted by timestamp within a call; that is done here so no
    caller can forget it.
    """
    batches, oversized = chunk_events(sorted(events, key=lambda e: e["timestamp"]))
    sent = 0
    for batch in batches:
        resp = logs.put_log_events(logGroupName=log_group, logStreamName=log_stream,
                                   logEvents=batch)
        rejected = resp.get("rejectedLogEventsInfo")
        if rejected:
            raise RuntimeError(f"PutLogEvents rejected events: {rejected}")
        sent += len(batch)
    return sent, oversized
