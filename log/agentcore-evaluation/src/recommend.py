"""Step 6: AgentCore Recommendation - optimized system prompt + tool descriptions.

Runs PER AI AGENT. One Connect log group carries every AI agent of the assistant
(a voice agent, a chat agent, agent-assist search, ...), each with its own system
prompt and tools; a recommendation is only meaningful against the traces of the
agent whose prompt it optimizes. The collect step captured the prompt and the
tool descriptions of every agent seen in the selected sessions, and each session
records which agents it ran through, so this step starts one system-prompt job
and one tool-description job per agent over that agent's own spans.

Uses the inline `sessionSpans` form of `agentTraces` (verified end to end), not
the batchEvaluationArn form.

API constraints verified 2026-07-15 against the live service:
  * SYSTEM_PROMPT_RECOMMENDATION requires evaluationConfig with EXACTLY 1
    evaluator (builtin ARN form arn:aws:bedrock-agentcore:::evaluator/Builtin.X).
  * TOOL_DESCRIPTION_RECOMMENDATION takes no evaluationConfig, and FAILS if any
    listed tool never appears as a toolUse in the sampled traces - so tools that
    were never invoked are dropped rather than sent.
  * sessionSpans is capped at 1000 items, so whole sessions are kept, newest
    first, until the cap is reached.
  * A system prompt can be rejected by the service's prompt-attack protection
    ("The provided content was detected as unsafe"). That is reported per agent
    and does not affect the other agents or the rest of the run.
"""

import hashlib
import json
import os
import re
import time

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["RESULTS_BUCKET"]
EVALUATOR_ARN = "arn:aws:bedrock-agentcore:::evaluator/Builtin.{}"
GUIDING_EVALUATOR = os.environ.get("RECOMMENDATION_EVALUATOR", "Helpfulness")
MAX_SPANS = 1000
POLL_SECONDS = int(os.environ.get("REC_POLL_SECONDS", "20"))
BUDGET_SECONDS = int(os.environ.get("REC_BUDGET_SECONDS", "780"))

agentcore = boto3.client("bedrock-agentcore", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# ------------------------------------------------- extract current artifacts

def load_text(key):
    if not key:
        return None
    try:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
    except s3.exceptions.NoSuchKey:
        return None


def load_json(key, default):
    txt = load_text(key)
    return json.loads(txt) if txt else default


def tools_invoked_in(spans):
    """Tool names that actually have execute_tool spans in the traces."""
    return {s["attributes"].get("gen_ai.tool.name")
            for s in spans if "name" in s
            and s["attributes"].get("aws.genai.span_kind") == "TOOL"}


def agent_spans(spans, agent):
    """The spans (and their events) of one agent out of a session's sessionSpans.

    A session can run through several agents (a transfer, or agent-assist
    lookups during a self-service call); only the target agent's spans should
    shape its recommendation."""
    ids = {s["spanId"] for s in spans if "name" in s
           and s["attributes"].get("gen_ai.agent.name") == agent}
    return [s for s in spans if s["spanId"] in ids]


def trim_to_limit(sessions):
    """Keep whole sessions, newest first, within the sessionSpans cap."""
    def newest(kv):
        return -max((s.get("endTimeUnixNano") or s.get("timeUnixNano") or 0)
                    for s in kv[1])

    kept, total, dropped = [], 0, []
    for sid, spans in sorted(sessions.items(), key=newest):
        if total + len(spans) > MAX_SPANS:
            dropped.append(sid)
            continue
        kept.extend(spans)
        total += len(spans)
    return kept, dropped


def job_name(prefix, run_id, agent):
    """[a-zA-Z0-9-_]{1,48}, unique per (run, agent).

    Names are account-unique and the API rejects a repeat with ConflictException.
    Agent names are long and often share a prefix (SelfServiceOrchestrator_...),
    so a plain truncation collided; a short hash of the full agent name keeps the
    truncated slug unique."""
    digest = hashlib.md5(agent.encode()).hexdigest()[:6]
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", agent).strip("-")
    # the clock makes a retry of the same run (a Step Functions retry, or a
    # manual re-invoke of this step) a new name instead of a conflict
    head = f"{prefix}-{run_id[-14:].strip('-')}-{digest}-{int(time.time()) % 10_000_000}-"
    return (head + slug)[:48].rstrip("-")


# ------------------------------------------------------------- job plumbing

def wait_jobs(job_ids):
    """Poll every job to a final state, tolerating transient API errors.

    GetRecommendation has returned InternalServerException for minutes at a time
    while the jobs themselves kept running; letting that exception escape lost the
    results of the jobs that had already completed. So a failed poll is logged
    and retried on the next round, and only the overall budget ends the wait."""
    pending, done = dict(job_ids), {}
    deadline = time.time() + BUDGET_SECONDS
    last_error = {}
    while pending and time.time() < deadline:
        for jid in list(pending):
            try:
                r = agentcore.get_recommendation(recommendationId=jid)
            except Exception as e:  # transient service errors: keep polling
                last_error[jid] = f"{type(e).__name__}: {e}"
                print(f"{pending[jid]}: poll failed, will retry: {last_error[jid][:200]}")
                continue
            if r["status"] in ("COMPLETED", "FAILED"):
                r.pop("ResponseMetadata", None)
                print(f"{pending.pop(jid)}: {r['status']}")
                done[jid] = r
        if pending:
            time.sleep(POLL_SECONDS)
    for jid, label in pending.items():
        print(f"{label}: TIMEOUT (job {jid} not final after {BUDGET_SECONDS}s)")
        done[jid] = {"recommendationId": jid, "status": "TIMEOUT", "type": label,
                     "lastPollError": last_error.get(jid)}
    return done


def markdown_report(resp, agent):
    lines = [f"# {resp.get('type')} - {resp['status']}",
             f"- agent: `{agent}`",
             f"- job: `{resp.get('recommendationId')}`",
             f"- created: {resp.get('createdAt')}", ""]
    res = resp.get("recommendationResult", {})
    sp = res.get("systemPromptRecommendationResult")
    td = res.get("toolDescriptionRecommendationResult")
    if sp:
        if sp.get("errorMessage"):
            lines.append(f"**ERROR** {sp.get('errorCode')}: {sp['errorMessage']}")
        else:
            lines += ["## Recommended system prompt", "```",
                      sp.get("recommendedSystemPrompt", ""), "```", "",
                      "## Why", sp.get("explanation", "")]
    if td:
        if td.get("errorMessage"):
            lines.append(f"**ERROR** {td.get('errorCode')}: {td['errorMessage']}")
        for t in td.get("tools", []):
            lines += [f"## Tool `{t['toolName']}`", "",
                      "**Recommended description**", "",
                      t.get("recommendedToolDescription", ""), "",
                      "**Why**", "", t.get("explanation", ""), ""]
    return "\n".join(lines)


def handler(event, context):
    run_id = event["runId"]
    manifest = json.loads(s3.get_object(
        Bucket=BUCKET, Key=f"runs/{run_id}/collect.json")["Body"].read())
    sessions = {}
    for entry in manifest["sessions"]:
        sessions[entry["sessionId"]] = json.loads(s3.get_object(
            Bucket=BUCKET, Key=entry["key"])["Body"].read())
    if not sessions:
        return {"runId": run_id, "jobs": [], "skipped": "no sessions"}

    agents = manifest.get("agents") or {}
    if not agents:
        # older collect.json layout: one prompt for the whole run
        agents = {"all": {"sessions": list(sessions),
                          "systemPromptKey": manifest.get("systemPromptKey"),
                          "toolDescriptionsKey": manifest.get("toolDescriptionsKey")}}

    jobs, notes, dropped_all = {}, [], {}
    for agent, info in sorted(agents.items()):
        own = {sid: (agent_spans(sessions[sid], agent) if agent != "all" else sessions[sid])
               for sid in info.get("sessions", []) if sid in sessions}
        own = {sid: sp for sid, sp in own.items() if sp}
        if not own:
            notes.append(f"{agent}: no spans in the selected sessions, skipped")
            continue
        spans, dropped = trim_to_limit(own)
        if dropped:
            dropped_all[agent] = dropped
            print(f"{agent}: dropped {len(dropped)} session(s) to stay under the "
                  f"{MAX_SPANS}-span cap")
        system_prompt = load_text(info.get("systemPromptKey"))
        tool_descs = load_json(info.get("toolDescriptionsKey"), {})
        invoked = tools_invoked_in(spans)

        if system_prompt:
            r = agentcore.start_recommendation(
                name=job_name("sp", run_id, agent), type="SYSTEM_PROMPT_RECOMMENDATION",
                recommendationConfig={"systemPromptRecommendationConfig": {
                    "systemPrompt": {"text": system_prompt[:20000]},
                    "agentTraces": {"sessionSpans": spans},
                    "evaluationConfig": {"evaluators": [
                        {"evaluatorArn": EVALUATOR_ARN.format(GUIDING_EVALUATOR)}]}}})
            jobs[r["recommendationId"]] = (agent, "system_prompt")
            print(f"{agent}: started system-prompt job {r['recommendationId']}")
        else:
            notes.append(f"{agent}: system-prompt skipped, no system prompt in its logs")

        usable = {n: d for n, d in tool_descs.items() if n in invoked}
        never_invoked = sorted(set(tool_descs) - set(usable))
        if never_invoked:
            notes.append(f"{agent}: tools omitted (never invoked in these traces): "
                         f"{never_invoked}")
        # Connect only embeds <instruction> for tools configured in the AI agent
        # itself; MCP tools arrive as a bare <toolName>, so there is no description
        # to optimize even though the model calls them. Say so, or the skip note
        # below reads as though the tools were never used.
        undescribed = sorted(invoked - set(tool_descs) - {None, ""})
        if undescribed:
            notes.append(f"{agent}: tools invoked but carrying no description "
                         f"(nothing to optimize): {undescribed}")
        if usable:
            r = agentcore.start_recommendation(
                name=job_name("td", run_id, agent), type="TOOL_DESCRIPTION_RECOMMENDATION",
                recommendationConfig={"toolDescriptionRecommendationConfig": {
                    "toolDescription": {"toolDescriptionText": {"tools": [
                        {"toolName": n, "toolDescription": {"text": d[:20000]}}
                        for n, d in sorted(usable.items())]}},
                    "agentTraces": {"sessionSpans": spans}}})
            jobs[r["recommendationId"]] = (agent, "tool_description")
            print(f"{agent}: started tool-description job {r['recommendationId']}")
        elif tool_descs or invoked:
            notes.append(f"{agent}: tool-description skipped, none of its described "
                         "tools were invoked in these traces")

    if not jobs:
        s3.put_object(Bucket=BUCKET, Key=f"runs/{run_id}/recommendation.json",
                      Body=json.dumps({"jobs": [], "notes": notes},
                                      indent=1).encode(),
                      ContentType="application/json")
        return {"runId": run_id, "jobs": [], "notes": notes}

    done = wait_jobs({jid: f"{a}/{kind}" for jid, (a, kind) in jobs.items()})
    out, results = [], {}
    for jid, resp in done.items():
        agent, kind = jobs.get(jid, ("unknown", "recommendation"))
        slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", agent)[:80]
        s3.put_object(
            Bucket=BUCKET,
            Key=f"runs/{run_id}/recommendation/{slug}/{kind}.md",
            Body=markdown_report(resp, agent).encode(), ContentType="text/markdown")
        res = resp.get("recommendationResult", {})
        err = ((res.get("systemPromptRecommendationResult") or {}).get("errorMessage")
               or (res.get("toolDescriptionRecommendationResult") or {}).get("errorMessage")
               or resp.get("failureReason") or resp.get("lastPollError"))
        out.append({"jobId": jid, "agent": agent, "type": kind,
                    "status": resp["status"], "error": err})
        results.setdefault(agent, {})[kind] = resp

    payload = {"runId": run_id, "jobs": out, "notes": notes,
               "droppedSessions": dropped_all, "results": results}
    s3.put_object(Bucket=BUCKET, Key=f"runs/{run_id}/recommendation.json",
                  Body=json.dumps(payload, ensure_ascii=False, indent=1,
                                  default=str).encode(),
                  ContentType="application/json")
    return {"runId": run_id, "jobs": out, "notes": notes}
