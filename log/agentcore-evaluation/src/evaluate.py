"""Step 4/5: run AgentCore batch Evaluation and Insights over the ingested sessions.

Two API facts shape this module:

  * `evaluators` and `insights` are mutually exclusive on StartBatchEvaluation,
    so scoring and insight generation are separate jobs.
  * `evaluators` accepts at most 10 entries. Running all 13 built-in evaluators
    therefore needs 2 jobs. Verified 2026-08-03: multiple batch evaluation jobs
    DO run concurrently, so the jobs are started together and polled as a set.

Excluded from the default evaluator set: Builtin.Trajectory{ExactOrder,InOrder,
AnyOrder}Match. They require ground truth (expectedTrajectory), which historical
Connect logs do not contain, so they would only ever report errors.
"""

import json
import os
import re
import time

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["RESULTS_BUCKET"]

ALL_BUILTIN = [
    "Builtin.Correctness",            # TRACE
    "Builtin.Faithfulness",           # TRACE
    "Builtin.Helpfulness",            # TRACE
    "Builtin.ResponseRelevance",      # TRACE
    "Builtin.Conciseness",            # TRACE
    "Builtin.Coherence",              # TRACE
    "Builtin.InstructionFollowing",   # TRACE
    "Builtin.Refusal",                # TRACE
    "Builtin.Harmfulness",            # TRACE
    "Builtin.Stereotyping",           # TRACE
    "Builtin.GoalSuccessRate",        # SESSION
    "Builtin.ToolSelectionAccuracy",  # TOOL_CALL
    "Builtin.ToolParameterAccuracy",  # TOOL_CALL
]
DEFAULT_INSIGHTS = ["Builtin.Insight.FailureAnalysis", "Builtin.Insight.UserIntent"]

MAX_EVALUATORS_PER_JOB = 10   # API limit
MAX_SESSION_IDS = 500         # API limit on filterConfig.sessionIds
POLL_SECONDS = int(os.environ.get("EVAL_POLL_SECONDS", "20"))
BUDGET_SECONDS = int(os.environ.get("EVAL_BUDGET_SECONDS", "780"))

agentcore = boto3.client("bedrock-agentcore", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def env_list(name, default):
    raw = os.environ.get(name, "").strip()
    return [x.strip() for x in raw.split(",") if x.strip()] or default


def job_name(prefix, run_id, suffix):
    """API pattern: [a-zA-Z][a-zA-Z0-9_]{0,47} - no hyphens allowed."""
    raw = f"{prefix}_{run_id}_{suffix}"
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "", raw)
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "run" + cleaned
    return cleaned[:48]


def data_source(ingest, session_ids):
    return {"cloudWatchLogs": {
        "serviceNames": [ingest["serviceName"]],       # exactly 1
        "logGroupNames": ingest["logGroupNames"],      # must include aws/spans
        "filterConfig": {"sessionIds": session_ids[:MAX_SESSION_IDS]}}}


def start_jobs(ingest, session_ids, run_id, mode):
    """mode: 'evaluators' | 'insights' -> [{jobId, kind, items}]"""
    ds = data_source(ingest, session_ids)
    jobs = []
    if mode == "evaluators":
        evaluators = env_list("EVALUATORS", ALL_BUILTIN)
        chunks = [evaluators[i:i + MAX_EVALUATORS_PER_JOB]
                  for i in range(0, len(evaluators), MAX_EVALUATORS_PER_JOB)]
        for n, chunk in enumerate(chunks, 1):
            resp = agentcore.start_batch_evaluation(
                batchEvaluationName=job_name("eval", run_id, f"p{n}"),
                evaluators=[{"evaluatorId": e} for e in chunk],
                dataSourceConfig=ds)
            jobs.append({"jobId": resp["batchEvaluationId"], "kind": "evaluators",
                         "items": chunk})
            print(f"started evaluators job {resp['batchEvaluationId']}: {chunk}")
    else:
        insights = env_list("INSIGHTS", DEFAULT_INSIGHTS)
        resp = agentcore.start_batch_evaluation(
            batchEvaluationName=job_name("insight", run_id, "p1"),
            insights=[{"insightId": i} for i in insights],
            dataSourceConfig=ds)
        jobs.append({"jobId": resp["batchEvaluationId"], "kind": "insights",
                     "items": insights})
        print(f"started insights job {resp['batchEvaluationId']}: {insights}")
    return jobs


def poll(jobs):
    """Wait for all jobs, returning their full GetBatchEvaluation responses."""
    pending = {j["jobId"]: j for j in jobs}
    done, deadline = {}, time.time() + BUDGET_SECONDS
    while pending and time.time() < deadline:
        for jid in list(pending):
            d = agentcore.get_batch_evaluation(batchEvaluationId=jid)
            if d["status"] in ("COMPLETED", "FAILED", "STOPPED"):
                d.pop("ResponseMetadata", None)
                d["_kind"] = pending.pop(jid)["kind"]
                done[jid] = d
                print(f"{jid}: {d['status']}")
        if pending:
            time.sleep(POLL_SECONDS)
    for jid, j in pending.items():
        print(f"{jid}: TIMEOUT after {BUDGET_SECONDS}s (job still running)")
        done[jid] = {"batchEvaluationId": jid, "status": "TIMEOUT",
                     "_kind": j["kind"]}
    return done


def handler(event, context):
    run_id = event["runId"]
    mode = event.get("mode", "evaluators")
    ingest = json.loads(s3.get_object(
        Bucket=BUCKET, Key=f"runs/{run_id}/ingest.json")["Body"].read())
    session_ids = ingest["sessionIds"]
    if not session_ids:
        return {"runId": run_id, "mode": mode, "jobs": [], "skipped": "no sessions"}
    if len(ingest["sessionIds"]) > MAX_SESSION_IDS:
        print(f"WARNING: {len(session_ids)} sessions exceeds the {MAX_SESSION_IDS} "
              "sessionIds limit; evaluating the first "
              f"{MAX_SESSION_IDS} and skipping the rest")

    jobs = start_jobs(ingest, session_ids, run_id, mode)
    done = poll(jobs)

    summary = []
    for jid, d in done.items():
        er = d.get("evaluationResults", {})
        summary.append({
            "jobId": jid,
            "kind": d.get("_kind"),
            "status": d["status"],
            "arn": d.get("batchEvaluationArn"),
            "resultsLogGroup": d.get("outputConfig", {})
                                .get("cloudWatchConfig", {}).get("logGroupName"),
            "resultsLogStream": d.get("outputConfig", {})
                                 .get("cloudWatchConfig", {}).get("logStreamName"),
            "sessions": {k: v for k, v in er.items() if k != "evaluatorSummaries"},
            "evaluatorSummaries": er.get("evaluatorSummaries", []),
            "failureAnalysisResult": d.get("failureAnalysisResult"),
            "userIntentResult": d.get("userIntentResult"),
            "executionSummaryResult": d.get("executionSummaryResult"),
        })

    key = f"runs/{run_id}/{'eval' if mode == 'evaluators' else 'insights'}.json"
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(summary, ensure_ascii=False, indent=1,
                                  default=str).encode(),
                  ContentType="application/json")
    return {"runId": run_id, "mode": mode, "key": key,
            "jobs": [{"jobId": s["jobId"], "status": s["status"]} for s in summary]}
