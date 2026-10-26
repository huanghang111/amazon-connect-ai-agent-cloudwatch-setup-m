"""Step 7: read per-session scores, render charts, write everything to S3.

Batch evaluation writes one gen_ai.evaluation.result log event per
(session, evaluator) into its results log group. Those events - not the job
summaries - carry the individual scores and the LLM explanations, so they are
what the heatmap and the CSV are built from.

Also writes the dedup ledger: a session is recorded as evaluated only once its
results have actually been published, so a failed run leaves it eligible for the
next attempt instead of silently dropping it.

matplotlib is optional. If the layer is missing, the JSON/CSV/HTML artifacts are
still produced and the summary says the charts were skipped, rather than failing
the whole run over a picture.
"""

import csv
import io
import json
import os
import time
from collections import Counter, defaultdict

import boto3

from converter import connect_session_id

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["RESULTS_BUCKET"]
LEDGER_TABLE = os.environ["LEDGER_TABLE"]
LEDGER_TTL_DAYS = int(os.environ.get("LEDGER_TTL_DAYS", "30"))

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)

# palette and ordering shared with the standalone plot_results.py
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SERIES_1 = "#2a78d6"
INK = "#1a1a19"
INK_MUTED = "#6f6e66"
GRID = "#e8e7e2"
SURFACE = "#fcfcfb"
MISSING = "#f1f0eb"
LOWER_IS_BETTER = {"Builtin.Refusal", "Builtin.Harmfulness", "Builtin.Stereotyping"}
EVALUATOR_ORDER = [
    "Builtin.Helpfulness", "Builtin.Correctness", "Builtin.Faithfulness",
    "Builtin.ResponseRelevance", "Builtin.Conciseness", "Builtin.Coherence",
    "Builtin.InstructionFollowing", "Builtin.Refusal",
    "Builtin.Harmfulness", "Builtin.Stereotyping",
    "Builtin.GoalSuccessRate",
    "Builtin.ToolSelectionAccuracy", "Builtin.ToolParameterAccuracy",
]


def read_result_events(jobs):
    """-> [{sessionId, evaluator, score, label, explanation}] from the results log group."""
    rows = []
    for job in jobs:
        lg, ls = job.get("resultsLogGroup"), job.get("resultsLogStream")
        if not lg or not ls:
            continue
        token = None
        while True:
            kw = {"logGroupName": lg, "logStreamName": ls, "startFromHead": True}
            if token:
                kw["nextToken"] = token
            try:
                r = logs.get_log_events(**kw)
            except logs.exceptions.ResourceNotFoundException:
                print(f"results stream not found yet: {lg}/{ls}")
                break
            for e in r["events"]:
                try:
                    msg = json.loads(e["message"])
                except json.JSONDecodeError:
                    continue
                a = msg.get("attributes", {})
                if "gen_ai.evaluation.name" not in a:
                    continue
                rows.append({
                    # spans carry a run-scoped session.id; report the real one
                    "sessionId": connect_session_id(a.get("session.id")),
                    "evaluator": a.get("gen_ai.evaluation.name"),
                    "score": a.get("gen_ai.evaluation.score.value"),
                    "label": a.get("gen_ai.evaluation.score.label"),
                    "explanation": a.get("gen_ai.evaluation.explanation"),
                    "error": a.get("error.message"),
                    "traceId": msg.get("traceId"),
                })
            if not r["events"] or r.get("nextForwardToken") == token:
                break
            token = r["nextForwardToken"]
    return rows


def grouped(rows):
    """-> {evaluator: {sessionId: [scores]}}"""
    data = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r["score"] is None or r["sessionId"] is None:
            continue
        data[r["evaluator"]][r["sessionId"]].append(float(r["score"]))
    return data


def ordered_evaluators(data):
    known = [e for e in EVALUATOR_ORDER if e in data]
    return known + sorted(e for e in data if e not in EVALUATOR_ORDER)


def render_charts(data, run_id):
    """Heatmap + overview bars -> S3. Returns the keys written."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    evaluators = ordered_evaluators(data)
    sessions = sorted({s for ev in data.values() for s in ev})
    cmap = LinearSegmentedColormap.from_list("blues", BLUE_RAMP)
    written = []

    def ylabel(e):
        name = e.replace("Builtin.", "")
        return name + " ↓" if e in LOWER_IS_BETTER else name

    def upload(fig, name):
        buf = io.BytesIO()
        fig.savefig(buf, format="png", facecolor=SURFACE, bbox_inches="tight")
        buf.seek(0)
        key = f"runs/{run_id}/charts/{name}"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(),
                      ContentType="image/png")
        written.append(key)
        plt.close(fig)

    # heatmap: session x evaluator
    fig, ax = plt.subplots(
        figsize=(max(6, 1.35 * len(sessions) + 3.6), 0.46 * len(evaluators) + 1.8),
        dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    n_total = 0
    for y, ev in enumerate(evaluators):
        for x, sid in enumerate(sessions):
            scores = data[ev].get(sid, [])
            if scores:
                mean = sum(scores) / len(scores)
                n_total += len(scores)
                ax.add_patch(plt.Rectangle((x + 0.03, y + 0.06), 0.94, 0.88,
                                           facecolor=cmap(mean), edgecolor="none"))
                ax.text(x + 0.5, y + 0.5,
                        f"{mean:.2f}" + (f" ({len(scores)})" if len(scores) > 1 else ""),
                        ha="center", va="center", fontsize=8,
                        color="#ffffff" if mean > 0.55 else INK)
            else:
                ax.add_patch(plt.Rectangle((x + 0.03, y + 0.06), 0.94, 0.88,
                                           facecolor=MISSING, edgecolor="none"))
                ax.text(x + 0.5, y + 0.5, "–", ha="center", va="center",
                        fontsize=8, color=INK_MUTED)
    ax.set_xlim(0, len(sessions))
    ax.set_ylim(len(evaluators), 0)
    ax.set_xticks([x + 0.5 for x in range(len(sessions))])
    ax.set_xticklabels([s.split("-")[0] for s in sessions], fontsize=8.5, color=INK)
    ax.set_yticks([y + 0.5 for y in range(len(evaluators))])
    ax.set_yticklabels([ylabel(e) for e in evaluators], fontsize=8.5, color=INK)
    ax.tick_params(length=0)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.set_xlabel("Session", fontsize=9, color=INK_MUTED)
    ax.set_title("AgentCore Evaluation — mean score per session × evaluator",
                 fontsize=11.5, color=INK, fontweight="bold", loc="left", pad=26)
    ax.text(0, -0.4, f"{len(sessions)} sessions · {n_total} evaluations · "
            "cell = mean score 0–1 (n traces) · – = not applicable · "
            "↓ = lower is better", fontsize=8, color=INK_MUTED)
    sm = plt.cm.ScalarMappable(cmap=cmap)
    sm.set_clim(0, 1)
    cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02, ticks=[0, 0.5, 1])
    cbar.ax.tick_params(labelsize=7.5, colors=INK_MUTED, length=0)
    cbar.outline.set_visible(False)
    fig.tight_layout()
    upload(fig, "eval_scores.png")

    # overview: mean per evaluator with per-session dots
    fig, ax = plt.subplots(figsize=(7.5, 0.4 * len(evaluators) + 1.6), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for y, ev in enumerate(evaluators):
        session_means = [sum(v) / len(v) for v in data[ev].values()]
        overall = sum(sum(v) for v in data[ev].values()) / \
            sum(len(v) for v in data[ev].values())
        ax.barh(y, overall, height=0.62, color=SERIES_1, zorder=3)
        ax.scatter(session_means, [y] * len(session_means), s=26, zorder=4,
                   facecolor=SURFACE, edgecolor=SERIES_1, linewidth=1.4)
        ax.annotate(f"{overall:.2f}", (max([overall] + session_means), y),
                    textcoords="offset points", xytext=(8, 0), va="center",
                    fontsize=8.5, color=INK, fontweight="bold")
    ax.set_yticks(list(range(len(evaluators))))
    ax.set_yticklabels([ylabel(e) for e in evaluators], fontsize=8.5, color=INK)
    ax.invert_yaxis()
    ax.set_xlim(0, 1.14)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
    ax.tick_params(colors=INK_MUTED, length=0)
    ax.grid(axis="x", color=GRID, linewidth=0.8, zorder=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.set_xlabel("Score (0–1)", fontsize=9, color=INK_MUTED)
    ax.set_title("AgentCore Evaluation — overall score by evaluator",
                 fontsize=11.5, color=INK, fontweight="bold", loc="left", pad=20)
    ax.text(0, 1.03, "bar = mean of all evaluations · dot = per-session mean · "
            "↓ = lower is better", transform=ax.transAxes, fontsize=8,
            color=INK_MUTED)
    fig.tight_layout()
    upload(fig, "eval_overview.png")
    return written


def write_csv(rows, run_id):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["sessionId", "evaluator", "score", "label",
                                        "explanation", "error", "traceId"])
    w.writeheader()
    w.writerows(rows)
    key = f"runs/{run_id}/eval_scores.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode(),
                  ContentType="text/csv")
    return key


def write_ledger(session_ids, run_id):
    ttl = int(time.time()) + LEDGER_TTL_DAYS * 86400
    for sid in session_ids:
        ddb.put_item(TableName=LEDGER_TABLE, Item={
            "sessionId": {"S": sid},
            "runId": {"S": run_id},
            "evaluatedAt": {"N": str(int(time.time()))},
            "expiresAt": {"N": str(ttl)}})


def load(run_id, name, default):
    try:
        return json.loads(s3.get_object(
            Bucket=BUCKET, Key=f"runs/{run_id}/{name}")["Body"].read())
    except s3.exceptions.NoSuchKey:
        return default


def render_index(summary, run_id):
    """Small landing page so the S3 prefix is browsable without tooling."""
    ev = summary["evaluatorAverages"]
    rows = "".join(
        f"<tr><td>{k}</td><td style='text-align:right'>{v:.3f}</td></tr>"
        for k, v in sorted(ev.items()))
    failed = summary.get("evaluationsFailed") or {}
    reasons = summary.get("evaluationErrors") or []
    reasons_note = ("<p style='color:#b3261e'>" + "<br>".join(
        f"<code>{r}</code>" for r in reasons) + "</p>") if reasons else ""
    # loud, because a page full of plausible scores over a handful of mapped spans
    # is the failure mode that gets believed
    failed_note = ("<p style='color:#b3261e'><b>" + str(sum(failed.values()))
                   + " evaluations failed to map to a span</b> (see "
                   "<code>evaluationsFailed</code> in summary.json and "
                   "<code>error.message</code> in the results log stream): "
                   + ", ".join(f"{k} x{v}" for k, v in sorted(failed.items()))
                   + "</p>") if failed else ""
    agents = summary.get("agentsEvaluated") or []
    agents_note = ("<p>AI agents: " + ", ".join(f"<code>{a}</code>" for a in agents)
                   + "</p>") if agents else ""
    intents = summary.get("userIntents") or []
    intent_rows = "".join(
        f"<tr><td>{i.get('name')}</td><td style='text-align:right'>"
        f"{i.get('affectedSessionCount')}</td><td>{i.get('description','')}</td></tr>"
        for i in intents)
    return f"""<!doctype html><meta charset="utf-8">
<title>AgentCore evaluation run {run_id}</title>
<style>body{{font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;
max-width:960px;margin:40px auto;padding:0 16px;color:{INK};background:{SURFACE}}}
h1{{font-size:20px}}h2{{font-size:15px;margin-top:32px}}
table{{border-collapse:collapse;width:100%}}
td,th{{border-bottom:1px solid {GRID};padding:6px 8px;text-align:left;
vertical-align:top}}img{{max-width:100%;border:1px solid {GRID};margin:8px 0}}
code{{background:#f4f3ee;padding:1px 4px;border-radius:3px}}</style>
<h1>AgentCore evaluation run <code>{run_id}</code></h1>
<p>{summary['sessionsEvaluated']} sessions evaluated &middot;
{summary['totalEvaluations']} individual evaluations &middot;
window {summary['window']['hours']}h</p>
{agents_note}
{failed_note}
{reasons_note}
<h2>Scores by evaluator</h2><table><tr><th>Evaluator</th><th>Mean</th></tr>
{rows}</table>
<h2>Charts</h2>
<img src="charts/eval_overview.png" alt="overview">
<img src="charts/eval_scores.png" alt="heatmap">
<h2>User intents</h2>
<table><tr><th>Intent</th><th>Sessions</th><th>Description</th></tr>
{intent_rows or '<tr><td colspan=3>none reported</td></tr>'}</table>
<h2>Files</h2><ul>
<li><code>eval_scores.csv</code> - every score with its explanation</li>
<li><code>eval.json</code> / <code>insights.json</code> - raw job output</li>
<li><code>recommendation/&lt;agent&gt;/*.md</code> - optimized prompt and tool descriptions, per AI agent</li>
<li><code>sessions/*.json</code> - converted OTEL spans per session</li>
<li><code>collect.json</code> / <code>ingest.json</code> - what ran, what was skipped</li>
</ul>"""


def handler(event, context):
    run_id = event["runId"]
    collect = load(run_id, "collect.json", {})
    eval_jobs = load(run_id, "eval.json", [])
    insight_jobs = load(run_id, "insights.json", [])
    rec = load(run_id, "recommendation.json", {})

    rows = read_result_events(eval_jobs)
    data = grouped(rows)
    csv_key = write_csv(rows, run_id)

    charts, chart_note = [], None
    if data:
        try:
            charts = render_charts(data, run_id)
        except ImportError as e:
            chart_note = f"charts skipped: matplotlib unavailable ({e})"
            print(chart_note)
    else:
        chart_note = "charts skipped: no scores were produced"

    intents, failures, exec_summary = [], [], None
    for j in insight_jobs:
        intents += (j.get("userIntentResult") or {}).get("userIntents", []) or []
        fa = j.get("failureAnalysisResult") or {}
        failures += fa.get("failureModes", []) or fa.get("failures", []) or []
        exec_summary = exec_summary or j.get("executionSummaryResult")

    # a session counts as evaluated only if at least one evaluator produced a
    # score; result events that carry only error.message (e.g. "no span
    # documents") must leave the session eligible for the next run
    evaluated = sorted({r["sessionId"] for r in rows
                        if r["sessionId"] and r["score"] is not None})
    unscored = sorted({r["sessionId"] for r in rows if r["sessionId"]} - set(evaluated))
    # the distinct reasons, so summary.json says WHY nothing was scored
    error_reasons = sorted({(r.get("error") or "")[:300] for r in rows
                            if r["score"] is None and r.get("error")})
    # which AI agent(s) each session ran through and which log format it came
    # in, so scores can be read per agent when several share one log group
    session_info = {s["sessionId"]: {"agents": s.get("agents", []),
                                     "format": s.get("format")}
                    for s in collect.get("sessions", [])}
    summary = {
        "runId": run_id,
        "window": collect.get("window", {}),
        "sessionsFound": collect.get("sessionsFound"),
        "sessionsSelected": collect.get("sessionsSelected"),
        "sessionsEvaluated": len(evaluated),
        "sessionsUnscored": unscored,
        "evaluationErrors": error_reasons,
        "sessionsSkipped": collect.get("skipped"),
        "totalEvaluations": len(rows),
        "sessionInfo": {sid: session_info.get(sid, {}) for sid in evaluated},
        "agentsEvaluated": sorted({a for sid in evaluated
                                   for a in session_info.get(sid, {}).get("agents", [])}),
        "sessionFormats": collect.get("sessionFormats"),
        "evaluatorAverages": {ev: sum(sum(v) for v in s.values())
                              / sum(len(v) for v in s.values())
                              for ev, s in data.items()},
        "evaluationJobs": [{"jobId": j.get("jobId"), "status": j.get("status")}
                           for j in eval_jobs],
        # A job reports COMPLETED as long as the SESSION completed, even when most
        # of its spans could not be mapped and scored (AgentSpanMappingException,
        # ToolSpanMappingException). Those are exactly the runs that look healthy
        # and are not, so the per-evaluator failure counts are surfaced here.
        "evaluationsFailed": dict(sorted(Counter(
            r["evaluator"] for r in rows if r["score"] is None).items())),
        # the job's own counters. For the tool evaluators these also count every
        # session WITHOUT tool calls as "failed", which is not a defect, so the
        # banner above is driven by result events that carry an error instead.
        "evaluatorFailedCounts": {
            s["evaluatorId"]: s.get("totalFailed", 0)
            for j in eval_jobs for s in (j.get("evaluatorSummaries") or [])
            if s.get("totalFailed")},
        "insightJobs": [{"jobId": j.get("jobId"), "status": j.get("status")}
                        for j in insight_jobs],
        "userIntents": intents,
        "failureModes": failures,
        "executionSummary": exec_summary,
        "recommendationJobs": rec.get("jobs", []),
        "recommendationNotes": rec.get("notes", []),
        "charts": charts,
        "chartNote": chart_note,
        "csv": csv_key,
    }
    s3.put_object(Bucket=BUCKET, Key=f"runs/{run_id}/summary.json",
                  Body=json.dumps(summary, ensure_ascii=False, indent=1,
                                  default=str).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key=f"runs/{run_id}/index.html",
                  Body=render_index(summary, run_id).encode(),
                  ContentType="text/html")

    # only now is a session safely "done" and eligible to be skipped next run
    write_ledger(evaluated, run_id)

    print(json.dumps({k: v for k, v in summary.items()
                      if k not in ("userIntents", "failureModes",
                                   "executionSummary")}, default=str))
    return {"runId": run_id, "sessionsEvaluated": len(evaluated),
            "totalEvaluations": len(rows),
            "output": f"s3://{BUCKET}/runs/{run_id}/",
            "chartNote": chart_note}
