#!/usr/bin/env bash
#
# diagnose-index-gate.sh
#
# 只读诊断: 状态机以 LogEventsNotIndexed 失败时，回答三个问题
#   1. 部署的是哪个版本的 wait_indexed(有没有带 parse 判定的修复)
#   2. 卡住的是哪一半——aws/spans 的 span 文档，还是运行时日志组的 log event
#   3. 数据到底是「没到」还是「到了但查询看不见」
#
# 用法:
#   ./diagnose-index-gate.sh [--instance <key>] [--stack <name>] [--region <region>] [--run-id <runId>]
#
#   --instance 按 ../cloudfront/config-*.json 的 connect.<key> 取区域与 AWS profile。
#   --run-id 省略时自动取状态机最近一次失败执行的 runId。
#
# 不修改任何资源: 只做 describe / get / Logs Insights 查询。
#
set -uo pipefail

STACK_NAME=""
REGION=""
RUN_ID=""
INSTANCE_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance) INSTANCE_ARG="$2"; shift 2 ;;
    --stack)   STACK_NAME="$2"; shift 2 ;;
    --region)  REGION="$2"; shift 2 ;;
    --run-id)  RUN_ID="$2"; shift 2 ;;
    -h|--help) sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/config.env" ]]; then
  # shellcheck disable=SC1090
  source "${SCRIPT_DIR}/config.env"
fi
CONNECT_INSTANCE="${INSTANCE_ARG:-${CONNECT_INSTANCE:-}}"
if [[ -n "${CONNECT_INSTANCE}" ]]; then
  # shellcheck source=instance-config.sh
  source "${SCRIPT_DIR}/instance-config.sh"
  load_instance "${CONNECT_INSTANCE}"
  REGION="${REGION:-${INSTANCE_LOG_REGION}}"
fi
STACK_NAME="${STACK_NAME:-${STACK_NAME_CONF:-connect-agentcore-eval}}"
REGION="${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || echo us-east-1)}}}"

echo "==> 诊断对象: stack=${STACK_NAME} region=${REGION}"

out() { aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue|[0]" --output text 2>/dev/null; }

BUCKET="$(out ResultsBucket)"
RT_LOG_GROUP="$(out ObservabilityLogGroup)"
SM_ARN="$(out StateMachineArn)"
SPANS_LOG_GROUP="aws/spans"

if [[ -z "${BUCKET}" || "${BUCKET}" == "None" ]]; then
  echo "错误: 读不到栈 ${STACK_NAME} 的输出，确认 --stack / --region 是否正确" >&2
  exit 1
fi
echo "    结果桶      : ${BUCKET}"
echo "    运行时日志组: ${RT_LOG_GROUP}"

# ---------------------------------------------------------------------------
# 1. 部署的是哪个版本
# ---------------------------------------------------------------------------
echo ""
echo "==> 1. wait_indexed 版本"
ENV_JSON="$(aws lambda get-function-configuration \
  --function-name "${STACK_NAME}-wait-indexed" --region "${REGION}" \
  --query 'Environment.Variables' --output json 2>/dev/null || echo '{}')"
echo "    环境变量: ${ENV_JSON}"
if grep -q "REQUIRE_SPAN_INDEX" <<<"${ENV_JSON}"; then
  echo "    ✅ 带索引门修复(parse @message 判定 + REQUIRE_SPAN_INDEX 开关)"
else
  echo "    ❌ 仍是修复前的版本: 判定用 filter ispresent(spanId)。"
  echo "       在某些区域这个查询对 aws/spans 恒返回 0 条，索引门永远打不开,"
  echo "       每次运行都必然以 LogEventsNotIndexed 结束。请先更新代码再重新部署。"
fi
echo "    预算/轮次: INDEX_BUDGET_SECONDS=$(grep -oE '"INDEX_BUDGET_SECONDS": *"[0-9]+"' <<<"${ENV_JSON}" | grep -oE '[0-9]+' || echo '默认 180')，状态机最多 5 次 attempt"

# ---------------------------------------------------------------------------
# 2. 卡住的是哪一半
# ---------------------------------------------------------------------------
echo ""
echo "==> 2. 最近一次失败执行"
if [[ -z "${RUN_ID}" && -n "${SM_ARN}" && "${SM_ARN}" != "None" ]]; then
  RUN_ID="$(aws stepfunctions list-executions --state-machine-arn "${SM_ARN}" \
    --status-filter FAILED --max-items 1 --region "${REGION}" \
    --query 'executions[0].name' --output text 2>/dev/null)"
fi
if [[ -z "${RUN_ID}" || "${RUN_ID}" == "None" ]]; then
  echo "    没有找到失败执行，用 --run-id 手工指定"; exit 0
fi
echo "    runId: ${RUN_ID}"

echo ""
echo "    wait_indexed 的判定过程(每轮两半各缺多少):"
aws logs filter-log-events --log-group-name "/aws/lambda/${STACK_NAME}-wait-indexed" \
  --start-time "$(( ($(date +%s) - 3 * 86400) * 1000 ))" --region "${REGION}" \
  --query 'events[].message' --output text 2>/dev/null \
  | tr '\t' '\n' > /tmp/wait-indexed-lines.txt
grep -E "waiting to index|budget exhausted|all .* spans indexed|query on|missing spanIds|NOTE:" \
  /tmp/wait-indexed-lines.txt | tail -28 | sed 's/^/      /'
echo "      (span documents 一直是满值 = span 那半没通过; log events 一直是满值 = 事件那半没通过;"
echo "       两半都归零那一行出现过，就说明门开过，失败在别处)"

# ---------------------------------------------------------------------------
# 3. 数据没到，还是到了但查不到
# ---------------------------------------------------------------------------
echo ""
echo "==> 3. 取一个本次注入的 spanId 实测三种查询"
ING="$(aws s3 cp "s3://${BUCKET}/runs/${RUN_ID}/ingest.json" - --region "${REGION}" 2>/dev/null)"
if [[ -z "${ING}" ]]; then
  echo "    读不到 runs/${RUN_ID}/ingest.json，跳过"; exit 0
fi
SID="$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print((d.get('spanIds') or [''])[0])" <<<"${ING}")"
# a span the gate reported as MISSING is the only interesting one: spanIds[0] is
# usually one that already landed, and then all three queries return 1 and prove
# nothing. wait_indexed logs the ids since 2026-09-10.
MISSING_SID="$(grep -oE "missing spanIds \(span documents[^]]*\[[^]]*" /tmp/wait-indexed-lines.txt 2>/dev/null \
  | tail -1 | grep -oE "[0-9a-f]{16}" | head -1)"
MISSING_WHAT="span 文档"
if [[ -z "${MISSING_SID}" ]]; then   # 没有 span 侧缺失就退到事件侧
  MISSING_SID="$(grep -oE "missing spanIds \(log events[^]]*\[[^]]*" /tmp/wait-indexed-lines.txt 2>/dev/null \
    | tail -1 | grep -oE "[0-9a-f]{16}" | head -1)"
  MISSING_WHAT="log event"
fi
N_SPANS="$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(len(d.get('spanIds') or []))" <<<"${ING}")"
FAILURES="$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(json.dumps(d.get('failures') or [], ensure_ascii=False))" <<<"${ING}")"
START_EPOCH="$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); r=d.get('spanTimeRange') or {}; print(r.get('startEpoch') or r.get('ingestedAtEpoch') or 0)" <<<"${ING}")"
REJECTED="$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('rejectedSpans','n/a(旧版本 ingest)'), d.get('rejectedSpanErrors') or '')" <<<"${ING}")"
echo "    注入 span 数: ${N_SPANS}   ingest 失败的会话: ${FAILURES}"
echo "    OTLP 端点拒收的 span: ${REJECTED}"
echo "      (>0 就是根因: 这些 span 根本没被 X-Ray 收下，永远不会出现在 aws/spans,"
echo "       索引门等到超时也不可能通过)"
echo "    ingest.json spanTimeRange: $(python3 -c "import json,sys; print(json.dumps((json.loads(sys.stdin.read()).get('spanTimeRange') or {})))" <<<"${ING}")"
[[ -z "${SID}" ]] && { echo "    ingest.json 里没有 spanId，说明第 2 步(Ingest)就没写进去"; exit 0; }

# 索引门用的窗口(span 文档按 span 自己的 startTimeUnixNano 落库，实测确认)
QSTART=$(( START_EPOCH - 3600 ))
QEND=$(( $(date +%s) + 600 ))
# 宽窗口: 覆盖 aws/spans 的整个保留期。用来区分「文档不存在」和「文档在窗口外」
WSTART=$(( $(date +%s) - 35 * 86400 ))
WEND=$(( $(date +%s) + 3600 ))
echo "    索引门窗口  : [${QSTART}, ${QEND}]"
echo "    对照宽窗口  : [${WSTART}, ${WEND}] (35 天)"

run_q() {  # $1=log group  $2=query  $3=start  $4=end  -> recordsMatched
  local qid matched
  qid="$(aws logs start-query --log-group-name "$1" --start-time "$3" \
    --end-time "$4" --query-string "$2" --region "${REGION}" \
    --query queryId --output text 2>/dev/null)" || { echo "查询失败"; return; }
  for _ in $(seq 1 30); do
    matched="$(aws logs get-query-results --query-id "${qid}" --region "${REGION}" \
      --query '[status,statistics.recordsMatched]' --output text 2>/dev/null)"
    [[ "${matched}" == Running* || "${matched}" == Scheduled* ]] || { echo "${matched}" | awk '{print $2}'; return; }
    sleep 2
  done
  echo "超时"
}

probe_sid() {  # $1=spanId  $2=标签
  local sid="$1" label="$2" lg
  local parse_q raw_q field_q
  parse_q='parse @message /"spanId":\s*"(?<sid>[0-9a-fA-F]{16})"/ | filter sid = "'"${sid}"'" | limit 5'
  raw_q='fields @message | filter @message like /'"${sid}"'/ | limit 5'
  field_q='fields spanId | filter spanId = "'"${sid}"'" | limit 5'
  echo ""
  echo "    --- ${label}: ${sid}"
  for lg in "${SPANS_LOG_GROUP}" "${RT_LOG_GROUP}"; do
    echo "      ${lg}"
    echo "        A 原始文本 (@message like)        门窗口=$(run_q "${lg}" "${raw_q}" "${QSTART}" "${QEND}")  宽窗口=$(run_q "${lg}" "${raw_q}" "${WSTART}" "${WEND}")"
    echo "        B parse 判定(流水线在用)          门窗口=$(run_q "${lg}" "${parse_q}" "${QSTART}" "${QEND}")  宽窗口=$(run_q "${lg}" "${parse_q}" "${WSTART}" "${WEND}")"
    echo "        C 字段判定 (旧 ispresent 路径)    门窗口=$(run_q "${lg}" "${field_q}" "${QSTART}" "${QEND}")"
  done
}

if [[ -n "${MISSING_SID}" ]]; then
  probe_sid "${MISSING_SID}" "索引门报告【缺失】的 span(${MISSING_WHAT}那一半)"
else
  echo ""
  echo "    (日志里没有 'missing spanIds' 行 —— 要么这次两半都通过了，要么 wait_indexed"
  echo "     还是 2026-09-10 之前的版本。只能退而测 ingest.json 的第一个 span,"
  echo "     它通常是已经落库的那批，测不到关键对象)"
fi
probe_sid "${SID}" "ingest.json 的第一个 span(对照)"

echo ""
echo "    aws/spans 里一共有多少文档(35 天):"
echo "      $(run_q "${SPANS_LOG_GROUP}" 'fields @message | filter @message like /spanId/ | limit 1' "${WSTART}" "${WEND}") 条"

cat <<'HINT'

    怎么读(以【缺失】那个 span 为准):
      两个窗口都 0    -> 文档确实不在 aws/spans。X-Ray 收下了请求(rejectedSpans=0)却没落库,
                         对照第 4 步的 Transaction Search 状态，并把 ingest.json 与本段一起发出来。
      门窗口 0/宽窗口>0 -> 文档在，但落在索引门查询的时间窗之外。把上面两个窗口和文档的
                         @timestamp 对比即可定位(实测: 文档时间戳 = span 的 startTimeUnixNano)。
      两个窗口都>0     -> 查询能看到，判定却说缺失: 说明是时序——查的那一刻还没可见。
                         实测同一批 span 的可见时间能错开 6 分钟以上，单轮预算 180s 不够:
                         调大 INDEX_BUDGET_SECONDS(≤240)与 INDEX_MAX_ATTEMPTS。
      A>0 而 B=0       -> 序列化形状与 parse 不符，把 A 查到的原始 @message 发出来。
HINT

# ---------------------------------------------------------------------------
# 4. Transaction Search
# ---------------------------------------------------------------------------
echo ""
echo "==> 4. Transaction Search(决定 span 文档能不能进 aws/spans)"
aws xray get-trace-segment-destination --region "${REGION}" --output table 2>/dev/null \
  || echo "    读取失败(权限?)"
aws xray get-indexing-rules --region "${REGION}" \
  --query 'IndexingRules[].{Name:Name,SamplingPct:Rule.Probabilistic.DesiredSamplingPercentage}' \
  --output table 2>/dev/null || true
echo "    Destination 必须是 CloudWatchLogs 且 Status=ACTIVE,否则 aws/spans 永远收不到 span。"
