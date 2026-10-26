#!/usr/bin/env bash
#
# run-now.sh
#
# 立即触发一次评估流水线(不等定时调度)，并等待结束、打印结果位置。
#
# 用法:
#   ./run-now.sh [--instance <key>] [--stack <name>] [--region <region>] [--hours <n>]
#                [--max-sessions <n>] [--archive <s3://bucket/prefix>] [--force] [--no-wait]
#
# 参数:
#   --instance <key>    按 ../cloudfront/config-*.json 的 connect.<key> 取区域与 AWS profile
#   --hours <n>         本次回看小时数，覆盖栈里的默认值(便于首次回溯较长时间)
#   --max-sessions <n>  本次最多评估多少个会话(0=不限)，覆盖栈里的 MaxSessionsPerRun
#   --archive <s3://bucket/prefix>
#                       只用于验证: 不读日志组，改读该前缀下的 NDJSON 归档
#                       (每行 {"timestamp","message"})。需要部署时配置 ARCHIVE_BUCKET
#   --force             忽略去重台账，重新评估已评估过的会话(会重复产生评估费用)
#   --no-wait           只触发，不等待
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config.env"
[[ -f "${CONFIG_FILE}" ]] && source "${CONFIG_FILE}"

STACK_NAME="${STACK_NAME_CONF:-connect-agentcore-eval}"
REGION=""
INSTANCE_ARG=""
HOURS=""
MAX_SESSIONS=""
ARCHIVE=""
FORCE="false"
WAIT="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance) INSTANCE_ARG="$2"; shift 2 ;;
    --stack)   STACK_NAME="$2"; shift 2 ;;
    --region)  REGION="$2"; shift 2 ;;
    --hours)   HOURS="$2"; shift 2 ;;
    --max-sessions) MAX_SESSIONS="$2"; shift 2 ;;
    --archive) ARCHIVE="$2"; shift 2 ;;
    --force)   FORCE="true"; shift ;;
    --no-wait) WAIT="false"; shift ;;
    -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

CONNECT_INSTANCE="${INSTANCE_ARG:-${CONNECT_INSTANCE:-}}"
if [[ -n "${CONNECT_INSTANCE}" ]]; then
  # shellcheck source=instance-config.sh
  source "${SCRIPT_DIR}/instance-config.sh"
  load_instance "${CONNECT_INSTANCE}"
  REGION="${REGION:-${INSTANCE_LOG_REGION}}"
fi
REGION="${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
if [[ -z "${REGION}" ]]; then
  REGION="$(aws configure get region 2>/dev/null || echo us-east-1)"
fi

SM_ARN="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue | [0]" \
  --output text)"
if [[ -z "${SM_ARN}" || "${SM_ARN}" == "None" ]]; then
  echo "错误: 未能从栈 ${STACK_NAME} 读取 StateMachineArn，请先运行 deploy.sh" >&2
  exit 1
fi

RUN_ID="manual-$(date -u +%Y%m%dT%H%M%SZ)"
INPUT="$(python3 - "${RUN_ID}" "${HOURS}" "${FORCE}" "${MAX_SESSIONS}" "${ARCHIVE}" <<'PY'
import json, sys
run_id, hours, force, max_sessions, archive = sys.argv[1:6]
# runId is injected by the Collect step; passing it here keeps manual runs traceable
payload = {"source": "manual", "runId": run_id, "force": force}
if hours:
    payload["hours"] = int(hours)
if max_sessions:
    payload["maxSessions"] = int(max_sessions)
if archive:
    payload["archive"] = archive
print(json.dumps(payload))
PY
)"

# The state machine reads runId/hours/force out of the Collect step's own event,
# so the execution input is forwarded verbatim to that first Lambda.
EXEC_ARN="$(aws stepfunctions start-execution \
  --state-machine-arn "${SM_ARN}" \
  --name "${RUN_ID}" \
  --input "${INPUT}" \
  --region "${REGION}" \
  --query 'executionArn' --output text)"

echo "已启动: ${EXEC_ARN}"
[[ "${WAIT}" == "true" ]] || exit 0

echo "等待执行结束(评估通常需要 3-15 分钟，取决于会话数与评估器数量) ..."
while true; do
  STATUS="$(aws stepfunctions describe-execution --execution-arn "${EXEC_ARN}" \
    --region "${REGION}" --query 'status' --output text)"
  [[ "${STATUS}" == "RUNNING" ]] || break
  sleep 20
done

echo "执行状态: ${STATUS}"
aws stepfunctions describe-execution --execution-arn "${EXEC_ARN}" \
  --region "${REGION}" --query 'output' --output text 2>/dev/null || true
echo ""

if [[ "${STATUS}" != "SUCCEEDED" ]]; then
  echo "失败原因:" >&2
  aws stepfunctions describe-execution --execution-arn "${EXEC_ARN}" \
    --region "${REGION}" --query '[error,cause]' --output text >&2 || true
  echo "" >&2
  echo "已转换的会话仍保留在 S3(runs/<runId>/sessions/)，重跑即可继续评估。" >&2
  exit 1
fi

BUCKET="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='ResultsBucket'].OutputValue | [0]" \
  --output text)"
echo "下载结果:"
echo "  aws s3 sync s3://${BUCKET}/runs/ ./results/ --region ${REGION}"
