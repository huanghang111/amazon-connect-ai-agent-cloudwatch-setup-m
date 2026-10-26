#!/usr/bin/env bash
#
# deploy.sh
#
# 一键部署 Connect AI Agent 日志自动评估流水线。
#
# 用法:
#   ./deploy.sh [--instance <key>] [--config <file>] [--region <region>] \
#               [--stack <name>] [--bucket <name>] [--schedule "<cron>"] \
#               [--no-schedule] [--no-charts] [--yes]
#
#   --instance <key>  从 ../cloudfront/config-*.json 的 connect.<key> 读取实例 ARN 与
#                     ai_log_arn，自动得到区域、账号(AWS profile)与 Connect 日志组
#
# 做的事:
#   1. 检查依赖与 Connect 日志组是否存在
#   2. 检查并按需开启 CloudWatch Transaction Search
#      (AgentCore 的会话发现依赖它，未开启则批量评估永远返回 0 个会话)
#   3. 构建 Lambda layer:
#      - boto3(必需，Lambda 自带版本不认识 AgentCore 批量评估 API)
#      - matplotlib(用于出图；失败则降级为只出 JSON/CSV)
#   4. 部署 CloudFormation 栈(S3 + DynamoDB + 6 个 Lambda + Step Functions + 定时调度)
#   5. 打印结果 S3 位置与手动触发方式
#
# 依赖: aws cli v2(已配置凭证)、python3、pip、zip。
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config.env"
STACK_NAME=""
REGION=""
BUCKET=""
SCHEDULE=""
SCHEDULE_ENABLED=""
BUILD_CHARTS="true"
ASSUME_YES="false"
INSTANCE_ARG=""

usage() { sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)    INSTANCE_ARG="$2"; shift 2 ;;
    --config)      CONFIG_FILE="$2"; shift 2 ;;
    --region)      REGION="$2"; shift 2 ;;
    --stack)       STACK_NAME="$2"; shift 2 ;;
    --bucket)      BUCKET="$2"; shift 2 ;;
    --schedule)    SCHEDULE="$2"; shift 2 ;;
    --no-schedule) SCHEDULE_ENABLED="false"; shift ;;
    --no-charts)   BUILD_CHARTS="false"; shift ;;
    --yes|-y)      ASSUME_YES="true"; shift ;;
    -h|--help)     usage ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# 0. 读取配置与默认值
# ---------------------------------------------------------------------------
if [[ -f "${CONFIG_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
  echo "==> 已加载配置: ${CONFIG_FILE}"
else
  echo "==> 未找到 ${CONFIG_FILE}，全部使用默认值(可 cp config.env.example config.env 后修改)"
fi

command -v aws >/dev/null 2>&1 || { echo "错误: 未找到 aws CLI" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "错误: 未找到 python3" >&2; exit 1; }

# 选了实例时，区域与日志组以实例配置为准(命令行 --region、config.env 里显式写的
# CONNECT_LOG_GROUP 仍可覆盖)。区域不让 AWS_REGION 抢先: 它常常是 shell 里全局设置的，
# 会把流水线悄悄部署到日志组之外的区域。
CONNECT_INSTANCE="${INSTANCE_ARG:-${CONNECT_INSTANCE:-}}"
if [[ -n "${CONNECT_INSTANCE}" ]]; then
  # shellcheck source=instance-config.sh
  source "${SCRIPT_DIR}/instance-config.sh"
  load_instance "${CONNECT_INSTANCE}"
  if [[ -z "${CONNECT_LOG_GROUP:-}" ]]; then
    resolve_instance_log_group
    CONNECT_LOG_GROUP="${INSTANCE_LOG_GROUP}"
  fi
  REGION="${REGION:-${INSTANCE_LOG_REGION}}"
fi

STACK_NAME="${STACK_NAME:-${STACK_NAME_CONF:-connect-agentcore-eval}}"
REGION="${REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
if [[ -z "${REGION}" ]]; then
  REGION="$(aws configure get region 2>/dev/null || true)"
fi
REGION="${REGION:-us-east-1}"

CONNECT_LOG_GROUP="${CONNECT_LOG_GROUP:-/aws/connect/ai-agent-logs}"
OBSERVABILITY_RUNTIME_NAME="${OBSERVABILITY_RUNTIME_NAME:-connect-ai-agent}"
LOOKBACK_HOURS="${LOOKBACK_HOURS:-24}"
MAX_AGE_DAYS="${MAX_AGE_DAYS:-14}"
OVERLAP_MINUTES="${OVERLAP_MINUTES:-30}"
SETTLE_MINUTES="${SETTLE_MINUTES:-10}"
EVALUATORS="${EVALUATORS:-ALL}"
INSIGHTS="${INSIGHTS:-Builtin.Insight.FailureAnalysis,Builtin.Insight.UserIntent}"
RECOMMENDATION_EVALUATOR="${RECOMMENDATION_EVALUATOR:-Helpfulness}"
SCHEDULE="${SCHEDULE:-${SCHEDULE_EXPRESSION:-cron(0 3 * * ? *)}}"
SCHEDULE_ENABLED="${SCHEDULE_ENABLED:-${SCHEDULE_ENABLED_CONF:-true}}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"
LEDGER_TTL_DAYS="${LEDGER_TTL_DAYS:-30}"
REQUIRE_SPAN_INDEX="${REQUIRE_SPAN_INDEX:-true}"
INDEX_BUDGET_SECONDS="${INDEX_BUDGET_SECONDS:-180}"
INDEX_MAX_ATTEMPTS="${INDEX_MAX_ATTEMPTS:-5}"
MAX_SESSIONS_PER_RUN="${MAX_SESSIONS_PER_RUN:-0}"
SHIFT_OLDER_THAN_DAYS="${SHIFT_OLDER_THAN_DAYS:-1}"
ARCHIVE_BUCKET="${ARCHIVE_BUCKET:-}"
BUCKET="${BUCKET:-${RESULTS_BUCKET:-}}"
ENABLE_TRANSACTION_SEARCH="${ENABLE_TRANSACTION_SEARCH:-true}"

if [[ -n "${CONNECT_INSTANCE}" && "${REGION}" != "${INSTANCE_LOG_REGION}" ]]; then
  echo "错误: 部署区域 ${REGION} 与实例 ${CONNECT_INSTANCE} 的日志组区域 ${INSTANCE_LOG_REGION} 不一致。" >&2
  echo "      流水线用 Logs Insights 读日志组，必须部署在日志组所在区域。" >&2
  exit 1
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

echo ""
echo "==> 部署参数"
echo "    Region            : ${REGION}"
echo "    Account           : ${ACCOUNT_ID}"
[[ -n "${CONNECT_INSTANCE}" ]] && echo "    Connect 实例      : ${CONNECT_INSTANCE} (${INSTANCE_ARN})"
echo "    Stack             : ${STACK_NAME}"
echo "    Connect 日志组    : ${CONNECT_LOG_GROUP}"
echo "    Observability 名称: ${OBSERVABILITY_RUNTIME_NAME}"
echo "    回看窗口          : ${LOOKBACK_HOURS}h (+${OVERLAP_MINUTES}min 重叠)"
echo "    评估器            : ${EVALUATORS}"
echo "    Insights          : ${INSIGHTS}"
echo "    定时调度          : ${SCHEDULE} (enabled=${SCHEDULE_ENABLED})"
echo "    结果桶            : ${BUCKET:-<自动创建>}"
echo "    每次最多会话数    : ${MAX_SESSIONS_PER_RUN} (0=不限)"
[[ -n "${ARCHIVE_BUCKET}" ]] && echo "    归档桶(仅验证用)  : ${ARCHIVE_BUCKET}"
echo ""

if [[ "${EVALUATORS}" == "ALL" && "${ASSUME_YES}" != "true" ]]; then
  echo "提示: 将启用全部 13 个内置评估器。评估按「每条 trace × 每个评估器」计费(LLM 调用)，"
  echo "      这是本方案的主要成本项。可在 config.env 里把 EVALUATORS 改成子集。"
fi

if [[ "${ASSUME_YES}" != "true" ]]; then
  read -r -p "确认部署? [y/N] " ans
  [[ "${ans}" =~ ^[Yy]$ ]] || { echo "已取消。"; exit 0; }
fi

# ---------------------------------------------------------------------------
# 1. 检查 Connect 日志组
# ---------------------------------------------------------------------------
echo ""
echo "==> 检查 Connect 日志组: ${CONNECT_LOG_GROUP}"
FOUND_LG="$(aws logs describe-log-groups \
  --log-group-name-prefix "${CONNECT_LOG_GROUP}" --region "${REGION}" \
  --query "logGroups[?logGroupName=='${CONNECT_LOG_GROUP}'].logGroupName | [0]" \
  --output text 2>/dev/null || true)"

if [[ -z "${FOUND_LG}" || "${FOUND_LG}" == "None" ]]; then
  echo "错误: 日志组不存在: ${CONNECT_LOG_GROUP}" >&2
  echo "      先用 ./find-connect-log-group.sh 查实例的日志实际投递到哪个日志组；" >&2
  echo "      确实没有投递时，再运行上一级目录的 setup_connect_ai_agent_logs.py。" >&2
  exit 1
fi
echo "    OK"

# ---------------------------------------------------------------------------
# 2. Transaction Search: 检查并按需开启
#
# AgentCore 批量评估不会扫描原始日志组，它通过 X-Ray / Transaction Search 的索引
# 发现会话。未开启时 totalNumberOfSessions 恒为 0，整条流水线静默空转。
# ---------------------------------------------------------------------------
echo ""
echo "==> 检查 CloudWatch Transaction Search"
TS_DEST="$(aws xray get-trace-segment-destination --region "${REGION}" \
  --query 'Destination' --output text 2>/dev/null || echo "UNKNOWN")"
TS_STATUS="$(aws xray get-trace-segment-destination --region "${REGION}" \
  --query 'Status' --output text 2>/dev/null || echo "UNKNOWN")"
echo "    当前: Destination=${TS_DEST} Status=${TS_STATUS}"

if [[ "${TS_DEST}" == "CloudWatchLogs" && "${TS_STATUS}" == "ACTIVE" ]]; then
  echo "    已开启，跳过。"
elif [[ "${ENABLE_TRANSACTION_SEARCH}" != "true" ]]; then
  echo "警告: Transaction Search 未开启，且 ENABLE_TRANSACTION_SEARCH != true。" >&2
  echo "      批量评估将无法发现会话。请手动开启或设置 ENABLE_TRANSACTION_SEARCH=true。" >&2
else
  echo "    未开启，正在开启(账户级设置，按索引的 span 量计费)..."

  # 2a. 允许 X-Ray 写入 aws/spans 与 Application Signals 日志组
  POLICY_NAME="TransactionSearchXRayAccess"
  POLICY_DOC="$(python3 - "${REGION}" "${ACCOUNT_ID}" <<'PY'
import json, sys
region, account = sys.argv[1], sys.argv[2]
print(json.dumps({
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "TransactionSearchXRayAccess",
    "Effect": "Allow",
    "Principal": {"Service": "xray.amazonaws.com"},
    "Action": "logs:PutLogEvents",
    "Resource": [
      f"arn:aws:logs:{region}:{account}:log-group:aws/spans:*",
      f"arn:aws:logs:{region}:{account}:log-group:/aws/application-signals/data:*",
    ],
    "Condition": {
      "StringEquals": {"aws:SourceAccount": account},
      "ArnLike": {"aws:SourceArn": f"arn:aws:xray:{region}:{account}:*"},
    },
  }],
}))
PY
)"
  aws logs put-resource-policy \
    --policy-name "${POLICY_NAME}" \
    --policy-document "${POLICY_DOC}" \
    --region "${REGION}" >/dev/null
  echo "    已配置 CloudWatch Logs 资源策略: ${POLICY_NAME}"

  # 2b. 把 trace segment 目的地切到 CloudWatch Logs
  aws xray update-trace-segment-destination \
    --destination CloudWatchLogs --region "${REGION}" >/dev/null
  echo "    已将 trace segment 目的地切换为 CloudWatchLogs"

  # 2c. 索引采样率(默认 100%，可用 INDEXING_PERCENTAGE 降低成本)
  if [[ -n "${INDEXING_PERCENTAGE:-}" ]]; then
    aws xray update-indexing-rule \
      --name Default \
      --rule "{\"Probabilistic\":{\"DesiredSamplingPercentage\":${INDEXING_PERCENTAGE}}}" \
      --region "${REGION}" >/dev/null
    echo "    已设置索引采样率: ${INDEXING_PERCENTAGE}%"
  fi

  # 等待生效
  for _ in $(seq 1 12); do
    TS_STATUS="$(aws xray get-trace-segment-destination --region "${REGION}" \
      --query 'Status' --output text 2>/dev/null || echo UNKNOWN)"
    [[ "${TS_STATUS}" == "ACTIVE" ]] && break
    sleep 5
  done
  echo "    Transaction Search 状态: ${TS_STATUS}"
  if [[ "${TS_STATUS}" != "ACTIVE" ]]; then
    echo "警告: 状态尚未变为 ACTIVE，可能需要几分钟。首次运行若发现 0 个会话，请稍后重跑。" >&2
  fi
fi

# ---------------------------------------------------------------------------
# 3a. 构建 boto3 Lambda layer(必需)
#
# Lambda python3.12 自带的 boto3 不认识 bedrock-agentcore 的批量评估/推荐 API
# (start_batch_evaluation / start_recommendation)，直接调用会报
# AttributeError: 'BedrockAgentCore' object has no attribute ...
# 所以这里打一个当前版本的 boto3 层覆盖掉内置版本。
# ---------------------------------------------------------------------------
echo ""
echo "==> 构建 boto3 Lambda layer(AgentCore 新 API 需要)"
command -v pip3 >/dev/null 2>&1 || python3 -m pip --version >/dev/null 2>&1 || {
  echo "错误: 未找到 pip，无法构建 boto3 层。" >&2; exit 1; }
BOTO_DIR="$(mktemp -d)"
trap 'rm -rf "${BOTO_DIR}"' EXIT
python3 -m pip install --quiet --upgrade \
  --platform manylinux2014_x86_64 --only-binary=:all: \
  --implementation cp --python-version 3.12 \
  --target "${BOTO_DIR}/python" \
  boto3 botocore >/dev/null 2>&1 || {
    echo "错误: boto3 下载失败。" >&2; exit 1; }
BOTO_VER="$(python3 -c "
import pathlib,sys
p=[d.name for d in pathlib.Path('${BOTO_DIR}/python').glob('boto3-*.dist-info')]
print(p[0].split('-')[1] if p else 'unknown')")"
find "${BOTO_DIR}/python" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true
(cd "${BOTO_DIR}" && zip -qr boto3-layer.zip python)
echo "    boto3 ${BOTO_VER} ($(( $(wc -c < "${BOTO_DIR}/boto3-layer.zip") / 1024 / 1024 )) MB)，正在发布 ..."
BOTO3_LAYER_ARN="$(aws lambda publish-layer-version \
  --layer-name "${STACK_NAME}-boto3" \
  --description "boto3 ${BOTO_VER} with bedrock-agentcore batch evaluation APIs" \
  --zip-file "fileb://${BOTO_DIR}/boto3-layer.zip" \
  --compatible-runtimes python3.12 \
  --region "${REGION}" \
  --query 'LayerVersionArn' --output text)"
echo "    ${BOTO3_LAYER_ARN}"

# ---------------------------------------------------------------------------
# 3b. 构建 matplotlib Lambda layer(可选)
# ---------------------------------------------------------------------------
LAYER_ARN=""
if [[ "${BUILD_CHARTS}" == "true" ]]; then
  echo ""
  echo "==> 构建 matplotlib Lambda layer(用于生成图表)"
  if ! command -v pip3 >/dev/null 2>&1 && ! python3 -m pip --version >/dev/null 2>&1; then
    echo "    跳过: 未找到 pip。图表将被跳过，JSON/CSV/HTML 仍会生成。"
  else
    BUILD_DIR="$(mktemp -d)"
    trap 'rm -rf "${BOTO_DIR}" "${BUILD_DIR}"' EXIT
    if python3 -m pip install \
         --platform manylinux2014_x86_64 --only-binary=:all: \
         --implementation cp --python-version 3.12 \
         --target "${BUILD_DIR}/python" \
         matplotlib >/dev/null 2>&1; then
      # 去掉打包无用的体积
      find "${BUILD_DIR}/python" -name "tests" -type d -prune -exec rm -rf {} + 2>/dev/null || true
      find "${BUILD_DIR}/python" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true
      (cd "${BUILD_DIR}" && zip -qr layer.zip python)
      SIZE_MB=$(( $(wc -c < "${BUILD_DIR}/layer.zip") / 1024 / 1024 ))
      echo "    layer.zip 构建完成 (${SIZE_MB} MB)，正在发布 ..."
      LAYER_ARN="$(aws lambda publish-layer-version \
        --layer-name "${STACK_NAME}-matplotlib" \
        --description "matplotlib for Connect AgentCore evaluation charts" \
        --zip-file "fileb://${BUILD_DIR}/layer.zip" \
        --compatible-runtimes python3.12 \
        --region "${REGION}" \
        --query 'LayerVersionArn' --output text)"
      echo "    ${LAYER_ARN}"
    else
      echo "    跳过: matplotlib 轮子下载失败。图表将被跳过，JSON/CSV/HTML 仍会生成。"
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 4. 部署 CloudFormation 栈
# ---------------------------------------------------------------------------
echo ""
echo "==> 部署 CloudFormation 栈: ${STACK_NAME}"

# Lambda 代码需要先打包上传，用 CloudFormation 托管的暂存桶
PACKAGE_BUCKET="cf-templates-${ACCOUNT_ID}-${REGION}-agentcore-eval"
if ! aws s3api head-bucket --bucket "${PACKAGE_BUCKET}" --region "${REGION}" 2>/dev/null; then
  echo "    创建打包暂存桶: ${PACKAGE_BUCKET}"
  if [[ "${REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "${PACKAGE_BUCKET}" --region "${REGION}" >/dev/null
  else
    aws s3api create-bucket --bucket "${PACKAGE_BUCKET}" --region "${REGION}" \
      --create-bucket-configuration "LocationConstraint=${REGION}" >/dev/null
  fi
  aws s3api put-public-access-block --bucket "${PACKAGE_BUCKET}" --region "${REGION}" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" >/dev/null
  aws s3api put-bucket-encryption --bucket "${PACKAGE_BUCKET}" --region "${REGION}" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' >/dev/null
fi

PACKAGED="${SCRIPT_DIR}/.packaged.yaml"
aws cloudformation package \
  --template-file "${SCRIPT_DIR}/template.yaml" \
  --s3-bucket "${PACKAGE_BUCKET}" \
  --s3-prefix "${STACK_NAME}" \
  --output-template-file "${PACKAGED}" \
  --region "${REGION}" >/dev/null
echo "    代码已打包"

PARAMS=(
  "ConnectLogGroup=${CONNECT_LOG_GROUP}"
  "ObservabilityRuntimeName=${OBSERVABILITY_RUNTIME_NAME}"
  "ResultsBucketName=${BUCKET}"
  "LookbackHours=${LOOKBACK_HOURS}"
  "MaxAgeDays=${MAX_AGE_DAYS}"
  "OverlapMinutes=${OVERLAP_MINUTES}"
  "SettleMinutes=${SETTLE_MINUTES}"
  "Evaluators=${EVALUATORS}"
  "Insights=${INSIGHTS}"
  "RecommendationEvaluator=${RECOMMENDATION_EVALUATOR}"
  "ScheduleExpression=${SCHEDULE}"
  "ScheduleEnabled=${SCHEDULE_ENABLED}"
  "LogRetentionDays=${LOG_RETENTION_DAYS}"
  "LedgerTtlDays=${LEDGER_TTL_DAYS}"
  "RequireSpanIndex=${REQUIRE_SPAN_INDEX}"
  "IndexBudgetSeconds=${INDEX_BUDGET_SECONDS}"
  "IndexMaxAttempts=${INDEX_MAX_ATTEMPTS}"
  "MaxSessionsPerRun=${MAX_SESSIONS_PER_RUN}"
  "ShiftOlderThanDays=${SHIFT_OLDER_THAN_DAYS}"
  "ArchiveBucket=${ARCHIVE_BUCKET}"
  "MatplotlibLayerArn=${LAYER_ARN}"
  "Boto3LayerArn=${BOTO3_LAYER_ARN}"
)

aws cloudformation deploy \
  --template-file "${PACKAGED}" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides "${PARAMS[@]}" \
  --region "${REGION}" \
  --no-fail-on-empty-changeset

# ---------------------------------------------------------------------------
# 5. 输出
# ---------------------------------------------------------------------------
echo ""
echo "==> 部署完成，栈输出:"
aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table

RESULTS_PREFIX="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='ResultsPrefix'].OutputValue | [0]" \
  --output text)"

cat <<EOF

===================================================================
部署完成!

立即跑一次(不必等定时):
  ./run-now.sh --stack ${STACK_NAME} --region ${REGION}

下载结果:
  aws s3 sync ${RESULTS_PREFIX} ./results/ --region ${REGION}

每次运行会在 ${RESULTS_PREFIX}<runId>/ 下生成:
  index.html              一页汇总(评分表 + 图表 + 用户意图)
  summary.json            机器可读汇总
  eval_scores.csv         每条评分 + LLM 解释
  charts/*.png            热力图与总览图
  recommendation/*.md     优化后的系统提示词与工具描述
  sessions/*.json         转换后的 OTEL spans
  collect.json            本次窗口读了什么、跳过了什么(及原因)
===================================================================
EOF
