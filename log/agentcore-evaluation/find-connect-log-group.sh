#!/usr/bin/env bash
#
# 找出 Connect AI Agent 日志实际投递到哪个 CloudWatch 日志组，用于填写
# ../cloudfront/config-*.json 的 ai_log_arn(或 config.env 的 CONNECT_LOG_GROUP)。
#
# 为什么需要这个脚本：
#   `aws logs describe-deliveries` 会列出账号里**全部**投递关系，而且
#   deliveryDestinationArn 只是投递目标的名字（形如
#   arn:aws:logs:...:delivery-destination:xxx），**不含日志组名**。日志组名要
#   再调一次 get-delivery-destination 才能从 destinationResourceArn 里取到。
#
# 筛选锚点与上游 setup_connect_ai_agent_logs.py 保持一致：投递源的判定条件是
# logType=EVENT_LOGS 且 resourceArn 指向 Q in Connect (Wisdom) assistant
# (arn:aws:wisdom:...:assistant/...)，而不是靠名字猜——投递源可能是别人建的、
# 叫别的名字（控制台建的常叫 connect-assistant-delivery-source，而上游默认名是
# connect-ai-agent-delivery-source-<InstanceAlias>）。
#
# 用法:
#   ./find-connect-log-group.sh --instance <key>
#   ./find-connect-log-group.sh [--region us-east-1] [--instance-id <connect-instance-id>]
#
# --instance 从 ../cloudfront/config-*.json 的 connect.<key>.instance_arn 取区域、
# 实例 ID 与 AWS profile。不指定实例时列出该区域所有 Connect AI Agent 投递；
# 指定了则只看该实例关联的 assistant。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION=""
INSTANCE_ID=""
INSTANCE_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)    INSTANCE_ARG="$2"; shift 2 ;;
    --region)      REGION="$2"; shift 2 ;;
    --instance-id) INSTANCE_ID="$2"; shift 2 ;;
    -h|--help)     sed -n '2,28p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

if [[ -n "${INSTANCE_ARG}" ]]; then
  # shellcheck source=instance-config.sh
  source "${SCRIPT_DIR}/instance-config.sh"
  load_instance "${INSTANCE_ARG}"
  REGION="${REGION:-${INSTANCE_REGION}}"
fi
REGION="${REGION:-${AWS_REGION:-us-east-1}}"

# 可选：把 Connect 实例收窄到它自己的 assistant，避免多实例账号看花眼
WANT_ASSISTANT=""
if [[ -n "${INSTANCE_ID}" ]]; then
  WANT_ASSISTANT="$(aws connect list-integration-associations \
    --instance-id "${INSTANCE_ID}" --region "${REGION}" \
    --query "IntegrationAssociationSummaryList[?IntegrationType=='WISDOM_ASSISTANT'].IntegrationArn | [0]" \
    --output text 2>/dev/null || true)"
  if [[ -z "${WANT_ASSISTANT}" || "${WANT_ASSISTANT}" == "None" ]]; then
    echo "实例 ${INSTANCE_ID} 上没找到 WISDOM_ASSISTANT 集成关联。" >&2
    echo "该实例可能未启用 Q in Connect / AI Agent。" >&2
    exit 1
  fi
  echo "==> 实例 ${INSTANCE_ID} 的 assistant: ${WANT_ASSISTANT}"
fi

# 1) 找投递源：EVENT_LOGS + 指向 wisdom assistant。名字不可靠，按 ARN 判定。
#    describe-delivery-sources 的输出用 text 逐行读，比在 JMESPath 里写
#    contains() 嵌套更好懂，也不受某些字段为 None 的干扰。
SOURCES="$(aws logs describe-delivery-sources --region "${REGION}" \
  --query "deliverySources[?logType=='EVENT_LOGS'].[name,resourceArns[0]]" \
  --output text 2>/dev/null | grep ':wisdom:' || true)"

if [[ -n "${WANT_ASSISTANT}" ]]; then
  SOURCES="$(printf '%s\n' "${SOURCES}" | grep -F "${WANT_ASSISTANT}" || true)"
fi

if [[ -z "${SOURCES}" ]]; then
  echo "未找到任何 Connect AI Agent 的日志投递（logType=EVENT_LOGS 且指向 wisdom assistant）。" >&2
  echo "说明上游方案还没跑过，请先执行 ../setup_connect_ai_agent_logs.py。" >&2
  exit 1
fi

FOUND_CWL=""
while read -r SRC_NAME ASSISTANT_ARN; do
  [[ -z "${SRC_NAME}" ]] && continue
  echo ""
  echo "==> 投递源: ${SRC_NAME}"
  echo "    assistant: ${ASSISTANT_ARN}"

  # 2) 该投递源下的所有投递关系。一个源可以有多个目标（S3 + 日志组并存很常见）。
  DELIVERIES="$(aws logs describe-deliveries --region "${REGION}" \
    --query "deliveries[?deliverySourceName=='${SRC_NAME}'].[deliveryDestinationType,deliveryDestinationArn]" \
    --output text 2>/dev/null || true)"

  if [[ -z "${DELIVERIES}" ]]; then
    echo "    !! 投递源存在但没有投递关系，日志不会落到任何地方。" >&2
    continue
  fi

  while read -r DTYPE DARN; do
    [[ -z "${DTYPE}" ]] && continue
    DEST_NAME="${DARN##*:delivery-destination:}"
    # 3) 只有再解一层才拿到真正的目标资源（日志组名 / S3 桶名）
    TARGET="$(aws logs get-delivery-destination --name "${DEST_NAME}" \
      --region "${REGION}" \
      --query "deliveryDestination.deliveryDestinationConfiguration.destinationResourceArn" \
      --output text 2>/dev/null || echo "")"

    case "${DTYPE}" in
      CWL)
        # arn:aws:logs:<r>:<acct>:log-group:<name>:*  ->  <name>
        LG="${TARGET#*:log-group:}"; LG="${LG%:\*}"
        echo "    [CloudWatch] 日志组: ${LG}"
        FOUND_CWL="${LG}"
        ;;
      S3)
        echo "    [S3] ${TARGET}   (本流水线不使用，只读 CloudWatch)"
        ;;
      *)
        echo "    [${DTYPE}] ${TARGET}"
        ;;
    esac
  done <<< "${DELIVERIES}"
done <<< "${SOURCES}"

echo ""
if [[ -z "${FOUND_CWL}" ]]; then
  echo "投递源存在，但没有指向 CloudWatch 日志组的投递（只有 S3 等其他目标）。" >&2
  echo "本流水线读 CloudWatch，请执行 ../setup_connect_ai_agent_logs.py 补一条 CWL 投递。" >&2
  exit 1
fi

# 顺手确认日志组里真的有 AI Agent trace，否则流水线会收集到 0 个会话
echo "==> 检查日志组近 24 小时是否有 TRANSCRIPT_AI_AGENT_TRACE ..."
HAS_TRACE="$(aws logs filter-log-events --log-group-name "${FOUND_CWL}" \
  --region "${REGION}" --filter-pattern '"TRANSCRIPT_AI_AGENT_TRACE"' \
  --start-time "$(( ($(date +%s) - 86400) * 1000 ))" --max-items 1 \
  --query 'length(events)' --output text 2>/dev/null || echo 0)"
if [[ "${HAS_TRACE}" == "0" || "${HAS_TRACE}" == "None" ]]; then
  echo "    近 24 小时没有 trace 事件。投递配置本身没问题，但需要先在 Connect 里"
  echo "    产生一次 AI Agent 对话（投递配置不会回溯历史通话）。"
else
  echo "    有 trace 事件，可以直接跑流水线。"
fi

ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo '<account>')"
echo ""
echo "把这一行写进 ../cloudfront/config-${ACCOUNT}.json 对应实例的 ai_log_arn:"
echo "\"ai_log_arn\": \"arn:aws:logs:${REGION}:${ACCOUNT}:log-group:${FOUND_CWL}:*\""
echo ""
echo "或者(不用 --instance 时)写进 config.env:"
echo "CONNECT_LOG_GROUP=\"${FOUND_CWL}\""
