#!/usr/bin/env bash
#
# undeploy.sh
#
# 删除评估流水线。默认保留结果 S3 桶与已写入的日志(数据不会被误删)。
#
# 用法:
#   ./undeploy.sh [--instance <key>] [--stack <name>] [--region <region>] [--delete-results] [--yes]
#
# 参数:
#   --instance <key>  按 ../cloudfront/config-*.json 的 connect.<key> 取区域与 AWS profile
#   --delete-results  同时清空并删除结果 S3 桶(不可恢复)
#   --yes             跳过确认
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config.env"
[[ -f "${CONFIG_FILE}" ]] && source "${CONFIG_FILE}"

STACK_NAME="${STACK_NAME_CONF:-connect-agentcore-eval}"
REGION=""
INSTANCE_ARG=""
DELETE_RESULTS="false"
ASSUME_YES="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)       INSTANCE_ARG="$2"; shift 2 ;;
    --stack)          STACK_NAME="$2"; shift 2 ;;
    --region)         REGION="$2"; shift 2 ;;
    --delete-results) DELETE_RESULTS="true"; shift ;;
    --yes|-y)         ASSUME_YES="true"; shift ;;
    -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
[[ -n "${REGION}" ]] || REGION="$(aws configure get region 2>/dev/null || echo us-east-1)"

BUCKET="$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='ResultsBucket'].OutputValue | [0]" \
  --output text 2>/dev/null || echo "")"

echo "将删除栈: ${STACK_NAME} (region ${REGION})"
if [[ "${DELETE_RESULTS}" == "true" && -n "${BUCKET}" && "${BUCKET}" != "None" ]]; then
  echo "并将清空删除结果桶: ${BUCKET}  <-- 不可恢复"
else
  echo "结果桶将保留: ${BUCKET:-<未知>}"
fi

if [[ "${ASSUME_YES}" != "true" ]]; then
  read -r -p "确认? [y/N] " ans
  [[ "${ans}" =~ ^[Yy]$ ]] || { echo "已取消。"; exit 0; }
fi

if [[ "${DELETE_RESULTS}" == "true" && -n "${BUCKET}" && "${BUCKET}" != "None" ]]; then
  echo "==> 清空 ${BUCKET}(含历史版本) ..."
  aws s3 rm "s3://${BUCKET}" --recursive --region "${REGION}" >/dev/null || true
  python3 - "${BUCKET}" "${REGION}" <<'PY'
import sys
import boto3
bucket, region = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", region_name=region)
paginator = s3.get_paginator("list_object_versions")
for page in paginator.paginate(Bucket=bucket):
    objs = [{"Key": v["Key"], "VersionId": v["VersionId"]}
            for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
    if objs:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
print("versions purged")
PY
  aws s3api delete-bucket --bucket "${BUCKET}" --region "${REGION}" || true
fi

echo "==> 删除栈 ..."
aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" \
  --region "${REGION}" || true
echo "完成。"

cat <<EOF

注意: 以下资源不属于本栈，未被删除:
  - Connect 日志投递配置(由上一级 setup_connect_ai_agent_logs.py 创建)
  - CloudWatch Transaction Search(账户级设置)。如需关闭:
      aws xray update-trace-segment-destination --destination XRay --region ${REGION}
  - aws/spans 日志组中已索引的 span 数据(按保留期自然过期)
  - AgentCore 评估结果日志组 /aws/bedrock-agentcore/evaluations/...
EOF
