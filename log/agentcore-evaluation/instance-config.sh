# shellcheck shell=bash
#
# instance-config.sh
#
# 被 deploy.sh / run-now.sh / undeploy.sh / diagnose-index-gate.sh /
# find-connect-log-group.sh source，不单独执行。
#
# 让本流水线和上一级方案共用同一份 cloudfront/config-*.json:
#   --instance <key>(或 config.env 里的 CONNECT_INSTANCE)选中 connect.<key>，
#   从 instance_arn 取区域 / 账号 / 实例 ID，从 ai_log_arn 取日志组名。
#
# 多个 config-*.json 的合并规则与 setup_connect_ai_agent_logs.py 一致: 按文件名
# 排序依次读入，同名 key 以靠后的文件为准。
#
# 凭证也沿用上一级的约定: 存在名为「账号 ID」的 AWS profile 且没有显式设置
# AWS_PROFILE 时，自动用它；无论用哪套凭证，都校验它确实属于 ARN 里的账号。

INSTANCE_CONFIG_DIR="${INSTANCE_CONFIG_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/cloudfront}"

# load_instance <key>
#   设置 INSTANCE_KEY INSTANCE_ARN INSTANCE_REGION INSTANCE_ACCOUNT INSTANCE_ID
#        INSTANCE_LOG_GROUP(ai_log_arn 为空时为空) INSTANCE_LOG_REGION
load_instance() {
  local key="$1" vars
  vars="$(python3 - "${INSTANCE_CONFIG_DIR}" "${key}" <<'PY'
import json, re, shlex, sys
from pathlib import Path

cfg_dir, key = Path(sys.argv[1]), sys.argv[2]
files = sorted(cfg_dir.glob("config-*.json"))
if not files:
    sys.exit(f"错误: {cfg_dir} 下没有 config-*.json")
merged = {}
for f in files:
    merged.update(json.loads(f.read_text(encoding="utf-8")).get("connect", {}))
if key not in merged:
    sys.exit(f"错误: 实例 '{key}' 不在 {cfg_dir}/config-*.json 中。可选: {', '.join(merged) or '<无>'}")
entry = merged[key]

m = re.match(r"^arn:aws:connect:([a-z0-9-]+):(\d{12}):instance/([0-9a-fA-F-]+)$",
             (entry.get("instance_arn") or "").strip())
if not m:
    sys.exit(f"错误: connect.{key}.instance_arn 不是合法的 Connect 实例 ARN: {entry.get('instance_arn')!r}")
region, account, instance_id = m.groups()

log_group, log_region = "", ""
ai_log_arn = (entry.get("ai_log_arn") or "").strip()
if ai_log_arn:
    lm = re.match(r"^arn:aws:logs:([a-z0-9-]+):(\d{12}):log-group:(.+?)(:\*)?$", ai_log_arn)
    if not lm:
        sys.exit(f"错误: connect.{key}.ai_log_arn 不是合法的日志组 ARN: {ai_log_arn!r}")
    log_region, log_account, log_group = lm.group(1), lm.group(2), lm.group(3)
    if log_account != account:
        sys.exit(f"错误: connect.{key} 的 ai_log_arn 账号({log_account})与 instance_arn 账号({account})不一致")

for name, value in [("INSTANCE_KEY", key), ("INSTANCE_ARN", m.group(0)),
                    ("INSTANCE_REGION", region), ("INSTANCE_ACCOUNT", account),
                    ("INSTANCE_ID", instance_id), ("INSTANCE_LOG_GROUP", log_group),
                    ("INSTANCE_LOG_REGION", log_region or region)]:
    print(f"{name}={shlex.quote(value)}")
PY
)" || exit 1
  eval "${vars}"

  if [[ -z "${AWS_PROFILE:-}" ]] && aws configure list-profiles 2>/dev/null | grep -qx "${INSTANCE_ACCOUNT}"; then
    export AWS_PROFILE="${INSTANCE_ACCOUNT}"
  fi
  local actual
  actual="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  if [[ "${actual}" != "${INSTANCE_ACCOUNT}" ]]; then
    echo "错误: 当前凭证(profile=${AWS_PROFILE:-<默认>})属于账号 '${actual:-<无法获取>}'，" >&2
    echo "      而实例 ${INSTANCE_KEY} 在账号 ${INSTANCE_ACCOUNT}。" >&2
    echo "      请配置名为 ${INSTANCE_ACCOUNT} 的 AWS profile(见 ../DEPLOYMENT.md「配置 profile」)，" >&2
    echo "      或设置 AWS_PROFILE 指向该账号。" >&2
    exit 1
  fi
  echo "==> 实例 ${INSTANCE_KEY}: ${INSTANCE_ARN} (profile=${AWS_PROFILE:-<默认>})"
}

# resolve_instance_log_group
#   ai_log_arn 没填时，按 setup_connect_ai_agent_logs.py 的命名规则
#   /aws/connect/ai-agent-logs-<InstanceAlias> 推导日志组名。
resolve_instance_log_group() {
  [[ -n "${INSTANCE_LOG_GROUP}" ]] && return 0
  local alias
  alias="$(aws connect describe-instance --instance-id "${INSTANCE_ID}" \
    --region "${INSTANCE_REGION}" --query 'Instance.InstanceAlias' --output text 2>/dev/null || true)"
  if [[ -z "${alias}" || "${alias}" == "None" ]]; then
    echo "错误: connect.${INSTANCE_KEY}.ai_log_arn 为空，且无法通过 DescribeInstance 取得 InstanceAlias" >&2
    echo "      来推导日志组名。请在 config 里填写 ai_log_arn，或在 config.env 里设置 CONNECT_LOG_GROUP。" >&2
    exit 1
  fi
  INSTANCE_LOG_GROUP="/aws/connect/ai-agent-logs-${alias}"
  echo "    ai_log_arn 未填写，按命名规则推导日志组: ${INSTANCE_LOG_GROUP}"
}
