# CloudFront 部署指南

本文档说明如何把 Connect AI Agent 日志分析站点部署到 CloudFront(Cognito 登录鉴权),并记录了一次实际部署的参数与结果。部署脚本是 `cloudfront/setup_cloudfront.py`。

> 文中的账号 ID、邮箱、ARN、资源 ID 和域名都是脱敏后的示例值(如账号 `123456789012`、邮箱 `user@example.com`),使用时请换成你自己的。

## 1. 部署内容

脚本会在 **us-east-1** 创建以下资源。账号后缀用实例 ARN 里的账号 ID。

| 类型 | 名称(以账号 123456789012 为例) |
| --- | --- |
| S3 日志桶 | `connect-ai-logs-<account>` |
| S3 Web 桶 | `connect-ai-logs-<account>-web`(只允许 CloudFront OAC 访问) |
| DynamoDB 表 | `connect-ai-contacts-<account>`,另有一张表 |
| 解析 Lambda | `connect-ai-logs-<account>-splitter`(S3 事件触发) |
| 查询 Lambda | `connect-ai-logs-<account>-query` |
| 事件解析 Lambda | `connect-ai-logs-<account>-process-events`(`events/*.json.gz` 创建事件触发,桶通知 Id `process-events-logs`;结果写入 `/aws/lambda/connect-ai-logs-<account>-process-events`,保留 30 天) |
| IAM 角色 | `…-lambda-role`(解析/查询/事件解析 Lambda 共用,内联策略 `logs-bucket-rw`、`ddb-read-and-logs`、`events-read-and-contact`),以及 Cognito 鉴权角色 |
| Cognito | 用户池、应用客户端、身份池 |
| API Gateway | `connect-ai-logs-<account>-api`(stage 为 `prod`) |
| CloudFront | 分配 + OAC(使用默认 `*.cloudfront.net` 域名) |

数据流:本机 profile 通过 `filter_log_events` 把 CloudWatch 日志拉到 S3 `raw/<实例名>/`,写入 `trigger/<实例名>.json`,S3 事件触发解析 Lambda,按 InstanceAlias 写入 DynamoDB。拉取日志的是运行脚本的本机身份,Lambda 不读 CloudWatch。解析 Lambda 写出的每个 `events/<instanceName>/<contactId>.json.gz` 再触发事件解析 Lambda `connect-ai-logs-<account>-process-events`:用 `cloudfront/lib/lambda/process_events_logs.py` 内置的会话解析逻辑解析 faqSearch 调用,附上 DescribeContact 渠道和 GetContactAttributes 属性,每个文件输出一行 JSON 到它自己的 CloudWatch 日志。

## 2. 前置条件

- Python 3 和 `boto3`。
- AWS CLI。
- 一个名为 **账号 ID** 的 AWS profile。脚本把 profile 名固定为实例 ARN 里的账号 ID(例如 `123456789012`),找不到就会失败。
- 运行者需要能创建上面列出的资源(S3、DynamoDB、Lambda、IAM、Cognito、API Gateway、CloudFront)。另外需要:
  - `logs:FilterLogEvents`:对两个日志组。
  - `connect:DescribeInstance`、`connect:ListInstanceStorageConfigs`:对 Connect 实例。
- 能收邮件的邮箱。Cognito 会把一次性密码发到这个邮箱。

### 配置 profile

如果你的凭证在 `default` 里,可以在 `~/.aws/config` 加一个同名别名。下面的写法不会复制密钥:

```ini
[profile 123456789012]
region = us-west-2
output = json
credential_process = aws configure export-credentials --profile default --format process
```

注意:`source_profile = default` 不能用,因为它需要配合 `role_arn`,单独写会报 `NoCredentials`。

验证 profile 指向正确账号:

```bash
aws sts get-caller-identity --profile 123456789012
```

返回的 `Account` 必须等于 ARN 里的账号。

## 3. 准备三个 ARN

| 配置项 | 含义 | 本次值 |
| --- | --- | --- |
| `instance_arn` | Connect 实例 | `arn:aws:connect:us-west-2:123456789012:instance/11111111-2222-3333-4444-555555555555` |
| `ai_log_arn` | Connect AI Agent 日志组 | `arn:aws:logs:us-west-2:123456789012:log-group:/aws/connect/ai-agent-logs:*` |
| `gateway_log_arn` | AgentCore Gateway 日志组(可选) | `arn:aws:logs:us-west-2:123456789012:log-group:/aws/vendedlogs/bedrock-agentcore/gateway/APPLICATION_LOGS/my-mcp-server-gw-xxxxxxxxxx:*` |

脚本会从这些 ARN 解析 region、账号、instance-id 和日志组名,日志组 ARN 末尾的 `:*` 会自动去掉。InstanceAlias 和通话录音桶则通过 Connect API 查询。

注意事项:

- `ai_log_arn` 必须显式提供。你的日志组名 `/aws/connect/ai-agent-logs` 没有 `-<InstanceAlias>` 后缀,和 `setup_connect_ai_agent_logs.py` 创建的命名规则不一致,不能靠命名推导。
- 实例、日志组可以在与部署不同的 region(本次在 us-west-2)。部署 region 固定为 us-east-1,不要用 ARN 的 region 覆盖。
- 只支持 commercial partition(`arn:aws:`)。
- 部署前先确认日志组存在:

```bash
aws logs describe-log-groups --profile 123456789012 --region us-west-2 \
  --log-group-name-prefix /aws/connect/ai-agent-logs \
  --query 'logGroups[].logGroupName'
```

## 4. 编写 config

文件放在 `cloudfront/` 目录下。建议命名为 `config-<账号ID>.json`,脚本也能扫描 `config-*.json`。

```json
{
    "email": "user@example.com",
    "hours": 8,
    "connect": {
        "connect-repair": {
            "instance_arn": "arn:aws:connect:us-west-2:123456789012:instance/11111111-2222-3333-4444-555555555555",
            "ai_log_arn": "arn:aws:logs:us-west-2:123456789012:log-group:/aws/connect/ai-agent-logs:*",
            "gateway_log_arn": "arn:aws:logs:us-west-2:123456789012:log-group:/aws/vendedlogs/bedrock-agentcore/gateway/APPLICATION_LOGS/my-mcp-server-gw-xxxxxxxxxx:*"
        }
    }
}
```

| 字段 | 说明 |
| --- | --- |
| `email` | 登录邮箱。 |
| `hours` | 拉取最近多少小时的日志。`0` 表示全部历史。 |
| `connect` 的 key(如 `connect-repair`) | 实例名,可任取。它会成为 S3 前缀 `raw/<key>/` 和 `trigger/<key>.json`。 |

可以在 `connect` 下加多个 key 来支持多实例。注意:多个实例共用同一个 `ai_log_arn` 时,每个实例会拉到同一份日志,目前脚本不做按实例过滤。

config 里含账号信息和邮箱,不要提交到仓库。

## 5. 执行部署

```bash
cd cloudfront
python3 -u setup_cloudfront.py --config config-123456789012.json
```

可选参数:

| 参数 | 说明 |
| --- | --- |
| `--lambda-memory` | 解析 Lambda 内存,默认 1024 MB。 |
| `--csat-attr` | CSAT 属性名(也可用环境变量 `CSAT_ATTRIBUTE_KEY`)。 |
| `--out-dir` | 输出目录。 |
| `--keep` | 保留临时产物。 |

执行过程大致分这几段,CloudFront 分配通常等 3 到 10 分钟:

1. 打印部署配置(区域、账号、实例、桶名、邮箱、拉取范围);对每个实例查询 InstanceAlias 和录音桶(只读),并校验共用角色内联策略合计长度。任一失败都在创建或修改任何 AWS 资源之前退出。
2. 创建日志桶、DynamoDB 表、共用 Lambda 执行角色 `…-lambda-role` 和解析 Lambda,配置 S3 事件通知(按 Id 合并,保留其它通知配置);在拉取日志前创建/更新事件解析 Lambda `connect-ai-logs-<account>-process-events` 及其 `events/*.json.gz` 通知(Id `process-events-logs`)。查看结果:`aws logs tail /aws/lambda/connect-ai-logs-<account>-process-events --follow`。
3. 拉取 AI Agent 日志和 Gateway 日志到 S3,等待 Lambda 解析出 Contact。
4. 创建 Cognito、查询 Lambda、API Gateway,并向邮箱发送一次性密码。
5. 发布站点到 Web 桶,创建 OAC 和 CloudFront 分配,等待状态变为 Deployed。

## 6. 部署结果(本次)

| 项目 | 值 |
| --- | --- |
| 访问地址 | https://dxxxxxxxxxxxxx.cloudfront.net |
| CloudFront 分配 ID | `EXXXXXXXXXXXXX` |
| API Gateway | `https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com/prod` |
| Cognito 用户池 | `us-east-1_XXXXXXXXX` |
| Cognito 应用客户端 | `xxxxxxxxxxxxxxxxxxxxxxxxxx` |
| 实例 | `connect-repair`(InstanceAlias `my-connect-instance`) |
| 拉取数据 | AI Agent 日志 3 条,Gateway 日志 32 条(最近 8 小时) |
| 解析结果 | 3 个 Contact |

## 7. 首次登录

1. 打开访问地址。
2. 用 `email` 和邮件里的一次性密码登录。
3. 按提示设置新密码。
4. 忘记密码时,在登录页点「忘记密码」,验证码会发到该邮箱。

## 8. 重新部署与更新数据

- 重跑同一条命令会重新拉取并归档日志,再触发 Lambda 解析。已存在的 Contact 日志会被覆盖写入。
- 想看更早的数据,调大 `hours`,或设为 `0`。
- 日志桶的 `raw/` 和 `trigger/` 前缀 7 天后自动删除,解析后的数据保存在 DynamoDB。
- 从旧版本升级:旧版本额外创建了 `connect-ai-logs-<account>-query-role` 和 `connect-ai-logs-<account>-process-events-role`。重跑会把查询 Lambda 和事件解析 Lambda 切换到 `…-lambda-role`,旧角色不会被自动删除;脚本结束时若检测到它们,会在 stderr 打印清理命令。确认切换后可手动清理(先用 `list-role-policies` / `list-attached-role-policies` 查看实际策略名):

```bash
ACCT=123456789012
aws iam list-role-policies --role-name connect-ai-logs-$ACCT-query-role --profile $ACCT
aws iam list-attached-role-policies --role-name connect-ai-logs-$ACCT-query-role --profile $ACCT
aws iam delete-role-policy --role-name connect-ai-logs-$ACCT-query-role --policy-name ddb-read-and-logs --profile $ACCT
# 有托管策略时先 detach: aws iam detach-role-policy --role-name connect-ai-logs-$ACCT-query-role --policy-arn <arn> --profile $ACCT
aws iam delete-role --role-name connect-ai-logs-$ACCT-query-role --profile $ACCT
aws iam list-role-policies --role-name connect-ai-logs-$ACCT-process-events-role --profile $ACCT
aws iam list-attached-role-policies --role-name connect-ai-logs-$ACCT-process-events-role --profile $ACCT
aws iam delete-role-policy --role-name connect-ai-logs-$ACCT-process-events-role --policy-name events-read-and-contact --profile $ACCT
# 有托管策略时先 detach: aws iam detach-role-policy --role-name connect-ai-logs-$ACCT-process-events-role --policy-arn <arn> --profile $ACCT
aws iam delete-role --role-name connect-ai-logs-$ACCT-process-events-role --profile $ACCT
```

## 9. 常见问题

| 现象 | 原因与处理 |
| --- | --- |
| `NoCredentials` 或 profile 找不到 | 缺少名为账号 ID 的 profile,按第 2 节配置。 |
| 拉取日志时 `ResourceNotFound` / `AccessDenied` | 日志组名或 region 不对,或运行身份没有 `logs:FilterLogEvents`。账号需与 ARN 一致。 |
| 某实例显示 `fetch failed`,没有 Contact | Gateway 或 AI 日志拉取失败时整个实例会被跳过解析。最常见原因是日志组还不存在,比如 Gateway 日志投递没开启或还没有流量。先确认日志组存在。 |
| 实例提示 `no ai_log_arn` | config 里没填 `ai_log_arn`,该实例会被跳过解析。 |
| 页面能打开但没数据 | 拉取范围内没有日志。调大 `hours` 后重跑。 |
| 访问地址打不开 | CloudFront 状态未到 Deployed。脚本会等到部署完成才结束。 |

## 10. 已知限制

- 部署 region 固定为 us-east-1。Bedrock 调用也写死 us-east-1,不要随意改。
- 只支持 commercial partition。
- 脚本没有 ARN 格式校验。格式错误会直接抛异常,不会给出可读提示。
- 脚本没有一键卸载功能。需要清理时,按第 1 节的资源列表逐个删除。
- 查询 Lambda 和解析 Lambda 的 `connect:DescribeContact` 权限目前是通配实例(`instance/*/contact/*`),没有收窄到配置的实例。
- 三个 Lambda 共用一个执行角色,每个函数实际拥有三份内联策略的并集权限(例如查询 Lambda 也能写日志桶)。
- 共用角色的内联策略合计不能超过 IAM 上限 10,240 字符。按典型命名估算约 44 个 Connect 实例才会超限,脚本会在写入前校验并报错退出。

## 11. 相关文件

| 文件 | 说明 |
| --- | --- |
| `cloudfront/setup_cloudfront.py` | 部署脚本 |
| `cloudfront/config-example.json` | config 模板 |
| `cloudfront/README.md` | 更详细的功能说明 |
| `cloudfront/lib/fetch-to-s3.py` | 日志拉取 |
| `cloudfront/lib/lambda/handler.py` | 解析 Lambda |
| `cloudfront/lib/lambda/query_handler.py` | 查询 Lambda |
| `cloudfront/lib/lambda/process_events_logs.py` | 事件解析 Lambda(内含 AI Agent 会话 / faqSearch 解析;部署时与 `cloudfront/lib/lambda/contact_summary.py` 一起打包) |
| `.agents/tasks/cloudfront-arn-config-analysis.md` | ARN 驱动部署的分析报告 |
