# setup_connect_ai_agent_logs.py 使用说明

## 1. 脚本作用

`setup_connect_ai_agent_logs.py` 为 Amazon Connect 实例的 AI Agent(Q in Connect / Wisdom assistant)配置 **CloudWatch Logs 日志投递**。脚本从 `cloudfront/config-*.json` 中读取 Connect 实例 ARN,让用户选择一个实例后,自动完成「投递源 → 日志组 → 投递目标 → 投递关系」的创建,使该实例的 AI Agent 事件日志(`EVENT_LOGS`)以 JSON 格式写入 CloudWatch 日志组。

### 创建/配置的 AWS 资源

以下名称中的 `<InstanceAlias>` 由脚本调用 Connect `DescribeInstance` 取得(即实例别名)。

| 资源 | 名称 / 说明 |
| --- | --- |
| 日志投递源(Delivery Source) | `connect-ai-agent-delivery-source-<InstanceAlias>`(名称中的 `_` 会被替换为 `-`);`logType=EVENT_LOGS`,资源为该实例关联的 Wisdom assistant ARN。若已存在指向同一 assistant 的投递源,则直接复用其现有名称 |
| CloudWatch 日志组 | `/aws/connect/ai-agent-logs-<InstanceAlias>` |
| 日志投递目标(Delivery Destination) | `connect-ai-agent-delivery-destination-<InstanceAlias>`;输出格式 `json`,目标为上述日志组(`arn:aws:logs:<region>:<account_id>:log-group:<日志组名>:*`) |
| 日志投递关系(Delivery) | 把投递源与投递目标关联起来 |

### 执行流程(与代码顺序一致)

1. 扫描 `cloudfront/` 目录下所有 `config-*.json`,合并其中的 `connect` 字典,列出可选实例,交互式输入实例名。
2. 用实例 ARN 解析出 region、account-id、instance-id(ARN 格式不合法则报错退出)。
3. 以 **account-id 作为 AWS profile 名**创建 boto3 Session,调用 `DescribeInstance` 取 `InstanceAlias`。
4. 调用 `ListIntegrationAssociations`(`IntegrationType=WISDOM_ASSISTANT`)找到 Wisdom assistant ARN;找不到则报错退出。
5. 创建或复用投递源(`PutDeliverySource`)。
6. 创建日志组,已存在则跳过。
7. 创建/更新投递目标(`PutDeliveryDestination`)。
8. 创建投递关系(`CreateDelivery`),已存在则跳过。

## 2. 前置条件

- **Python**:`python3`。代码使用了 `tuple[str, str, str]` 这类类型注解写法,需要 Python 3.9 及以上(此为根据语法的推断,代码未声明最低版本)。
- **依赖包**:`boto3`(`pip install boto3`)。
- **AWS 凭证**:本机 `~/.aws/config` / `credentials` 中必须存在一个**名称等于实例 ARN 中账号 ID 的 profile**(脚本固定使用 `profile_name=<account_id>`)。区域取自实例 ARN,无需额外设置。
- **权限**:该 profile 需要能调用脚本中用到的 API:
  - Amazon Connect:`DescribeInstance`、`ListIntegrationAssociations`
  - CloudWatch Logs:`DescribeDeliverySources`、`PutDeliverySource`、`CreateLogGroup`、`PutDeliveryDestination`、`DescribeDeliveries`、`CreateDelivery`
- **需要预先存在的资源**:Connect 实例已存在,并已在 Connect 控制台为该实例启用 AI agent / Q in Connect(即存在 `WISDOM_ASSISTANT` 集成关联),否则脚本报错退出。
- **配置文件**:`cloudfront/config-<account_id>.json`(至少一个)。脚本只读取其中的 `connect` 字段:

| 字段 | 说明 |
| --- | --- |
| `connect` | 字典,key 为实例名(运行时输入的就是这个 key) |
| `connect.<key>.instance_arn` | Connect 实例 ARN,格式 `arn:aws:connect:<region>:<account_id>:instance/<instance-id>`。脚本从中解析区域、账号 ID 和实例 ID |

config 中的其他字段(如 `email`、`hours`、`ai_log_arn`、`gateway_log_arn`)由 `cloudfront/setup_cloudfront.py` 使用,本脚本不读取。示例(占位符):

```json
{
    "connect": {
        "<实例key>": {
            "instance_arn": "arn:aws:connect:<region>:<account_id>:instance/<instance-id>"
        }
    }
}
```

## 3. 如何执行

脚本**不接收命令行参数**(文件顶部 docstring 中提到的 `<amazon-connect-instance-arn>` 参数写法与实际入口不符,入口代码不读取 `sys.argv`),而是交互式运行:

```bash
cd log
python3 setup_connect_ai_agent_logs.py
```

运行后:

1. 终端打印 `Available instances:` 及所有实例名(来自 `config-*.json` 的 `connect` 的 key,多个文件会合并,同名 key 以文件名排序靠后者为准)。
2. 在 `Enter instance name:` 提示后输入其中一个实例名;输入不存在的名称会提示并要求重新输入。
3. 脚本打印 `Loaded instance ARN: ...` 后开始配置。

退出条件:`cloudfront/` 下没有 `config-*.json`,或所有文件中都没有 `connect` 实例,脚本会输出错误并以非零状态退出。

### 幂等性 / 重复执行

脚本可重复执行:

- 投递源:已有指向同一 assistant ARN 的投递源则复用,否则创建。
- 日志组:已存在(`ResourceAlreadyExistsException`)则跳过。
- 投递目标:`PutDeliveryDestination` 为创建或更新。
- 投递关系:已存在相同「投递源 + 投递目标」的关系则跳过。

## 4. 执行后的产出

结束时终端打印汇总:投递源名称、投递目标名称、日志组名称(及区域),并给出查看日志的命令。产生真实会话事件后可执行:

```bash
aws logs tail "/aws/connect/ai-agent-logs-<InstanceAlias>" --region <region> --follow
```

脚本不会在本地生成或修改任何文件。

## 5. 与 cloudfront/setup_cloudfront.py 的关系

本脚本是上游:先用它建立 AI Agent 日志投递并创建日志组 `/aws/connect/ai-agent-logs-<InstanceAlias>`,再把该日志组 ARN 填入 config 的 `ai_log_arn`,由 `cloudfront/setup_cloudfront.py` 部署日志分析站点。两个脚本读取同一批 `cloudfront/config-*.json`,详见 [cloudfront/README.md](cloudfront/README.md)。

## 6. 注意事项

- 获取 `InstanceAlias` 失败(例如 profile 不存在、无权限)时,脚本仅打印 `instance_name find error` 后直接返回,不会输出具体原因,也不会创建任何资源。
- 查询已有投递源/投递关系时,若调用返回 `ClientError`,脚本会忽略该错误并继续创建;另外代码只读取一次 `describe_*` 的返回结果,未做翻页处理。
- 脚本**不提供**删除/清理功能,如需移除上述资源需自行在 AWS 中删除。
- `config-*.json` 含邮箱与账号信息,请勿提交到公开仓库。
