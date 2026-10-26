# Connect AI Agent 日志自动评估流水线（AgentCore Evaluation / Insights / Recommendation）

在上一级 [`setup_connect_ai_agent_logs.py`](../setup_connect_ai_agent_logs.py)（或 Connect 控制台）已经把 Connect AI Agent 日志投递到 CloudWatch 日志组（上一级脚本建的是 `/aws/connect/ai-agent-logs-<InstanceAlias>`）的基础上，本目录提供**一键部署**的全自动流水线：

```
Connect AI Agent 日志组
        │  ① 定时拉取（默认每天一次，回看 24h）
        ▼
   格式转换（Connect 日志 → OTEL GenAI span/event）
        │  ② 写入 AgentCore Observability
        ▼
/aws/bedrock-agentcore/runtimes/<name>-DEFAULT  +  aws/spans
        │  ③ 自动调用 AgentCore
        ▼
 Evaluation（13 个内置评估器） │ Insights │ Recommendation
        │  ④ 汇总
        ▼
   S3 结果桶（JSON + CSV + PNG 图表 + index.html），客户自行下载
```

| 文件 | 说明 |
|------|------|
| [`deploy.sh`](./deploy.sh) | 一键部署（含自动开启 Transaction Search、构建出图 layer），幂等可重复执行 |
| [`run-now.sh`](./run-now.sh) | 立即手动跑一次，不等定时调度 |
| [`find-connect-log-group.sh`](./find-connect-log-group.sh) | 查出 Connect 日志实际投递到哪个日志组，用于填 `ai_log_arn` / `CONNECT_LOG_GROUP` |
| [`diagnose-index-gate.sh`](./diagnose-index-gate.sh) | 状态机以 `LogEventsNotIndexed` 失败时的只读诊断 |
| [`instance-config.sh`](./instance-config.sh) | 各脚本共用：`--instance <key>` 从 `../cloudfront/config-*.json` 读取实例配置 |
| [`undeploy.sh`](./undeploy.sh) | 删除流水线，默认保留结果桶 |
| [`config.env.example`](./config.env.example) | 配置模板，`cp` 成 `config.env` 后按需修改 |
| [`template.yaml`](./template.yaml) | CloudFormation 模板（S3 + DynamoDB + 6 个 Lambda + Step Functions + 定时调度 + 告警）|
| [`src/`](./src) | Lambda 代码，见下方「流水线七步」|

---

## 前置条件

1. **AWS CLI v2**、**bash**，凭证具备部署权限（CloudFormation / IAM / Lambda / Step Functions / S3 / DynamoDB / Logs / X-Ray / bedrock-agentcore）。凭证约定与上一级相同：用 `--instance` 时，若存在名为**账号 ID** 的 AWS profile（且没有显式设置 `AWS_PROFILE`）就自动使用它，配置方法见 [`../DEPLOYMENT.md`](../DEPLOYMENT.md) 第 2 节；无论用哪套凭证，脚本都会校验它属于实例 ARN 里的账号。
2. **python3**；如需自动出图，还需 `pip`（用于构建 matplotlib Lambda layer，失败会自动降级为只出 JSON/CSV）。
3. Connect 日志组里**确实有** `TRANSCRIPT_AI_AGENT_TRACE` 事件（`./find-connect-log-group.sh --instance <key>` 会顺带检查近 24 小时）。
4. 区域需支持 Amazon Bedrock AgentCore Evaluation。Insights 目前为**公开预览**。流水线部署在**日志组所在区域**（不是上一级 CloudFront 站点固定使用的 us-east-1）。

---

## 用法

与上一级共用 `../cloudfront/config-*.json`：`--instance` 后面跟 `connect` 下的实例 key，脚本从 `instance_arn` 取区域 / 账号 / 实例 ID，从 `ai_log_arn` 取日志组名（`ai_log_arn` 为空时按上一级的命名规则用 `/aws/connect/ai-agent-logs-<InstanceAlias>`）。`email` / `hours` / `gateway_log_arn` 本流水线不使用。

```bash
cd log/agentcore-evaluation
cp config.env.example config.env      # 可选：不改也能用默认值；把 CONNECT_INSTANCE 写进去就不用每次带 --instance

./deploy.sh --instance connect-repair             # 一键部署（会先打印参数并要求确认）
./run-now.sh --instance connect-repair --hours 48 # 立即回溯 48 小时跑一次，验证端到端
```

不用 `config-*.json` 也可以：不带 `--instance`，在 `config.env` 里直接写 `AWS_REGION` 与 `CONNECT_LOG_GROUP`。

> 一个账号 + 区域里只能直接部署一套：结果桶名 `connect-agentcore-eval-<account>-<region>` 与运行时日志组名不含实例 key。同一账号同一区域的第二个实例要另起一套时，用 `--stack` 换栈名、在 `config.env` 里换 `OBSERVABILITY_RUNTIME_NAME`，并用 `--bucket` 指定一个已存在的结果桶。

部署完成后会打印结果桶与下载命令，例如：

```bash
aws s3 sync s3://connect-agentcore-eval-<account>-<region>/runs ./results
open results/<runId>/index.html
```

### deploy.sh 参数

| 参数 | 说明 |
|------|------|
| `--instance <key>` | 从 `../cloudfront/config-*.json` 读取该实例的区域、账号与日志组（等同 `config.env` 的 `CONNECT_INSTANCE`）|
| `--config <file>` | 指定配置文件（默认 `./config.env`）|
| `--region <region>` / `--stack <name>` / `--bucket <name>` | 覆盖区域 / 栈名 / 结果桶 |
| `--schedule "<cron>"` | 覆盖调度表达式，如 `"cron(0 19 * * ? *)"` |
| `--no-schedule` | 只部署，不启用定时（之后用 `run-now.sh` 手动跑）|
| `--no-charts` | 跳过 matplotlib layer 构建（只出 JSON/CSV）|
| `--yes` / `-y` | 跳过确认 |

### run-now.sh 参数

`run-now.sh` / `undeploy.sh` / `diagnose-index-gate.sh` 同样支持 `--instance <key>`，用来取区域与 AWS profile。

| 参数 | 说明 |
|------|------|
| `--hours <n>` | 本次回看小时数，覆盖栈默认值（首次回溯建议用它）|
| `--max-sessions <n>` | 本次最多评估多少个会话（0 = 不限），覆盖栈里的 `MAX_SESSIONS_PER_RUN`。**首次回溯时务必带上**，它是单次运行成本的上限 |
| `--archive <s3://bucket/prefix>` | **仅用于验证**：不读日志组，改读该前缀下的 NDJSON 归档回放历史日志，见「用归档日志验证」|
| `--force` | 忽略去重台账，重新评估已评估过的会话（**会重复产生评估费用**）|
| `--no-wait` | 只触发不等待 |

---

## 定时运行

调度由 CloudFormation 里的 `AWS::Scheduler::Schedule`（EventBridge Scheduler）管理，**跟着栈一起创建**，不需要单独操作。默认**每天 03:00 UTC 跑一次、回看 24 小时**。

### 三个参数

| 配置项（`config.env`）| 默认值 | 说明 |
|------|--------|------|
| `SCHEDULE_EXPRESSION` | `cron(0 3 * * ? *)` | 调度表达式，**时区固定为 UTC** |
| `SCHEDULE_ENABLED_CONF` | `true` | `false` 则完全不创建调度资源，只能用 `run-now.sh` 手动跑 |
| `LOOKBACK_HOURS` | `24` | 每次运行回看多少小时。**要和调度频率匹配**，见下 |

表达式支持 `cron(...)` 和 `rate(...)` 两种（EventBridge Scheduler 语法，cron 是 6 字段、含年）：

```bash
SCHEDULE_EXPRESSION="cron(0 3 * * ? *)"      # 每天 03:00 UTC（默认）
SCHEDULE_EXPRESSION="cron(0 19 * * ? *)"     # 每天 19:00 UTC = 北京时间次日 03:00
SCHEDULE_EXPRESSION="cron(0 */6 * * ? *)"    # 每 6 小时一次
SCHEDULE_EXPRESSION="cron(0 4 ? * MON *)"    # 每周一 04:00 UTC
SCHEDULE_EXPRESSION="rate(12 hours)"         # 每 12 小时
```

> **`LOOKBACK_HOURS` 必须 ≥ 调度间隔**，否则两次运行之间会有一段日志谁都不读，形成盲区。改成每 6 小时跑就把 `LOOKBACK_HOURS` 设为 `6`（或更大，重叠不会重复计费，见「定时拉取会不会重/漏？」）。反过来把间隔放大到一周则要设 `LOOKBACK_HOURS="168"`。

### 三种配置方式

优先级：**命令行参数 > 环境变量 > `config.env`**。

```bash
# 方式 1（推荐）：写进 config.env，deploy.sh 每次都读它
SCHEDULE_EXPRESSION="cron(0 19 * * ? *)"
SCHEDULE_ENABLED_CONF="true"
LOOKBACK_HOURS="24"

# 方式 2：命令行临时覆盖
./deploy.sh --schedule "cron(0 19 * * ? *)"
./deploy.sh --no-schedule          # 只部署，不定时

# 方式 3：用另一个配置文件（多环境）
./deploy.sh --config ./config.prod.env
```

> ⚠️ **不要用 `SCHEDULE_ENABLED_CONF=true ./deploy.sh` 这种前置环境变量的写法**——`deploy.sh` 是先解析命令行、再 `source config.env`，所以 `config.env` 里的同名值会把它覆盖掉。要临时改就用 `--schedule` / `--no-schedule`，或换 `--config`。

### 改调度 / 开关调度

都通过重新部署完成，`deploy.sh` 幂等，可反复执行：

```bash
./deploy.sh --schedule "cron(0 19 * * ? *)" --yes   # 改时间
./deploy.sh --no-schedule --yes                     # 关掉定时（删除调度资源）
./deploy.sh --yes                                   # 恢复到 config.env 里的设置
```

也可以不重新部署、只临时暂停（栈下次部署会覆盖回去）：

```bash
aws scheduler update-schedule --name connect-agentcore-eval-daily \
  --region us-east-1 --state DISABLED \
  --schedule-expression "cron(0 3 * * ? *)" --flexible-time-window Mode=OFF \
  --target "$(aws scheduler get-schedule --name connect-agentcore-eval-daily \
              --region us-east-1 --query Target --output json)"
```

### 确认调度生效

```bash
aws scheduler list-schedules --region us-east-1 \
  --query "Schedules[?contains(Name,'connect-agentcore')].{name:Name,state:State}" --output table

# 最近几次自动运行（source 为 schedule 的即定时触发）
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:us-east-1:<account>:stateMachine:connect-agentcore-eval-pipeline \
  --region us-east-1 --max-items 10 \
  --query "executions[].{name:name,status:status,start:startDate}" --output table
```

怎么区分定时运行和手动运行：调度传入的是 `{"source": "schedule"}`，不带 runId，所以 Step Functions 的执行名是一串自动生成的 UUID，S3 里的 runId 形如 `20260804T030000Z`（**纯时间戳、无前缀**）；`run-now.sh` 则会显式传 `manual-<时间戳>`，两者一眼可辨。定时运行的回看小时数取栈里的 `LOOKBACK_HOURS`（`run-now.sh --hours` 只影响那一次手动运行）。

失败时 EventBridge Scheduler 的 `RetryPolicy` 会自动重试 2 次。

> ⚠️ 栈里的 `PipelineFailureAlarm` 目前**只进 ALARM 状态、不发通知**（没有配 `AlarmActions`）。要让定时任务坏掉时能收到消息，得自己挂一个 SNS 主题，否则只能靠翻 S3 或 Step Functions 控制台才发现：
>
> ```bash
> TOPIC=$(aws sns create-topic --name connect-agentcore-eval-alerts \
>          --region us-east-1 --query TopicArn --output text)
> aws sns subscribe --topic-arn "$TOPIC" --protocol email \
>          --notification-endpoint you@example.com --region us-east-1
> aws cloudwatch put-metric-alarm --alarm-name connect-agentcore-eval-pipeline-failed \
>          --region us-east-1 --alarm-actions "$TOPIC" \
>          --namespace AWS/States --metric-name ExecutionsFailed \
>          --dimensions Name=StateMachineArn,Value=<StateMachineArn> \
>          --statistic Sum --period 3600 --evaluation-periods 1 \
>          --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold \
>          --treat-missing-data notBreaching
> ```
>
> 注意这是栈外的手工改动，下次 `deploy.sh` 会把告警覆盖回没有通知的状态。

---

## 从零复现一次端到端测试

用一通**新的** Connect 通话验证整条链路。顺序很关键：**日志投递配置不会回溯**，所以先确保投递已就绪，再打电话。

> ⚠️ **先用下面第 0 步确认投递是否已存在，只有查不到时才运行上一级的 `setup_connect_ai_agent_logs.py`。** 该脚本会复用已有的投递源，但总是新建一个 `/aws/connect/ai-agent-logs-<InstanceAlias>` 日志组和指向它的投递目标；如果这个 Connect 实例已经有人配过投递（可能是控制台配的，日志组形如 `/aws/connect/wisdom/<assistantId>`，也可能同时投到 S3），再跑一次就会给同一个投递源多加一条投递关系，造成日志重复与双份投递费用。这种情况下直接把已有的日志组填进 `ai_log_arn`（或 `CONNECT_LOG_GROUP`）即可，本流水线读哪个日志组都一样。

```bash
# 0. 确认投递已存在，并直接得到该填进 config-*.json 的 ai_log_arn
./find-connect-log-group.sh --instance connect-repair
# 不用 config-*.json 时：--region <region> [--instance-id <connect-instance-id>]
```

输出形如：

```
==> 投递源: connect-assistant-delivery-source
    assistant: arn:aws:wisdom:us-east-1:<acct>:assistant/<assistantId>
    [S3] arn:aws:s3:::connect-ai-agent-logs-<acct>   (本流水线不使用，只读 CloudWatch)
    [CloudWatch] 日志组: /aws/connect/wisdom/<assistantId>
==> 检查日志组近 24 小时是否有 TRANSCRIPT_AI_AGENT_TRACE ...
    有 trace 事件，可以直接跑流水线。

把这一行写进 ../cloudfront/config-<acct>.json 对应实例的 ai_log_arn:
"ai_log_arn": "arn:aws:logs:us-east-1:<acct>:log-group:/aws/connect/wisdom/<assistantId>:*"
```

> 别直接用 `aws logs describe-deliveries`：它会列出账号里**全部**投递关系，而且 `deliveryDestinationArn` 只是投递目标的名字，**不含日志组名**——日志组名要再调一次 `get-delivery-destination`，从 `destinationResourceArn` 里取。上面的脚本按上游同样的锚点（`logType=EVENT_LOGS` + resourceArn 指向 wisdom assistant）筛选，然后把这层解开。注意**不能按名字猜**投递源：控制台建的源常叫 `connect-assistant-delivery-source`，而上一级脚本默认建的名字是 `connect-ai-agent-delivery-source-<InstanceAlias>`。

| 步骤 | 做什么 | 验证点 |
|------|--------|--------|
| 1 | 记下当前 UTC 时间：`date -u`（后面用它算 `--hours`）| |
| 2 | 在 Connect 里与 AI Agent 完整对话一次：**要触发工具调用**（问 FAQ、报修等），并让通话**正常结束**（不要中途挂断）| 未触发工具则 `ToolSelectionAccuracy` 无数据；未正常结束则第 1 步按 SETTLED 规则跳过 |
| 3 | 等 2-5 分钟，确认日志已投递：<br>`aws logs tail <日志组> --since 10m --region <region> \| grep TRANSCRIPT_AI_AGENT_TRACE \| head` | 必须看到 `TRANSCRIPT_AI_AGENT_TRACE`。只有 `TRANSCRIPT_UTTERANCE` 说明 trace 还没落盘，继续等 |
| 4 | 再等到通话结束满 `SETTLE_MINUTES`（默认 10 分钟），然后跑：<br>`./run-now.sh --instance <key> --hours 2` | **不要加 `--force`**：正常跑一遍就该选中这个新会话；若显示 `already_evaluated` 说明选错了会话 |
| 5 | 下载结果：<br>`aws s3 sync s3://connect-agentcore-eval-<account>-<region>/runs/<runId> ./results/<runId>`<br>`open results/<runId>/index.html` | `summary.json` 里 `sessionsSelected: 1`、评估任务 `COMPLETED`、`failedSessions: 0` |

一次成功的运行大约 8-15 分钟（索引等待 + 13 个评估器 + Insights）。中途失败不用从头来：已转换的会话仍在 `s3://<bucket>/runs/<runId>/sessions/`，直接重跑 `run-now.sh` 即可，去重台账只在**发布成功后**才写。

排查时按步骤看 S3 里对应的 JSON：`collect.json`（选中/跳过原因）→ `ingest.json`（span 数、失败数）→ `eval.json`（任务状态与逐会话错误）。Step Functions 控制台的执行图能直接定位到失败的那个 Lambda。

---

## 流水线七步

Step Functions 状态机按顺序编排以下 Lambda（`src/` 下同名文件）：

| 步骤 | 文件 | 做什么 |
|------|------|--------|
| 1. Collect | `collect.py` | Logs Insights 拉取窗口内的 Connect 日志（单次查询撞到 10000 行上限就把窗口对半切开重查，直到读全），按会话转换，选出「已结束、未评估、未超期」的会话写入 S3，并按 `MAX_SESSIONS_PER_RUN` 截取最新的 N 个；同时按 **AI agent** 分别**完整**抽取系统提示词与工具描述供第 6 步使用 |
| 2. Ingest | `ingest.py` | span 经 SigV4 签名发往 X-Ray OTLP 端点（`/v1/traces`），event 用 `PutLogEvents` 写入 AgentCore Observability 日志组；最早事件超过 `SHIFT_OLDER_THAN_DAYS`（默认 1 天）的会话整体平移到当前时间，见「时间戳平移」|
| 3. WaitIndexed | `wait_indexed.py` | 等待 `aws/spans` 的 span 文档与运行时日志组的事件**都**被 Logs Insights 索引（两者索引时间不同，各用各的查询窗口），必要时以新时间戳重发仍缺失的事件 |
| 4/5. Evaluate / Insights | `evaluate.py` | `StartBatchEvaluation` 并轮询；评估器超过 10 个时自动拆成多个并发任务 |
| 6. Recommend | `recommend.py` | `StartRecommendation`：**按 AI agent 各起一对任务**——系统提示词优化 + 工具描述优化（inline `sessionSpans` 形式，只喂该 agent 自己的 span）|
| 7. Publish | `publish.py` | 读回评估结果事件，出 CSV + 热力图/总览图 + `summary.json` + `index.html`，最后写去重台账 |

第 4/5/6 步在 `Parallel` 状态中**并发**执行（已验证同账号允许多个批量评估任务同时进行）；Insights 与 Recommendation 各自带 `Catch`，单个环节失败不会拖垮整条流水线。

`converter.py` 是从最初的参考实现移植过来的转换器，含若干修正。每一条都以「评分会失真」为代价，改动前后都用同一个真实会话跑过对比：

- **`system_instructions` 是 MESSAGE 对象列表**，提示词文本位于 `values[i].text.value`；此前按列表项直接取 `value` 会得到空字符串，`Builtin.InstructionFollowing` 因此失真。
- **`input_messages` 是逆序的**（`output_messages` 是正序）。按原样读取会把对话倒着喂给评估器。现在按每条 MESSAGE 自带的 `timestamp` 排序。
- **一次助手回答被拆成多条 MESSAGE**：可见回复、推理（`reasoning`）、每个工具调用各一条。推理那条没有 text，被当成独立消息时整轮就以一条空的 `<NO_RESPONSE>` 结尾，质量类评估器于是判定「助手没有回答」。现在推理不再单独成条，回复与工具调用合并回同一轮。
- **部分 agent 会多出一条「累计」客户消息**（把此前所有客户话语用空格连起来），读起来像用户侧被复制了一份，Insights 的意图聚类里就是那些越来越长的字符串。现在按内容识别并去掉。
- **AGENT span 的 `input.messages` 必须以用户消息结尾**：评估器从末尾读该轮的 user query，否则整个会话以 `AgentSpanMappingException: Failed to parse user_query from agent-span` 失败。逆序读取时这一点是碰巧成立的（最老的消息排在最后，而最老的消息是客户话语）。现在显式截到最后一条用户消息；agent 自己发起的一轮（语音坐席的问候、"还在吗"）补一条 `<EMPTY_USER_INPUT>` 占位。

同一会话（10 轮、6 次工具调用）修正前后的对比：

| 评估器 | 修正前 | 修正后 |
|--------|-------:|-------:|
| `Builtin.Correctness` | 0.000 | 1.000 |
| `Builtin.GoalSuccessRate` | 0.000 | 1.000 |
| `Builtin.ResponseRelevance` | 0.000 | 1.000 |
| `Builtin.Helpfulness` | 0.034 | 0.735 |
| `Builtin.InstructionFollowing` | 0.800 | 1.000 |
| `Builtin.Conciseness` | 0.700 | 0.500 |
| `Builtin.ToolSelectionAccuracy` / `ToolParameterAccuracy` | 1.000 / 0.833 | 1.000 / 0.833 |

修正前那些 0 分不是「没评上」，而是**评错了**：评估器拿到的候选回复是 `<NO_RESPONSE>`（30 个 turn 里有 20 个），解释里写的就是 "the assistant provided no actual response"。`Conciseness` 下降是正常的——它现在评的是真实回复（含寒暄）而不是空消息。

> 批量评估作业只要「会话」跑完就报 `COMPLETED`，即使其中大部分 span 因为映射失败没打上分。所以 `summary.json` 与 `index.html` 里会显式列出 `evaluationsFailed`（按评估器统计带 `error.message` 的结果条数）与 `evaluationErrors`（去重后的错误原文），并在页面顶部标红——一页看着合理、实际只覆盖了少数 span 的结果，是最容易被误信的那种。作业自己的 `totalFailed` 计数放在 `evaluatorFailedCounts` 里：工具类评估器会把**没有调用过工具**的会话也计成 failed，那不是缺陷，所以不用它驱动红色横幅。

### 日志格式适配

同一个 Connect 日志组里会混着好几种 AI agent 与两代日志格式。转换器（`converter.py`）按**会话**判定格式，`collect.json` 的 `sessionFormats` 与每个会话的 `format` 字段记录判定结果：

| 格式 | 判定 | 转换方式 |
|------|------|----------|
| `trace` | 会话含 `TRANSCRIPT_AI_AGENT_TRACE` | 直接使用 Connect 记录的 span 树 |
| `legacy` | 没有 TRACE，但有 `TRANSCRIPT_AGENTIC_MESSAGE` / `TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION`（`prompt_type=ORCHESTRATION`）| 从每次模型调用的完整 Converse 风格 prompt + completion 重建：每个客户轮次一个 `invoke_agent`，每次模型调用一个 `chat`，在后续 prompt 的 `toolResult` 里找到结果的每次工具调用一个 `execute_tool` |
| `unsupported` | 只有 `CREATE_SESSION`、只有老式 `SELF_SERVICE_MESSAGE`、或只有知识库单次问答等非对话记录 | 跳过，列在 `skipped.unsupported` |

TRACE 格式本身也不是一种形状。在一个跑了 40 多个 AI agent 的真实账号里逐一对过：

- **agent-assist / 分析类 agent**（如 `AmazonConnectRAPIDCore`）的 inference span **没有 `operation_name`**，工具 span 的 `operation_name` 里放的是**工具名**（`get_widget_data`、`render_table`...），`span_name` 才说明它是 `execute_tool`。所以 span 类型以 `span_name` 为准，`operation_name` 只做回退；此前按 `operation_name` 取值会直接 `KeyError` 让整次运行崩掉。
- 这类 agent 的消息**多包了一层**：一条 MESSAGE 的 text 值本身是一个 `[{participant, timestamp, text}]` 的 JSON 列表；工具的参数与结果是纯文本。现在先展开再处理。
- **提问者不一定是 CUSTOMER**：agent-assist 场景里向 AI agent 提问的是人工坐席，participant 是 `AGENT`。现在只有 `BOT` 一侧算 assistant，其余都算 user；否则坐席的提问会被当成助手发言，user query 变成占位符。
- **不属于任何会话的联络**（manual search 等）全部记在全零 `session_id` 下，几周内的几十个联络挤在一个"会话"里。现在按 span 里的 `contact_id`（或调用它的 `request_id`）拆成独立会话，id 形如 `contact-<id>` / `request-<id>`。
- 同一会话可以经过**多个 agent**（自助服务通话中夹着坐席侧查询）；每个 span 记自己的 `gen_ai.agent.name`，子 span 没有名字时继承根 span 的。`summary.json` 的 `sessionInfo` / `agentsEvaluated` 记录每个会话经过了哪些 agent。
- `status=ERROR` 的 span（barge-in 打断、超时）带 `error_type` 与 `status_description`，现在映射成 OTEL 的 `ERROR` 状态并保留描述；没有任何输出的错误 span 与评估无关，按「没有可评估内容」处理，否则评估器报 "error status with insufficient data"。
- agent 自己发起的模型调用（语音 agent 的问候）没有输入消息，chat span 同样补 `<EMPTY_USER_INPUT>` 占位，否则 Insights 的失败分析里会出现 `SpanMappingException - Empty Messages Array`。
- 字段值带尾部空格（`ai_agent_name=Foo  `）统一 strip。

### 时间戳平移

批量评估只能发现**最近**的 span 文档。实测（us-west-2，2026-09-20）：同一批会话，span 时间戳在 13 天前时所有评估器都报

```
Provided input contains N log event(s) but no span documents. Log events alone
cannot be evaluated - the corresponding span documents are required. Please
ensure that transaction search is enabled and retry.
```

而 span 文档明明已在 `aws/spans` 里、Logs Insights 能查到；把同一批数据平移到当前时间再跑，13 个评估器全部正常出分。这条报错里的 "transaction search" 是误导，X-Ray 索引采样率（1% 还是 100%）对它没有影响。

所以 `ingest.py` 会把**最早一条事件**早于 `SHIFT_OLDER_THAN_DAYS`（默认 1 天）的会话整体平移到当前时间（保留会话内部的相对时序；一个会话跨度超过 13 天时最老的那部分会被钳到窗口边缘），原始时间仍在 `collect.json` 和 `sessions/*.json` 里。每天调度、评估昨天通话的正常运行不会触发平移。以会话**最早**而不是最新事件判定，是因为跨过 14 天边界的会话同样会被 `PutLogEvents` 以 `tooOldLogEventEndIndex` 拒收。

### 用归档日志验证

正式部署永远只读 Connect 日志组。但日志组保留期短（本例 3 天）时，很难攒够会话验证一次部署；上一级的 `cloudfront/setup_cloudfront.py` 每次部署都会把 AI Agent 日志归档到 `s3://connect-ai-logs-<account>/raw/<实例key>/connect.ndjson`（每行 `{"timestamp","message"}`，正是这里要的格式），可以直接回放：

```bash
# config.env
ARCHIVE_BUCKET="connect-ai-logs-<account>"  # 只是给 collect 步骤授只读权限
MAX_AGE_DAYS="90"                          # 归档里的会话早于 14 天，靠时间戳平移写入

./deploy.sh --instance <key> --yes
./run-now.sh --instance <key> --archive s3://connect-ai-logs-<account>/raw/<key>/connect.ndjson \
  --hours 900 --max-sessions 10
```

注意前缀要写到 `connect.ndjson`：同一目录下还有 `gateway.ndjson`（AgentCore Gateway 日志），`--archive` 会读前缀下所有 `.ndjson`。该桶的 `raw/` 前缀 7 天后自动过期，要回放更早的数据需先重跑 `setup_cloudfront.py`（调大 `hours`）重新归档。

`--archive` 只影响这一次运行；不带它的运行和定时运行照常读日志组。去重台账对两种来源一视同仁。

---

## 结果文件

一次运行的产物都在 `s3://<bucket>/runs/<runId>/` 下：

| 路径 | 内容 |
|------|------|
| `index.html` | **从这里看起**：本次运行总览，内嵌图表与结论 |
| `summary.json` | 机器可读总览：窗口、会话数、各评估器均值、跳过的会话、意图/失败模式、任务状态 |
| `eval_scores.csv` | 每条评估的分数、标签与解释（逐会话逐评估器）|
| `charts/eval_overview.png` | 各评估器均值总览图 |
| `charts/eval_scores.png` | 会话 × 评估器 热力图 |
| `recommendation/<agent>/system_prompt.md` | 该 AI agent 推荐的系统提示词 + 理由（或服务返回的错误，例如提示词被 prompt-attack 保护拦下）|
| `recommendation/<agent>/tool_description.md` | 该 AI agent 各工具的推荐描述 + 理由 |
| `collect.json` / `ingest.json` / `eval.json` / `insights.json` / `recommendation.json` | 各步骤的原始输出，排查时用。`collect.json` 里有每个会话的格式、经过的 agent、以及各类跳过原因 |
| `sessions/<sessionId>.json` | 转换后的 sessionSpans（喂给 AgentCore 的原始输入）|
| `agents/<agent>/system_prompt.txt` | 从日志中抽取的该 agent 的完整系统提示词 |
| `agents/<agent>/tool_descriptions.json` | 该 agent 的工具描述（TRACE 格式取自提示词里的 `<toolConfiguration>`，legacy 格式取自 prompt 的 `toolConfig`）|

> 结果桶开启了版本控制与 `DenyInsecureTransport`，栈删除时默认 `Retain`。**日志与结果包含客户真实对话内容，请按内部数据合规要求处置。**

---

## 定时拉取会不会重/漏？

「每天一次、只读 24 小时」听起来刚好，但边界情况会既漏又重。流水线用三条规则兜住：

1. **OVERLAP**：实际窗口是 `LOOKBACK_HOURS + OVERLAP_MINUTES`（默认多读 30 分钟）。跨窗口边界的会话（23:58 接入、00:03 结束），以及窗口关闭时投递还没落盘的最后几分钟，都会被下一次运行捞回来，而不是被静默截断。
2. **SETTLED**：只有「最后一个事件已早于 `SETTLE_MINUTES`（默认 10 分钟）」的会话才评估。否则会对进行中的通话按半截对话打分——那是**错的分数**，比缺失的分数更危险。
3. **LEDGER**：每个已评估的 `sessionId` 记入 DynamoDB 台账（TTL 默认 30 天）。所以第 1 条带来的重叠、以及手动重跑，都不会重复评估、不会重复计费。台账在**发布成功之后**才写，失败的运行下次仍可重试。

另外，超过 **`MAX_AGE_DAYS`**（默认 14 天）的会话会被显式跳过并在 `collect.json` 的 `skipped.too_old` 中列出。被 `MAX_SESSIONS_PER_RUN` 截掉的会话列在 `skipped.deferred`，**不写台账**，只要下一次运行的窗口还盖得住就会被补上；所以把上限设得很小、又只每天跑一次，回看窗口之外的那部分就会永远评不到——上限是成本闸门，不是抽样手段。


## 排错：状态机以 `LogEventsNotIndexed` 结束

WaitIndexed 连续 5 次都判定「数据还没被索引」时，状态机会以 `LogEventsNotIndexed` 失败。
一条命令把该看的都打出来（只读，不改任何资源）：

```bash
./diagnose-index-gate.sh                      # 自动取最近一次失败执行
./diagnose-index-gate.sh --run-id <runId>     # 或指定某次运行
```

它回答三个问题：部署的 `wait_indexed` 是不是带索引门修复的版本（看有没有 `REQUIRE_SPAN_INDEX`）、
卡住的是 `aws/spans` 的 span 文档还是运行时日志组的 log event（逐轮打印两半各缺多少）、
以及数据是「没到」还是「到了但查询看不见」（对同一个 spanId 跑原始文本 / parse / 字段三种查询对比）。
最后还会打印 Transaction Search 的 destination 与采样率。

下面是它背后的手工做法——**先分清两种情况**：数据真的没到，还是判定用的查询看不见已经到了的数据：

```bash
REGION=<region>
# 取一个本次注入的 spanId（ingest.json 里有）
SID=<spanId>

# A. 原始文本里在不在？(不依赖字段自动发现)
aws logs start-query --region "$REGION" --log-group-name aws/spans \
  --start-time $(( $(date +%s) - 7200 )) --end-time $(date +%s) \
  --query-string "fields @message | filter @message like /$SID/ | limit 5"

# B. 作为字段能不能过滤？
aws logs start-query --region "$REGION" --log-group-name aws/spans \
  --start-time $(( $(date +%s) - 7200 )) --end-time $(date +%s) \
  --query-string "fields spanId | filter spanId = '$SID' | limit 5"
```

用 `aws logs get-query-results --query-id <id>` 取结果，看 `statistics`：

- **A 和 B 都是 0**：span 文档确实还没进 `aws/spans`。检查 Transaction Search 是否已启用
  （`aws xray get-trace-segment-destination`，应为 `CloudWatchLogs` + `ACTIVE`）、`INDEXING_PERCENTAGE`
  是否被调得过低、以及 ingest 步骤的 OTLP 调用是否真的返回 200。等待并重跑即可。
- **A 有、B 是 0**：数据在库里，但 Logs Insights 没把 `spanId` 提升为可过滤字段（曾在
  eu-central-1 遇到，`recordsScanned` 有值而 `recordsMatched` 恒为 0）。这一半的门永远打不开，
  等多久都没用。`wait_indexed.py` 现在用 `parse @message` 从原始文本里抽 `spanId` 来判定，
  不再依赖字段自动发现，所以这种情况已被覆盖；请确认 Lambda 代码是当前版本（重跑 `deploy.sh`）。
  若某个区域仍然打不开这一半门，可以只跳过它：

  ```bash
  # config.env
  REQUIRE_SPAN_INDEX="false"
  ```

  跳过后运行时日志组那一半仍然要等（它决定 `LogEventMissingException`），span 这一半交给评估作业
  去报真实结果，而不是卡在门上超时。

判定查询本身也可以直接在本机验证——它就是 `wait_indexed.py` 里的 `PARSE_QUERY`：

```
parse @message /"spanId":\s*"(?<sid>[0-9a-fA-F]{16})"/ | filter ispresent(sid) | stats count(*) by sid | limit 10000
```

`aws/spans` 由 X-Ray 写入、是紧凑 JSON（`"spanId":"..."`），运行时日志组由本流水线用
`json.dumps` 写入、冒号后有空格（`"spanId": "..."`），所以模式里的 `\s*` 不能去掉。

---

## 成本提示

主要成本项是**评估本身**：`每条 trace × 每个评估器` 各一次 LLM 调用。默认开启全部 13 个内置评估器（不含需要 ground truth 的 `Trajectory*Match`），话务量大时请：

- 用 `MAX_SESSIONS_PER_RUN`（或 `run-now.sh --max-sessions`）给单次运行设会话数上限，这是最直接的闸门；
- 用 `EVALUATORS` 只保留关心的几个，例如
  `EVALUATORS="Builtin.Helpfulness,Builtin.GoalSuccessRate,Builtin.ToolSelectionAccuracy"`；
- 用 `INSIGHTS=""` 关掉 Insights；
- 下调 `INDEXING_PERCENTAGE` 控制 Transaction Search 的索引采样成本（已验证采样率不影响本流水线出结果）。

参考量级：40 个会话、234 个 span、13 个评估器 + 2 个 Insights，一次运行产生 1112 条评估。

Lambda / Step Functions / DynamoDB / S3 的开销相对可以忽略。

---

## 卸载

```bash
./undeploy.sh                    # 删栈，保留结果桶
./undeploy.sh --delete-results   # 同时清空并删除结果桶（不可恢复）
```

不属于本栈、不会被删除的：上一级 `setup_connect_ai_agent_logs.py` 创建的 Connect 日志投递配置与上一级 CloudFront 站点的全部资源、账户级的 Transaction Search 设置、`aws/spans` 中已索引的 span（按保留期自然过期）、AgentCore 评估结果日志组。`undeploy.sh` 结束时会把这些连同关闭命令一起打印出来。
