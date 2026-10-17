# process_events_logs.py 说明

## 1. 概述

`process_events_logs.py` 是事件解析 Lambda `connect-ai-logs-<account_id>-process-events` 的入口:每当解析 Lambda(splitter,`handler.py`)写入一个 `events/<instanceName>/<contactId>.json.gz`,它就读取这个文件,解析其中的 AI Agent 会话与 faqSearch 调用,再用 Connect `DescribeContact` / `GetContactAttributes` 补充渠道与联系人属性,最后把结果作为**一行 JSON 打印到本函数自己的 CloudWatch 日志**。它不写 DynamoDB、不写 S3,唯一的产出就是这些日志行。

### 它是不是"分析整个 contact 的 AI Agent 和 AgentCore Gateway 日志,统计 faqSearch tool 的调用情况"?

大体是,但需要几点修正:

- **分析范围是"一个 events 文件",不是"整个 contact"。** 文件里是 splitter 归到这个 Contact 的全部行:AI Agent 日志(`source=connect`)全部在内,而 Gateway 日志只包含 splitter 能关联上的那部分(先按 ID 子串匹配,再按 ±5s 时间窗,关联不上的直接丢弃)。时间窗关联也可能把别的 Contact 的 Gateway 行算进来。
- **"整个"受同步窗口限制。** splitter 每次同步按固定回看窗口(config `hours` / `sync_log_handler` 的 `hours`)重新生成并**整体覆盖**该文件。同步发生在通话进行中,文件就只有一部分;窗口起点切过一通电话时,开头部分会缺失。本函数每次只看当前这一版文件。
- **faqSearch 统计来自三个互相独立的来源,口径不同:** ① 会话 prompt 里的 `toolUse`(得到 query、命中 hit、置信度、是否追问,工具名匹配**区分大小写**);② `execute_tool` trace span(状态、成功与否、耗时、输出预览,**不区分大小写**);③ 提到 faqSearch 的 Gateway 日志(条数、错误数,**不区分大小写**)。三者数量不保证相等。
- **它输出的不只是 faqSearch。** 每条结果还包含:每个 session 的完整对话 turns、各类日志行计数(`analysis.counts`)、与 DynamoDB 摘要同口径的 `logSummary`(`mcpToolCalls`、`faqAsk`、`faqSend`、`errors`、`escalated` 等)、DescribeContact 的渠道/时间戳/队列/坐席等信息,以及 GetContactAttributes 返回的**全部**联系人属性。

## 2. 触发方式与在整体链路中的位置

```
CloudWatch Logs (AI Agent 日志 / Gateway 日志)
   │  lib/fetch-to-s3.py
   ▼
s3://connect-ai-logs-<account_id>/raw/<实例key>/connect.ndjson, gateway.ndjson
   │  setup_cloudfront.py / sync_log_handler.py 写入 trigger/<实例key>.json
   ▼
解析 Lambda connect-ai-logs-<account_id>-splitter (handler.py)
   ├─ DynamoDB: Contact 摘要
   └─ S3: events/<instanceName>/<contactId>.json.gz   (每次同步整体覆盖)
            │  S3 ObjectCreated, 通知 Id process-events-logs (前缀 events/, 后缀 .json.gz)
            ▼
事件解析 Lambda connect-ai-logs-<account_id>-process-events (本文件 + contact_summary.py)
   ├─ S3 GetObject 读取 events 文件
   ├─ Connect DescribeContact / GetContactAttributes
   └─ print → /aws/lambda/connect-ai-logs-<account_id>-process-events (每个文件一行 JSON)
```

- 触发:日志桶 `s3:ObjectCreated:*`,过滤规则 `prefix=events/`、`suffix=.json.gz`。
- 一次调用处理事件中的所有 `Records`,单条记录出错不影响其他记录(`handler`,1030–1048 行)。
- `setup_cloudfront.py` 在拉取日志**之前**部署本函数并写好通知,所以同一次部署写入的 events 文件也会被解析。

## 3. 输入

### events 文件格式

splitter(`handler.py` 的 `_write_contact`)写入的 gzip JSON 数组,按 `(timestamp, source, message)` 排序,每项:

| 字段 | 说明 |
| --- | --- |
| `timestamp` | 毫秒时间戳(整数) |
| `message` | 原始日志消息(splitter 已用 `sanitize_message` 修复非法 JSON 转义) |
| `source` | `connect`(AI Agent 日志)或 `gateway`(AgentCore Gateway 日志) |
| `contactId` | 文件所属的 Contact ID |

对象带 `ContentEncoding=gzip`;本函数按 gzip 魔数 `1f 8b` 判断是否需要解压,明文 JSON 也能读(`_load_rows`)。内容不是 JSON 数组会报错并写入 `processingError`。

### 行如何分类

- 只处理 `timestamp` 和 `message` 都不为 `None` 的 dict 行(`_is_valid_row`);`message` 不是字符串时先 `json.dumps`。
- `source == "gateway"` 的行按 Gateway 日志处理,**其它所有行**(包括缺 `source` 的)按 AI Agent(connect)日志处理(`_scan_rows`)。

### 文件名与跳过规则

- key 必须匹配 `^events/([^/]+)/([^/]+)\.json\.gz$`,否则输出一条 `.skip` 记录,原因 `not events/<instanceName>/<contactId>.json.gz`(例如多级子目录、缺少实例目录)。S3 事件中的 key 会先做 URL 解码(`unquote_plus`)。
- `<instanceName>` 是 Connect `InstanceAlias`,用于在 `INSTANCES_META` 中查找实例 ID 和 region。
- `<contactId>` 不一定是真正的 Contact ID。splitter 在行里没有 contactId 时会退回到 AI Agent 的 `session_id`,再没有就用 `unknown`。本函数仍会解析这类文件,但只有文件名是 UUID 格式时才调用 Connect API:`unknown`(以及旧版本可能留下的 `unattached-gateway-logs`)会得到 `contactError = attributesError = "not a contact id"`。session_id 也是 UUID 格式,会照常调用 API,通常以 `ResourceNotFoundException` 写入 error 字段。当前 splitter 会直接丢弃 `unattached-gateway-logs` 和 `double-uuid-session` 两组,不会为它们写文件。

## 4. 处理步骤(对应代码分段)

| 行号 | 代码分段 | 作用 |
| --- | --- | --- |
| 1–47 | 模块 docstring | 用途、环境变量、输出类型与字段摘要 |
| 62–69 | 导入 `contact_summary` | `_parse_span`、`_short_tool_name`、`_tool_name_from_span`、`_tool_output_text`、`_tool_succeeded`、`build_contact_summary`(与 splitter 共用) |
| 72–155 | 常量与配置 | 常量、正则、boto3 客户端(按 region 缓存,重试 `max_attempts=6, mode=adaptive`)、读取 `MAX_LOG_BYTES` / `INSTANCES_META` |
| 158–221 | 通用工具 | JSON 序列化、截断、预览、错误文本格式化等 |
| 224–262 | S3 输入读取 | 解码 key、解析 `instanceName`/`contactId`、`GetObject` 并解压 |
| 265–662 | 会话解析 | `parse_agentic_message` 把 prompt 解析成对话 turns 和 `faq_events`;`dedupe_latest_by_session` 每个 session 只保留时间戳最大的一条 |
| 665–738 | faqSearch 分析 | `_faq_tool_span`(execute_tool span)、`_gateway_faq_entry`(Gateway 日志) |
| 741–880 | 日志行汇总 | `_scan_rows` 单次遍历计数并收集输入;`_parse_sessions`;`_faq_search_summary`;`_add_log_summary`;`analyze_rows` 组装 `analysis` |
| 883–932 | Connect 联系人补充 | `_describe_contact`、`_get_attributes`、`_enrich_with_contact` |
| 935–981 | 日志输出 | `_emit`:超限先截断,再分片 |
| 984–1048 | Lambda 入口 | `process_record`(单条 S3 记录 → result dict)、`handler` |

单条记录的执行顺序(`process_record`,988–1027 行):

1. 从 S3 事件取 bucket、key、eTag、versionId、sequencer、size、eventName、eventTime,建立 result 骨架。
2. `_load_rows` 读取文件 → `analyze_rows`;这一步的任何异常都写入 `processingError`,不会中断。
3. 不管第 2 步是否成功,都执行 `_enrich_with_contact` 补充 Connect 信息。
4. `handler` 调用 `_emit` 输出。

## 5. faqSearch 分析的三个来源

工具名在不同实例中带不同前缀(如 `uat___faqSearch`、`agentcore_mcp_..._gateway___faqSearch`),所以统一按子串 `faqSearch` 匹配。

| 来源 | 输入 | 匹配方式 | 输出位置 |
| --- | --- | --- | --- |
| ① 会话 prompt | `TRANSCRIPT_AGENTIC_MESSAGE` 的 `prompt`;没有时退回 `TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION` | `toolUse.name` 包含 `faqSearch`,**区分大小写** | `analysis.transcript.sessions[].faq_events`;汇总为 `analysis.faqSearch.transcriptCalls` / `transcriptHits` |
| ② trace span | 任何带 `span_name=execute_tool` span 的 connect 行(实际主要是 `TRANSCRIPT_AI_AGENT_TRACE`) | 完整工具名匹配 `faqSearch`,**不区分大小写** | `analysis.faqSearch.toolSpans` / `toolSpanCount` / `toolSpanSuccess` |
| ③ Gateway 日志 | `source=gateway` 的行 | 消息文本匹配 `faqSearch`,**不区分大小写** | `analysis.faqSearch.gateway` |

另外,`analysis.logSummary.faqAsk` / `faqSend` 由 `build_contact_summary` 按**短工具名**(最后一个 `___` 之后的部分)不区分大小写统计 execute_tool span,口径接近来源 ②。

### ① 会话 prompt:`faq_events`

prompt 中的 messages 是累积快照,所以先逐条解析,再按 session 分组(`session_id`,没有则用 system-variables 里的 `sessionId`,再没有用 `contactId`),每组只保留 `event_timestamp` 最大的一条(行内没有 `event_timestamp` 时用日志行的 `timestamp`)。单条消息结构异常只丢弃这一条。

session 字段:`session_id`、`contact_id`、`instance_id`、`bot_language`(来自 system prompt 的 `<system-variables>` 块)、`event_timestamp`、`turns`(`[{role: CUSTOMER|BOT, text}]`,BOT 只取 `<message>` 内容,去掉 `<thinking>`)、`faq_events`。

每个 faqSearch `toolUse` 生成一条 `faq_event`:

| 字段 | 含义 |
| --- | --- |
| `query` | toolUse.input 里的 `query` |
| `original_question` | 该调用之前最近一条客户发言;没有就用 `query` |
| `answer` | 该调用之后机器人的第一条回复;prompt 快照里没有则为空字符串 |
| `hit` | 启发式判断机器人是否采用了 FAQ 答案。以下情况为 `false`:FAQ 结果为空、机器人没有回复、回复中包含兜底/转人工话术(`_NON_ADOPTION_PATTERNS`,多语言)、回复是少于 60 字符的单行问句;其余为 `true` |
| `confidence` | FAQ 返回条目中 `confidence`/`Confidence`/`score`/`Score` 的最大值;都没有则为 `null` |
| `is_followup` | 是否承接上一次 faqSearch 的提问(第一次调用总是 `false`)。满足任一即为 `true`:不超过 4 个词;以承接词开头(`y `、`and `、`what about`、`那`、`还有` 等);含指代词(`eso`、`that`、`also`、`还有` 等,按词边界匹配);和上一问有长度 ≥4 的相同实词 |
| `kb_key` / `product_category` | toolUse.input 里的 `kb_key` / `productCategory` |
| `faq_matches` | 解析出的 FAQ 条目 `[{question, answer[, confidence]}]`;兼容纯数组、`{"output": "[...]"}`、MCP 风格 `{"content": [{"text": "[...]"}]}` |

注意:`answer` 只能从 prompt 快照中找。如果最后一次 faqSearch 之后的机器人回复没有出现在任何后续 prompt 中,`answer` 为空,`hit` 为 `false`。

### ② trace span:`toolSpans`

每个 faqSearch execute_tool span 一条:

| 字段 | 含义 |
| --- | --- |
| `timestamp` | 日志行时间戳 |
| `sessionId` | 该行的 `session_id` |
| `toolName` / `shortToolName` | 完整工具名 / 最后一个 `___` 之后的部分 |
| `toolUseId`、`input` | span `input_messages` 中的 toolUse(input 是 JSON 字符串时会解析) |
| `status` | span 状态 |
| `success` | `_tool_succeeded`:status 为空或 `OK`,且输出中不含 `"error"`、不含 `HTTP 4xx/5xx` |
| `startTimestamp` / `endTimestamp` / `latencyMs` | span 起止时间与差值。代码不做单位换算,`latencyMs` 的单位与 span 时间戳一致 |
| `outputPreview` | 工具输出文本,截取前 1000 字符 |

`toolSpanCount` = 条数,`toolSpanSuccess` = `success` 为 true 的条数。`analysis.counts.executeToolSpans` 统计的是**所有工具**的 execute_tool span。

### ③ Gateway 日志:`gateway`

`{count, errors, entries}`,只统计提到 faqSearch 的 Gateway 行。每条 entry:`timestamp`;`level`(优先取 JSON 中的 `level`/`levelname`/`log_level`/`severity_text`/`severityText`/`severity`,否则在文本中找 `DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL`);`isError`(文本匹配 `\b(ERROR|Exception|Traceback|FATAL)\b`,区分大小写);`message`(前 1000 字符)。`errors` 是 `isError` 为 true 的条数。

## 6. Contact 信息与 attributes 补充

`_enrich_with_contact`(914–932 行):

1. 用 `instanceName` 在 `INSTANCES_META` 中查找 `instanceId` 和 `region`,写入 `instanceId` / `connectRegion`。找不到时 `contactError = attributesError = "instance not in INSTANCES_META"`,不调用 API。
2. `contactId` 不是 UUID 格式时写 `"not a contact id"`,不调用 API。
3. 在该实例所在 region 调用 `DescribeContact`。只保留以下字段中的非空值,并把首字母改为小写:`Channel`、`InitiationMethod`、`InitiationTimestamp`、`DisconnectTimestamp`、`LastUpdateTimestamp`、`DisconnectReason`、`InitialContactId`、`PreviousContactId`、`QueueInfo`、`AgentInfo`、`SystemEndpoint`。时间转为 ISO 字符串;嵌套对象(如 `queueInfo`)内部的键名保持原样。
4. 调用 `GetContactAttributes`,参数 `InitialContactId` 取 DescribeContact 返回的 `initialContactId`(属性挂在初始联系人上,转接后的 Contact 要用它查);DescribeContact 失败或没有该字段时用文件名中的 contactId。返回的**全部**属性原样写入 `attributes`。
5. 任一 API 失败都只写对应的 error 字段,格式 `"<ErrorCode>: <Message>"`(非 ClientError 用异常类型名),不影响 `analysis` 的输出。

PII 提示:`attributes`、对话 `turns`、`faq_events` 中的客户原话、Gateway 日志预览都可能包含个人信息,会全部写进本函数的日志组(保留 30 天)。

## 7. 输出记录

每行都是一个 JSON 对象(`ensure_ascii=False`),用 `type` 区分:

| type | 何时输出 | 字段 |
| --- | --- | --- |
| `process-events-logs.result` | 每个 events 文件一条 | 见下表 |
| `process-events-logs.chunk` | result 截断后仍超过 `MAX_LOG_BYTES`,拆成多行 | `type`、`key`、`eTag`、`part`(从 1 开始)、`parts`、`data` |
| `process-events-logs.skip` | key 不符合 events 规则 | `type`、`key`、`reason` |
| `process-events-logs.error` | 处理单条记录时有异常逃出 `process_record`/`_emit` | `type`、`error`、`traceback` |

函数返回值为 `{"ok": true, "processed", "skipped", "errors"}`,其中 `errors` 也计入带 `processingError` 的 result。

### result 字段

| 字段 | 含义 |
| --- | --- |
| `bucket`、`key`、`eTag`、`versionId`、`sequencer`、`size`、`eventName`、`eventTime` | S3 事件信息 |
| `instanceName` / `contactId` | 从 key 解析出的实例名与 Contact ID |
| `instanceId` / `connectRegion` | 来自 `INSTANCES_META`,找不到为 `null` |
| `contact` / `contactError` | DescribeContact 结果(见第 6 节)/ 失败原因 |
| `attributes` / `attributesError` | GetContactAttributes 全部属性 / 失败原因 |
| `analysis.counts` | `total`(有效行)、`connect`、`gateway`、`agenticMessages`、`llmInvocations`、`aiAgentTraces`(按 `event_type` 计数)、`executeToolSpans`(所有工具) |
| `analysis.transcript` | `inputEventType`(`TRANSCRIPT_AGENTIC_MESSAGE` / `TRANSCRIPT_LARGE_LANGUAGE_MODEL_INVOCATION` / `null`)与 `sessions` |
| `analysis.faqSearch` | `transcriptCalls`、`transcriptHits`、`toolSpans`、`toolSpanCount`、`toolSpanSuccess`、`gateway{count, errors, entries}` |
| `analysis.logSummary` | `build_contact_summary` 结果中的 `count`、`minTs`、`maxTs`、`mcpToolCalls`、`faqAsk`、`faqSend`、`errors`、`escalated`、`escalationReason`、`locale`;摘要计算失败时改为 `analysis.logSummaryError`;没有有效行时两者都没有 |
| `processingError` | 读取/解析失败时为 `"<异常类型>: <信息> \| <最内层 3 帧 traceback>"`,此时 `analysis` 为 `null` |
| `object` | `{contentLength, lastModified}`,`GetObject` 成功后才有 |
| `truncated` | 发生截断时为 `true` |

`logSummary` 各字段含义(来自 `contact_summary.py`):`count` 有效行数;`minTs`/`maxTs` 首末行毫秒时间戳;`mcpToolCalls` 所有 execute_tool span 数;`faqAsk` faqSearch span 数;`faqSend` 其中 `_tool_succeeded` 为真的数量;`errors` 状态非 `OK` 的 span 数 + 匹配错误关键字的**全部** Gateway 行数;`escalated`/`escalationReason` 是否转人工及原因;`locale` 从消息中识别的语言区域。

### 示例(仅为示意,字段值是编造的,数组已省略)

```json
{
  "type": "process-events-logs.result",
  "bucket": "connect-ai-logs-<account_id>",
  "key": "events/<InstanceAlias>/11111111-2222-3333-4444-555555555555.json.gz",
  "eTag": "<etag>", "versionId": null, "sequencer": "<sequencer>", "size": 12345,
  "eventName": "ObjectCreated:Put", "eventTime": "2025-01-01T00:00:00.000Z",
  "instanceName": "<InstanceAlias>", "instanceId": "<instance-id>", "connectRegion": "us-east-1",
  "contactId": "11111111-2222-3333-4444-555555555555",
  "contact": {"channel": "CHAT", "initiationMethod": "API", "initiationTimestamp": "2025-01-01T00:00:00+00:00"},
  "contactError": null,
  "attributes": {"<attrKey>": "<value>"},
  "attributesError": null,
  "analysis": {
    "counts": {"total": 120, "connect": 110, "gateway": 10, "agenticMessages": 6,
               "llmInvocations": 6, "aiAgentTraces": 40, "executeToolSpans": 3},
    "transcript": {"inputEventType": "TRANSCRIPT_AGENTIC_MESSAGE", "sessions": ["..."]},
    "faqSearch": {"transcriptCalls": 2, "transcriptHits": 1, "toolSpans": ["..."],
                  "toolSpanCount": 2, "toolSpanSuccess": 2,
                  "gateway": {"count": 4, "errors": 0, "entries": ["..."]}},
    "logSummary": {"count": 120, "minTs": 1735689600000, "maxTs": 1735689900000,
                   "mcpToolCalls": 3, "faqAsk": 2, "faqSend": 2, "errors": 0,
                   "escalated": false, "escalationReason": "", "locale": "es_ES"}
  },
  "processingError": null,
  "object": {"contentLength": 12345, "lastModified": "2025-01-01T00:00:00+00:00"}
}
```

### 截断与分片

`MAX_LOG_BYTES` 默认 200000 字节(CloudWatch 单条事件上限 256 KB),配置值小于 1000 时按 1000,非法值按默认值。

1. 整行不超限:原样输出一行。
2. 超限:递归把所有超过 2000 字符的字符串截断为前 2000 字符 + `…[truncated N chars]`,并加 `"truncated": true`;不超限则输出一行。
3. 仍超限:把截断后的 JSON 文本按字符切片,每片按最坏 4 字节/字符估算以保证不超限,逐行输出 `.chunk`。
4. 还原:取同一 `key` + `eTag` 的所有 chunk,按 `part` 从 1 到 `parts` 依次拼接 `data`,得到完整(已截断的)result JSON。chunk 行没有 `type=...result`,Logs Insights 按 result 过滤时查不到它们。

## 8. 配置与部署资源

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `INSTANCES_META` | `{}` | `{InstanceAlias: {"instanceId", "region", "configKey"}}`,由 `setup_cloudfront.py` 按 config 中每个实例写入;无法解析时按空处理 |
| `LOGS_BUCKET` | 无 | 日志桶名,setup 会写入;代码只把它当说明,读取用的是 S3 事件里的 bucket |
| `MAX_LOG_BYTES` | `200000` | 单行最大字节数,setup **不设置**,需要时手工在 Lambda 配置中添加 |

### 部署资源(`setup_cloudfront.py` 的 `deploy_process_events_lambda`,可重复执行)

| 项 | 值 |
| --- | --- |
| 函数名 | `connect-ai-logs-<account_id>-process-events`,python3.12,handler `process_events_logs.handler`,超时 120s,内存 512MB |
| 部署包 | `process_events_logs.zip` = `lib/lambda/process_events_logs.py` + `lib/lambda/contact_summary.py` |
| 执行角色 | 三个 Lambda 共用的 `connect-ai-logs-<account_id>-lambda-role`,本函数的权限在内联策略 `events-read-and-contact` 中:`logs:CreateLogGroup`;本函数日志组的 `logs:CreateLogStream`/`logs:PutLogEvents`;`s3:GetObject` 于 `arn:aws:s3:::<logs_bucket>/events/*`;`connect:DescribeContact`/`connect:GetContactAttributes` 于各实例 `<instance_arn>/contact/*` |
| 调用权限 | 函数资源策略(`add_permission`):StatementId `s3invoke-events`,Action `lambda:InvokeFunction`,Principal `s3.amazonaws.com`,SourceArn 日志桶,SourceAccount 账号;已存在则复用 |
| 桶通知 | Id `process-events-logs`,`s3:ObjectCreated:*`,prefix `events/`,suffix `.json.gz`(与 `process-raw-logs` 等其他通知合并写入) |
| 异步重试 | `MaximumRetryAttempts=0`(handler 不会因单个文件失败而抛错,重试只会重复输出) |
| 预留并发 | 5,避免一次同步写入大量 events 文件时打满 Connect API 速率;超出的异步调用由 Lambda 排队 |
| 日志组 | `/aws/lambda/connect-ai-logs-<account_id>-process-events`,保留 30 天 |

## 9. 限制与注意事项

- **重复记录,无去重。** 每次同步都会覆盖窗口内所有 Contact 的 events 文件,每次覆盖都触发一次解析,所以同一 Contact 会有多条 result。splitter 每次重新 gzip 压缩,即使内容没变 `eTag` 也可能不同。统计时请按 `contactId` 取最新一条(`eventTime` / `@timestamp`)。
- **结果可能不完整。** 同步发生在通话进行中时只有部分日志;窗口起点切过通话时开头缺失(会话 transcript 取的是累积快照,受影响较小,但 span 和 Gateway 统计会少)。某个 Contact 在后续同步窗口中不再出现时,它的文件不会再被改写,也就不会再触发。
- **Gateway 归属是推断的。** 没有 ID 子串命中的 Gateway 行按 ±5s 时间窗分给最近的 Contact,并发通话时可能分错;两种方式都关联不上的行在 splitter 中就被丢弃,本函数看不到。
- **三个 faqSearch 来源口径不同**(见第 5 节):大小写敏感性不同,`hit` / `is_followup` 是启发式,`latencyMs` 不做单位换算。
- **非 Contact ID 文件名**(`unknown`、session_id)也会被解析,但 Connect 信息为空或带 error。
- **超时与失败不会重试。** 超时 120s、异步重试 0;单次调用失败时,这一版文件的结果会丢失,要等下一次同步。
- **PII**:见第 6 节。

## 10. 如何查看结果

实时查看:

```bash
aws logs tail /aws/lambda/connect-ai-logs-<account_id>-process-events \
    --region us-east-1 --profile <account_id> --follow
```

CloudWatch Logs Insights 最多从一条 JSON 日志中自动提取 200 个字段([官方文档](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_AnalyzeLogData-discoverable-fields.html))。result 中的 `sessions`、`toolSpans` 等数组很容易超过这个数量,排在后面的 `analysis.faqSearch.*` 可能不会被自动提取,所以下面的查询用 `parse @message` 直接从原文中取数值。排在最前面的 `type`、`contactId` 可以直接使用。正则依据 `json.dumps` 的默认分隔符(`", "`、`": "`)。

每个 Contact 最新一条结果的 faqSearch 统计:

```
fields @timestamp, contactId, instanceName
| filter type = "process-events-logs.result"
| parse @message /"channel": "(?<channel>[^"]*)"/
| parse @message /"transcriptCalls": (?<faqCalls>\d+), "transcriptHits": (?<faqHits>\d+)/
| parse @message /"toolSpanCount": (?<spanCalls>\d+), "toolSpanSuccess": (?<spanOk>\d+)/
| parse @message /"gateway": \{"count": (?<gwFaq>\d+), "errors": (?<gwErrors>\d+)/
| sort @timestamp desc
| dedup contactId
| limit 100
```

`dedup` 之后只能接 `limit`。`channel` 取第一个 `"channel"` 键,也就是 `contact.channel`(`contact` 排在 `attributes` 之前);DescribeContact 失败时 `contact` 为 `null`,该列为空,除非 `attributes` 里恰好有同名键。

找出 faqSearch 工具调用失败或 Gateway 报错的 Contact:

```
filter type = "process-events-logs.result"
| parse @message /"toolSpanCount": (?<spanCalls>\d+), "toolSpanSuccess": (?<spanOk>\d+)/
| parse @message /"gateway": \{"count": (?<gwFaq>\d+), "errors": (?<gwErrors>\d+)/
| filter spanCalls != spanOk or gwErrors > 0
| stats latest(spanCalls) as calls, latest(spanOk) as ok, latest(gwErrors) as gatewayErrors by contactId
```

## 11. 本地测试

mock 测试(不调用 AWS)位于 `log/.agents/tasks/process-events-tests/`,覆盖本函数、部署函数、通知合并与共用角色。在 `log/` 目录下运行:

```bash
python3 -m unittest discover -s .agents/tasks/process-events-tests -p 'test_*.py'
```
