# setup_cloudfront.py 使用说明

## 1. 脚本作用

`setup_cloudfront.py` 是「Connect AI Agent 日志分析」Web 应用的一键部署脚本。它把 Amazon Connect AI Agent 日志(以及可选的 AgentCore Gateway 日志)从 CloudWatch Logs 归档到 S3,由云端 Lambda 解析后写入 DynamoDB / S3,并把前端站点通过 CloudFront 发布出来,用 Cognito 登录鉴权。支持多个 Connect 实例,数据按实例名(`InstanceAlias`,即 DynamoDB 分区键 `instanceName`)隔离。全程不在本地处理日志文件。

### 创建/配置的 AWS 资源

以下名称中的 `<account_id>` 取自 config 中**第一个实例**的 `instance_arn`。所有资源统一部署在 **us-east-1**(区域在代码中固定)。

| 资源 | 名称 / 说明 |
| --- | --- |
| S3 日志桶 | `connect-ai-logs-<account_id>`;`raw/`、`trigger/` 前缀 7 天后自动过期;配置了 CORS(GET/HEAD);`trigger/*.json` 的创建事件触发解析 Lambda,`events/*.json.gz` 的创建事件触发事件解析 Lambda |
| S3 Web 桶 | `connect-ai-logs-<account_id>-web`;关闭公共访问,仅允许该 CloudFront 分配经 OAC 读取 |
| DynamoDB 表 | `connect-ai-contacts-<account_id>`(分区键 `instanceName`,排序键 `contactId`;GSI `by-instance-time`:`instanceName` + `minTs`;按需计费) |
| 解析 Lambda | `connect-ai-logs-<account_id>-splitter`(python3.12,超时 900s,内存由 `--lambda-memory` 控制,异步重试次数 0);执行角色 `connect-ai-logs-<account_id>-lambda-role`,三个 Lambda 共用这一个角色,角色上有三份内联策略 `logs-bucket-rw`(解析)/ `ddb-read-and-logs`(查询)/ `events-read-and-contact`(事件解析),三者权限为合并后的并集 |
| 查询 Lambda | `connect-ai-logs-<account_id>-query`(python3.12,超时 30s,内存 1024MB);执行角色与解析 Lambda 共用 `connect-ai-logs-<account_id>-lambda-role`(内联策略 `ddb-read-and-logs`) |
| 事件解析 Lambda | `connect-ai-logs-<account_id>-process-events`(python3.12,超时 120s,内存 512MB,异步重试次数 0,预留并发 5);执行角色与解析 Lambda 共用 `connect-ai-logs-<account_id>-lambda-role`(内联策略 `events-read-and-contact`);桶通知 Id `process-events-logs`;日志组 `/aws/lambda/connect-ai-logs-<account_id>-process-events` 保留 30 天 |
| API Gateway | REST API `connect-ai-logs-<account_id>-api`,stage `prod`,Cognito Authorizer `cognito-auth` |
| Cognito 用户池 | `connect-ai-agent-logs-<account_id>`(仅管理员可创建用户,邮箱作用户名,含自定义属性 `custom:instances`)及应用客户端 `connect-ai-agent-logs-<account_id>-web`(无 secret,启用 `USER_PASSWORD_AUTH`) |
| CloudFront | OAC `connect-ai-logs-<account_id>-oac`;分配的 Comment 为 `connect-ai-agent-logs-<account_id>`,HTTPS 重定向,默认根对象 `index.html`,403/404 回落到 `/index.html` |

API 路由(均转发到查询 Lambda):`GET /instances`、`GET /contacts`、`GET /contacts/{contactId}/events`、`GET /contacts/{contactId}/describe`(以上需 Cognito IdToken),以及 `POST /translate`(代码中为无 API 鉴权,Bedrock 权限由共用 Lambda 执行角色上的 `ddb-read-and-logs` 内联策略控制)。

> 注意:脚本顶部 docstring 提到 Cognito 身份池/鉴权角色,但代码实际**没有创建**身份池或鉴权角色(`auth.js` 也说明不再使用 Identity Pool),前端通过 API Gateway 访问数据。

### 数据流

```
CloudWatch Logs (AI Agent 日志 / Gateway 日志)
   │  lib/fetch-to-s3.py (每个日志组一个子进程并发,流式分片上传)
   ▼
s3://connect-ai-logs-<account_id>/raw/<实例key>/connect.ndjson (与 gateway.ndjson)
   │  脚本写入 trigger/<实例key>.json  →  S3 事件触发
   ▼
解析 Lambda (handler.py + connect_parser.py)
   ├─ DynamoDB: Contact 摘要
   ├─ S3: events/<instanceName>/<contactId>.json.gz (事件明细)
   └─ S3: <实例key>/index.json (解析结果/错误,脚本据此判断完成)

S3: events/<instanceName>/<contactId>.json.gz  →  S3 事件触发
   ▼
事件解析 Lambda connect-ai-logs-<account_id>-process-events (process_events_logs.py,部署时与 contact_summary.py 一起打包)
   ├─ Connect DescribeContact / GetContactAttributes (渠道 + 联系人属性)
   └─ CloudWatch Logs: /aws/lambda/connect-ai-logs-<account_id>-process-events (每个文件一行 JSON 解析结果)

浏览器 → CloudFront(静态站点, auth.js 登录 Cognito)
       → API Gateway (IdToken) → 查询 Lambda → DynamoDB / S3 预签名 URL / Connect DescribeContact / Bedrock 翻译
```

## 2. 前置条件

- **Python**:`python3`(代码未声明最低版本;解析 Lambda 运行时为 python3.12)。
- **依赖包**:`boto3`(`pip install boto3`)。
- **AWS 凭证**:脚本把 **AWS profile 名固定为账号 ID**(即 config 第一个实例 ARN 中的账号),主脚本和 `fetch-to-s3.py` 子进程都使用 `--profile <account_id>`。因此本机 `~/.aws/config` / `credentials` 中需要存在名为 `<account_id>` 的 profile。
- **权限**:该 profile 需要能操作以下服务(脚本里实际调用到的):S3、IAM(创建/更新角色及内联策略)、Lambda、DynamoDB、Cognito User Pools、API Gateway、CloudFront(OAC、分配、失效)、CloudWatch Logs(`filter_log_events`、`create_log_group`、`put_retention_policy`)、Amazon Connect(`DescribeInstance`、`ListInstanceStorageConfigs`)。
- **需要预先存在的资源**:
  - 各 Connect 实例,且能查询到其 `InstanceAlias`(查不到会直接退出)。
  - `ai_log_arn` 对应的 AI Agent 日志组。可用上一级目录的 `../setup_connect_ai_agent_logs.py` 创建(它会建立日志投递并创建日志组 `/aws/connect/ai-agent-logs-<InstanceAlias>`,并读取本目录下的 `config-*.json`)。
  - `gateway_log_arn`(可选)对应的 AgentCore Gateway 应用日志组。
- **依赖文件**(缺失会报错退出,见下方目录结构):`lib/fetch-to-s3.py`、`lib/lambda/handler.py`、`lib/parse-connect-ai-logs.py`、`lib/lambda/process_events_logs.py`、`lib/web-cloudfront/auth.js`;另外会用到 `lib/lambda/query_handler.py` 与 `lib/web/*`。
- **网络**:本机需能访问各 AWS 服务端点。

### 配置文件 `config-<account_id>.json`

配置文件放在本目录(与脚本同目录),文件名形如 `config-<account_id>.json`。字段:

| 字段 | 说明 |
| --- | --- |
| `email` | 登录邮箱。必须是合法邮箱格式;脚本在 Cognito 中创建该用户,一次性密码发送到此邮箱 |
| `hours` | 拉取最近多少小时的日志;`0` 表示全部历史(缺省按 `0` 处理) |
| `connect` | 字典,key 为实例标识(`<实例key>`,用于 S3 前缀 `raw/<key>/`、`trigger/<key>.json` 和 `<key>/index.json`),不能为空 |
| `connect.<key>.instance_arn` | 必填。Connect 实例 ARN;区域、实例 ID 从中解析。**账号 ID 取第一个实例的 ARN**,所有实例应属同一账号 |
| `connect.<key>.ai_log_arn` | AI Agent 日志组 ARN(`arn:aws:logs:<region>:<account>:log-group:<name>:*`)。不配置则该实例被跳过解析(结果记为 `no ai_log_arn`) |
| `connect.<key>.gateway_log_arn` | 可选。Gateway 日志组 ARN,不配置则只处理 AI Agent 日志 |

示例(占位符):

```json
{
    "email": "user@example.com",
    "hours": 8,
    "connect": {
        "<实例key>": {
            "instance_arn": "arn:aws:connect:<region>:<account_id>:instance/<instance-id>",
            "ai_log_arn": "arn:aws:logs:<region>:<account_id>:log-group:/aws/connect/ai-agent-logs-<InstanceAlias>:*",
            "gateway_log_arn": "arn:aws:logs:<region>:<account_id>:log-group:<gateway-log-group>:*"
        }
    }
}
```

## 3. 如何执行

在本目录下执行:

```bash
cd log/cloudfront
python3 setup_cloudfront.py [--config <file>] [--out-dir <dir>] [--keep] \
    [--lambda-memory <MB>] [--csat-attr <key>] [-h]
```

参数:

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--config <file>` | `config.json` | 配置文件名,按**脚本所在目录**解析路径。见下方「配置文件选择」 |
| `--out-dir <dir>` | 系统临时目录 | 临时部署产物目录(`lambda.zip`、`process_events_logs.zip`、`query_lambda.zip`) |
| `--keep` | 关 | 保留临时产物目录;不加则脚本退出时删除(包括用 `--out-dir` 指定的目录) |
| `--lambda-memory <MB>` | `1024` | 解析 Lambda 内存 |
| `--csat-attr <key>` | 环境变量 `CSAT_ATTRIBUTE_KEY`,否则 `botevaluation` | CSAT 满意度评分对应的联系人属性键名 |
| `-h, --help` | | 显示帮助 |

**配置文件选择**:只要 `--config` 保持默认值 `config.json`,脚本就会扫描本目录下所有 `config-*.json`:只有一个时自动使用;多个时交互式提示输入序号选择;一个都没有则报错退出。若要直接指定,传入其他文件名即可(不要传 `config.json`,它与默认值无法区分,同样会触发扫描)。

示例:

```bash
# 本目录只有一个 config-*.json 时,自动选用
python3 setup_cloudfront.py

# 显式指定配置文件
python3 setup_cloudfront.py --config config-<account_id>.json

# 调大解析 Lambda 内存,指定 CSAT 属性键,并保留临时产物
python3 setup_cloudfront.py --config config-<account_id>.json \
    --lambda-memory 2048 --csat-attr botevaluation --keep
```

### 执行流程(与代码顺序一致)

1. 检查依赖文件、读取并校验 config(邮箱格式、`connect` 非空)。
   接着(只读,在任何 AWS 写入之前)对每个实例查询 `InstanceAlias`(失败则退出),并尝试查询通话录音桶(失败只忽略);
   再校验共用角色上三份内联策略的合计长度(IAM 上限 10,240 字符,1 个实例约 2,200,10 个实例约 3,900;超限则报错退出)。
2. 创建/复用日志桶,配置 Lifecycle 与 CORS;创建/复用 DynamoDB 表(缺 GSI 时补建)。
3. 创建/更新共用 Lambda 执行角色 `connect-ai-logs-<account_id>-lambda-role`(写入内联策略 `logs-bucket-rw`)与解析 Lambda,配置 S3 `trigger/*.json` 事件通知。
4. 创建/更新事件解析 Lambda `connect-ai-logs-<account_id>-process-events`(在共用角色上写入内联策略 `events-read-and-contact`,部署代码与配置、日志组、S3 调用权限),并合并写入 `events/*.json.gz` 事件通知。放在拉取日志之前,本次写入的 events 文件也会被解析。
5. 并发启动 `fetch-to-s3.py` 拉取各实例的 AI Agent / Gateway 日志到 `raw/<key>/`;任一拉取失败的实例会被跳过。
6. 删除旧的 `<key>/index.json`,写入 `trigger/<key>.json` 触发 Lambda,轮询 `index.json`(最长约 16 分钟,超时则提示 Lambda 仍在后台处理)。
7. 创建/复用 Cognito 用户池与应用客户端;在共用角色上写入内联策略 `ddb-read-and-logs` 并创建/更新查询 Lambda;配置并部署 API Gateway(`prod`);创建登录用户。
8. 上传前端文件到 Web 桶,创建/复用 OAC 与 CloudFront 分配,写 Web 桶策略,发起 `/*` 失效,等待 CloudFront 变为 `Deployed`(最长等 1200 秒,超时则提示稍后用 `aws cloudfront get-distribution` 查看)。

### 幂等性 / 重复执行

脚本可重复执行:S3 桶、DynamoDB 表、IAM 角色、Lambda、Cognito 用户池/客户端、API Gateway、OAC、CloudFront 分配均按名称检查并复用或更新。需要注意:

- 重跑会**重新拉取日志并重新触发解析**,已存在的 Contact 摘要与事件明细会被覆盖写入。
- 重跑会重新部署 Lambda 代码与配置,并更新前端文件和 `aws-config.js`。
- 已存在的登录用户不会重新创建(不会重发一次性密码)。
- 日志桶的事件通知按 Id(`process-raw-logs`、`process-events-logs`)合并写入:同 Id 或同函数且同过滤规则的旧条目被替换,其它 Lambda/SNS/SQS/EventBridge 通知配置保留。若事件解析 Lambda 或其 `s3invoke-events` 调用权限被手工删除,第 3 步会先丢弃失效的 `process-events-logs` 条目,第 4 步重新部署后再写回;其它保留的配置若指向已删除的目标,S3 会拒绝写入,需要先手工清理。
- 事件解析 Lambda 不去重:每次同步都会覆盖写入窗口内所有 Contact 的 events 文件,所以同一 Contact 每次同步都会再输出一条结果,可用 `key`/`eTag`/`eventTime` 区分。
- 从旧版本升级(一次性清理):旧版本为查询 Lambda 和事件解析 Lambda 分别创建了 `connect-ai-logs-<account_id>-query-role` 和 `connect-ai-logs-<account_id>-process-events-role`。重跑脚本会通过 `update_function_configuration` 把两个函数切换到 `connect-ai-logs-<account_id>-lambda-role`;脚本结束时若检测到旧角色仍存在,会在 stderr 打印提示和对应的清理命令。脚本**不会自动删除**旧角色,确认两个函数已切换后可手动执行:

  ```bash
  ACCT=<account_id>
  for R in connect-ai-logs-$ACCT-query-role connect-ai-logs-$ACCT-process-events-role; do
    aws iam list-role-policies --role-name "$R" --profile "$ACCT"
    aws iam list-attached-role-policies --role-name "$R" --profile "$ACCT"
  done
  # 按上面列出的策略名逐个删除(query-role 通常是 ddb-read-and-logs;
  # process-events-role 通常是 events-read-and-contact,更早版本可能还有 process-events-logs)
  aws iam delete-role-policy --role-name connect-ai-logs-$ACCT-query-role --policy-name ddb-read-and-logs --profile "$ACCT"
  aws iam delete-role-policy --role-name connect-ai-logs-$ACCT-process-events-role --policy-name events-read-and-contact --profile "$ACCT"
  # 有托管策略时先 detach: aws iam detach-role-policy --role-name <role> --policy-arn <arn> --profile "$ACCT"
  aws iam delete-role --role-name connect-ai-logs-$ACCT-query-role --profile "$ACCT"
  aws iam delete-role --role-name connect-ai-logs-$ACCT-process-events-role --profile "$ACCT"
  ```

## 4. 执行后的产出

脚本结束时在终端打印汇总,包括:

- CloudFront 访问地址 `https://<xxxx>.cloudfront.net`、分配 ID 与部署状态。
- 登录邮箱、每个实例的处理结果(Contact 数量 / 解析报错 / Lambda 处理中)。
- 资源清单:日志桶、Web 桶、解析/查询/事件解析 Lambda 名、`Lambda 执行角色`(三个 Lambda 共用的 `connect-ai-logs-<account_id>-lambda-role`)、Cognito 用户池 ID 与应用客户端 ID、API Gateway URL。
- 若账号里还留有旧版本的 `-query-role` / `-process-events-role`,最后会在 stderr 打印旧角色提示和手动清理命令(见上一节"从旧版本升级")。

**生成的内容**:

- 本地:仅有临时的 `lambda.zip`、`process_events_logs.zip`、`query_lambda.zip`(默认执行结束即删除,`--keep` 保留)。脚本不会修改本目录下的任何源文件。
- Web 桶中的文件:`index.html`(由 `lib/web/index.html` 改写:移除 `data.js` 引用,改为加载 AWS SDK、`aws-config.js`、`auth.js`,并给脚本加版本号)、`app.js`、`i18n.js`、`site-config.js`、`auth.js`、`aws-config.js`,以及 `connect-enrich.js`(`lib/web/` 下没有该文件时上传一个空占位)。
- `aws-config.js` 由脚本即时生成并直接上传(不落本地),包含区域、用户池 ID、应用客户端 ID、API URL、第一个实例的 ID/ARN/区域、`csatAttribute` 等运行时配置。

**事件解析 Lambda 的输出**(`/aws/lambda/connect-ai-logs-<account_id>-process-events`,可用 `aws logs tail /aws/lambda/connect-ai-logs-<account_id>-process-events --follow` 查看):每个 `events/<instanceName>/<contactId>.json.gz` 输出一行 JSON(`type=process-events-logs.result`),主要字段:

- `bucket`、`key`、`eTag`、`versionId`、`sequencer`、`eventTime`、`instanceName`、`instanceId`、`connectRegion`、`contactId`。
- `contact`:DescribeContact 的 `channel`、`initiationMethod`、各时间戳、`disconnectReason`、`initialContactId`、`queueInfo`、`agentInfo` 等;`attributes`:GetContactAttributes 的全部属性。失败时对应写 `contactError` / `attributesError`(文件名不是 Contact ID、实例不在 `INSTANCES_META`、ResourceNotFound 等),不影响解析结果输出。
- `analysis.transcript.sessions`:`process_events_logs.py` 中 `parse_agentic_message` + `dedupe_latest_by_session` 的原样输出(对话、`faq_events` 的 query/命中/置信度/追问)。
- `analysis.faqSearch`:`transcriptCalls`/`transcriptHits`;`toolSpans`(`TRANSCRIPT_AI_AGENT_TRACE` 中 faqSearch 的 execute_tool:输入、状态、成功与否、耗时 ms、输出预览);`gateway`(提到 faqSearch 的 Gateway 日志条数、错误数、条目)。
- `analysis.counts`、`analysis.logSummary`(与 DynamoDB 摘要同口径的 faqAsk/faqSend/errors 等)。

单行超过 `MAX_LOG_BYTES`(默认 200000 字节)时,先把超过 2000 字符的字符串截断并标记 `truncated: true`;仍超限则拆成多行 `type=process-events-logs.chunk`,按 `part` 顺序拼接 `data` 即为完整 JSON。`attributes` 可能包含个人信息,会写入该日志组(保留 30 天)。

**认证方式**:

- 首次登录:用邮件里的一次性密码登录,按提示设置新密码;忘记密码可在登录页走「忘记密码」流程。
- `lib/web-cloudfront/auth.js` 负责登录门禁:直接调用 Cognito User Pool 的 REST 接口(`USER_PASSWORD_AUTH`、首次改密、忘记密码),成功后将 IdToken 放入 `Authorization` 头调用 API Gateway;IdToken 与 RefreshToken 存在 `sessionStorage`(关闭标签页即清除),静默刷新 Token,最长会话 24 小时。登录后右上角提供实例切换器、渠道过滤和退出按钮。
- 实例访问权限由用户属性 `custom:instances` 决定:`*` 表示所有实例,`a,b` 表示仅限指定实例。脚本为 `email` 用户设置的值为 `*`(已存在但缺该属性时补设为 `*`)。

## 5. 目录结构

| 路径 | 说明 |
| --- | --- |
| `setup_cloudfront.py` | 部署脚本(本文档的主角) |
| `config-<account_id>.json` | 账号级配置(邮箱、拉取时间范围、各 Connect 实例 ARN) |
| `lib/fetch-to-s3.py` | 把一个 CloudWatch 日志组以 NDJSON 流式分片上传到 S3,由脚本以子进程方式调用 |
| `lib/parse-connect-ai-logs.py` | 日志解析模块,打包进解析 Lambda 时重命名为 `connect_parser.py` |
| `lib/lambda/handler.py` | 解析 Lambda 入口(S3 事件触发,写 DynamoDB 与 S3 明细) |
| `lib/lambda/query_handler.py` | 查询 Lambda(API Gateway 后端,按 `custom:instances` 校验实例权限) |
| `lib/lambda/process_events_logs.py` | 事件解析 Lambda `connect-ai-logs-<account_id>-process-events` 入口,内含 AI Agent 会话 / faqSearch 调用解析;部署时与 `lib/lambda/contact_summary.py` 一起打包;详见 [lib/lambda/README.md](lib/lambda/README.md) |
| `lib/lambda/sync_log_handler.py` | 独立的 Lambda 入口,按实例拉取日志并触发解析,前置条件是本脚本已创建好基础设施;本脚本不部署它 |
| `lib/lambda/faq_retrieval_log.py` | 定时 FAQ 分析 Lambda(查询 CloudWatch 日志并上传到外部 FAQ 检索日志服务);本脚本不部署它 |
| `lib/web/` | 前端文件:`index.html`、`app.js`、`i18n.js`、`site-config.js` |
| `lib/web-cloudfront/auth.js` | CloudFront 版登录门禁与数据加载脚本 |

上一级目录的 `../setup_connect_ai_agent_logs.py`:为 Connect 实例配置 AI Agent 日志投递到 CloudWatch Logs,是本脚本的上游(先有日志,再用本脚本部署分析站点)。

## 6. 历史 Contact 摘要回填

`backfill_ddb_contact_summaries.py` 从长期保留的 `events/<instanceName>/<contactId>.json.gz` 重新计算日志派生摘要，并仅更新指定 DynamoDB 字段。它复用解析 Lambda 的 `contact_summary.py`，适用于修改某列解析逻辑后重算历史数据。删除 DynamoDB item 本身不会触发 CloudWatch/S3 重放；本工具默认也不会重建已删除 item。

### 快速开始

工具读取 `setup_cloudfront.py` 使用的同一个 `config-<account_id>.json`，自动推导 profile、区域、S3 桶、DynamoDB 表和每个实例的 instanceName，通常无需手动传参：

```bash
# 零参数：在终端中先显示数据概览，再进入交互菜单（见下方“交互模式”）；
# 在脚本/CI（非 TTY）中或加 --non-interactive 时，直接 dry-run 全部实例的全部 Contact
python3 backfill_ddb_contact_summaries.py
python3 backfill_ddb_contact_summaries.py --non-interactive

# 每个实例各写入最多 25 个 Contact
python3 backfill_ddb_contact_summaries.py --apply --limit 25

# 写入全部实例的全部 Contact（务必先备份，见“验证和回滚”）
python3 backfill_ddb_contact_summaries.py --apply --all

# 只写入一个实例的全部 Contact
python3 backfill_ddb_contact_summaries.py --instance-name <config-key 或 InstanceAlias> --apply --all

# 写入指定几个实例的全部 Contact（逗号分隔）
python3 backfill_ddb_contact_summaries.py --instance-name <config-key-1>,<config-key-2> --apply --all

# 只写入一个实例的单个 Contact
python3 backfill_ddb_contact_summaries.py --instance-name <config-key 或 InstanceAlias> --contact-id <contactId> --apply

# 中断后用相同参数加 --resume 继续
python3 backfill_ddb_contact_summaries.py --apply --limit 25 --resume

# 覆盖推导值，例如先对 staging 表试跑
python3 backfill_ddb_contact_summaries.py --target-table <staging-table> --limit 25
```

建议顺序：先零参数运行，查看数据概览并选择 dry-run 核对统计（非终端环境直接 dry-run），再写一个已知 Contact（交互选项 3 或 `--contact-id`），再 `--limit` 小批量，最后才 `--apply --all`。

### 交互模式

触发条件：stdin 与 stdout 都是终端（TTY），且未传以下任何范围/选择参数：`--instance-name`、`--source-prefix`、`--contact-id`、`--all`、`--limit`、`--max-pages`、`--batch-number`、`--start-after-key`、`--end-key`、`--resume`、`--apply`、`--dry-run`。传了其中任一参数、非 TTY（脚本/CI/管道），或加 `--non-interactive` 时不进入交互，行为与以前相同（零参数即 dry-run 全部实例）。`--config`、`--profile`、`--region`、`--source-bucket`、`--target-table`、`--fields`、`--page-size`、`--batch-size`、`--rate-limit`、`--state-dir`、`--checkpoint`、`--failures-file` 不影响是否进入交互，会原样用于后续执行。

数据概览（只读，只在交互模式下运行；非交互运行不做概览，也不需要 `dynamodb:Query`）：先对所有实例做 DescribeInstance 预检，再对每个实例显示：

- DynamoDB：`Query`（`instanceName = <InstanceAlias>`，`Select=COUNT`，分页累加）得到的精确 item 数；
- S3：`events/<InstanceAlias>/` 下符合 `events/<InstanceAlias>/<contactId>.json.gz` 规则的对象数（与回填使用同一规则，`index`、目录占位、子目录和非 `.json.gz` 不计入）；
- 合计；以及计数差提示：S3 多于 DynamoDB 时提示“S3 中至少有 N 个 Contact 在 DynamoDB 不存在，回填会报告为 missing”，反之提示 DynamoDB 中至少有 N 个 item 在 S3 没有 events 文件。只比较数量，不逐条比对。

概览中 config key 明文显示，表名、桶名和 InstanceAlias 只显示脱敏摘要。任一统计失败（如缺少 `dynamodb:Query` 或 `s3:ListBucket`、凭证不可用）时报错并以退出码 2 结束，不写入任何数据。

菜单：

```
请选择更新范围:
  1) 所有实例的所有 Contact
  2) 指定实例（可多选，如 1,2）
  3) 指定一个 Contact
请选择执行方式:
  1) dry-run 预览（不写入）  [默认]
  2) 写入 DynamoDB
```

选项 2 输入概览中的实例编号（如 `1,2`）。选项 3 只需输入 Contact ID（不能为空、不能含 `/`、不能为 `index`），工具对每个实例做 DynamoDB `GetItem` 查找：只有一个实例命中时直接使用；多个实例命中时列出它们（config key + 脱敏 alias）让你选择；都未命中时报错退出（退出码 2）。非法输入会重新提示。

每种选择都映射为一条普通命令，再经过与手动输入完全相同的参数校验、`--apply`/`--all` 门槛和执行流程：

| 范围 | 执行方式 | 等价命令参数 |
| --- | --- | --- |
| 1 所有实例 | dry-run | `--dry-run` |
| 1 所有实例 | 写入 | `--apply --all` |
| 2 指定实例 | dry-run | `--instance-name <key1>,<key2> --dry-run` |
| 2 指定实例 | 写入 | `--instance-name <key1>,<key2> --apply --all` |
| 3 一个 Contact | dry-run | `--instance-name <key> --contact-id <contactId> --dry-run` |
| 3 一个 Contact | 写入 | `--instance-name <key> --contact-id <contactId> --apply` |

执行前会打印“等价的非交互命令”，方便在脚本中复现；其中 profile、桶、表、config 文件名和 Contact ID 显示为脱敏占位符（如 `<table:…摘要>`），复现时需替换回真实值。选择“写入”后，会先列出即将处理的实例及概览时的 S3/DynamoDB 数量（单 Contact 时显示 1 个 Contact），必须输入 `yes` 才会执行；输入其他任何内容、EOF（Ctrl-D）或 Ctrl-C 都会退出，退出码 130，不写入任何数据，也不创建状态文件。

### 参数自动推导

config 发现规则（与当前工作目录无关，始终以脚本所在的 `cloudfront/` 目录为准）：

- 未传 `--config` 时扫描 `cloudfront/config-*.json`，排除 `config-example.json`：恰好一个则使用它；多个则报错并列出文件名，需用 `--config` 指定；没有则报错，除非同时显式提供 `--source-bucket`、`--target-table` 和 `--instance-name`/`--source-prefix`（见下方“无 config 时”）。
- `--config` 的相对路径相对 `cloudfront/` 解析（与 `setup_cloudfront.py` 一致），绝对路径原样使用。

推导规则与 `setup_cloudfront.py` 一致，每一项都可用显式参数覆盖：

| 值 | 默认值 | 来源 | 覆盖参数 |
| --- | --- | --- | --- |
| AWS profile | `<account_id>` | config 中第一个实例 `instance_arn` 的账号 ID（setup 使用同名 profile） | `--profile` |
| 区域（S3/DynamoDB） | `us-east-1` | setup 固定部署区域 | `--region` |
| S3 桶 | `connect-ai-logs-<account_id>` | setup 的日志桶命名 | `--source-bucket` |
| DynamoDB 表 | `connect-ai-contacts-<account_id>` | setup 的 Contact 表命名 | `--target-table` |
| instanceName | 每个实例的 `InstanceAlias` | 在实例 ARN 所在区域调用 `connect:DescribeInstance` | `--source-prefix events/<InstanceAlias>/` |

启动摘要会标明每个值来自 `explicit`（显式参数）、`config`（config 推导）还是 `default`；profile、桶、表、前缀和 config 文件名仍只显示脱敏摘要。

无 config 时（兼容旧用法）必须显式给出全部目标；此时使用默认凭证链（除非传 `--profile`），区域默认 `us-east-1`，`--instance-name` 直接作为 instanceName，不调用 DescribeInstance：

```bash
python3 backfill_ddb_contact_summaries.py \
  --source-bucket <source-bucket> --target-table <target-table> \
  --instance-name <InstanceAlias> --limit 25
```

### 多实例

- 默认处理 config 中的全部实例，按 config 顺序逐个串行执行（不并发）。
- 预检：在任何 S3/DynamoDB 读写之前，先对所有选中实例调用 DescribeInstance 获取 InstanceAlias；任一失败立即以退出码 2 结束，不读写任何数据。
- 一个实例失败（如 S3 AccessDenied）不会中止其他实例；结束时打印每个实例的 `status`（`completed`/`skipped`/`failed`/`error`）和统计以及合计。任一实例有失败（包括 `--resume` 时被跳过、但 checkpoint 中仍有未解决失败的实例）时退出码为 1，全部成功为 0，参数/config/预检/checkpoint 校验错误、交互概览或 Contact 查找失败为 2。凭证不可用（profile 不存在、`NoCredentialsError`、`TokenRetrievalError`、SSO token 过期等）在所有路径上（含 `--source-prefix`、无 config 模式和 `--contact-id`）都会立即中止整个运行并返回 2，不记为实例错误，也不写入失败报告（被中断的 Contact 保留在 checkpoint 中，凭证恢复后 `--resume` 会重放）；`EndpointConnectionError` 等网络错误仍按实例 `error` 处理（退出码 1）。交互模式下用户取消返回 130。
- `--instance-name` 接受一个或多个逗号分隔的名字（如 `<config-key-1>,<config-key-2>`），每项可以是 config key 或 InstanceAlias：先按 config key 匹配（只查询这些实例的 alias）；含非 key 的名字时按 InstanceAlias 匹配（需对 config 中所有实例调用 DescribeInstance 以确认归属），每个 alias 必须恰好对应一个实例。结果去重并按 config 顺序处理。任一名字不匹配时以退出码 2 报错，列出可用的 config key 及其 InstanceAlias 的脱敏摘要。同一实例的 key 与 alias 同时出现时，解析阶段按多实例处理（因此不能配合 `--contact-id` 等单实例参数）。无 config 模式只接受一个名字。如需跳过 DescribeInstance，请改用 `--source-prefix`。
- `--source-prefix events/<InstanceAlias>/` 也只处理一个实例，且不调用 DescribeInstance；与 `--instance-name` 互斥。
- 范围参数按实例生效：`--limit 25` 表示每个实例最多 25 个，`--max-pages`、`--batch-number` 同理。`--contact-id`、`--start-after-key`、`--end-key`（key 需带该实例的 `events/<InstanceAlias>/` 前缀）以及显式 `--checkpoint`/`--failures-file` 只能用于单个实例：config 只有一个实例，或用 `--instance-name`（只给一个名字）/`--source-prefix` 限定。

### 写入确认（`--all`）

- `--apply` 必须带有限范围（`--contact-id`、`--limit`、`--max-pages`、`--batch-number` 配合显式 `--batch-size`、`--end-key`），或显式 `--all` 确认写入所选实例的全部 Contact；否则拒绝执行。
- `--resume` 本身不算范围：恢复 apply 时必须带上原来的范围参数或 `--all`（参数指纹包含它们），例如 `--apply --limit 25 --resume`、`--apply --all --resume`。单独的 `--apply --resume` 不会匹配任何 apply checkpoint，会因参数指纹不匹配（退出码 2）或下条所述的 `ResumeWithoutCheckpoint`（退出码 1）而不写入。
- `--all` 不能与 `--contact-id`、`--limit`、`--max-pages`、`--batch-number`、`--start-after-key`、`--end-key` 组合；可与 `--instance-name`、`--source-prefix`、`--resume`、`--retry-failures`、`--fields`、`--page-size`、`--batch-size`、`--rate-limit` 一起用。
- `--all` 在 dry-run 下也可用，效果与零参数 dry-run 相同（但会计入 checkpoint 参数指纹）。
- 只用 `--apply --resume`（无范围、无 `--all`）时，没有 checkpoint 的实例不会从头开始写入，而是记为失败（`ResumeWithoutCheckpoint`），避免无确认的全量写入。

### 前提条件和最小权限

- Python 3、`boto3`。凭证需以名为 `<account_id>` 的 AWS profile 存在（与 `setup_cloudfront.py` 相同），或用 `--profile` 指定其他 profile。
- S3 对象必须是 gzip 压缩的非空 JSON 数组，每项包含整数 `timestamp`、字符串 `message`、字符串 `source`，路径严格为 `events/<instanceName>/<contactId>.json.gz`。
- 自动推导 instanceName 需要每个实例的 `connect:DescribeInstance`。`--instance-name <config-key>` 只需该实例的权限；`--instance-name <InstanceAlias>` 需要 config 中所有实例的权限（用于确认 alias 归属，拼写错误时以退出码 2 报错）。`--source-prefix events/<InstanceAlias>/` 或无 config 的显式模式不需要该权限，是跳过查询的方式。
- dry-run 需要 `s3:ListBucket`（限制 `s3:prefix` 为每个选定实例的 `events/<InstanceAlias>/`）、这些前缀对象的 `s3:GetObject`、目标表的 `dynamodb:GetItem`。apply 额外需要目标表的 `dynamodb:UpdateItem`。非交互的单 Contact 模式不需要 `s3:ListBucket`。
- 交互模式的数据概览额外需要目标表的 `dynamodb:Query`（不使用 GSI），以及每个实例前缀的 `s3:ListBucket`（即使最后只选单个 Contact，概览也会列举）；选项 3 的 Contact 查找使用 `dynamodb:GetItem`。非交互运行不需要 `dynamodb:Query`。

最小权限策略示意（全部为占位符，多个实例时按实例重复 prefix 与 instance ARN）：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "connect:DescribeInstance",
      "Resource": "arn:aws:connect:<instance-region>:<account_id>:instance/<instance-id>"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::connect-ai-logs-<account_id>",
      "Condition": {"StringLike": {"s3:prefix": "events/<InstanceAlias>/*"}}
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::connect-ai-logs-<account_id>/events/<InstanceAlias>/*"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:us-east-1:<account_id>:table/connect-ai-contacts-<account_id>"
    }
  ]
}
```

dry-run 角色可去掉 `dynamodb:UpdateItem`；只做非交互运行时可去掉 `dynamodb:Query`。工具不需要 `PutItem`、`Scan`、`DeleteItem` 或 CloudWatch 权限。

### 安全行为和字段

默认模式是 **dry-run**；只有显式 `--apply` 才写入，且 apply 必须设置有限范围或显式 `--all`（见上文）。写入使用带主键存在条件的 `UpdateItem`：非 `None` 值使用 `SET`，重算为 `None` 的可选字段使用 `REMOVE`，不会覆盖完整 item，也不会创建缺失 item。每个候选项归类为 `unchanged`、`changed`、`missing` 或 `failed`。

`--fields` 默认是 `escalated,escalationReason`。允许的日志派生字段为：`count,minTs,maxTs,sourceCounts,locale,bu,srNumber,mcpToolCalls,faqAsk,faqSend,srIntent,srSuccess,srRounds,guardBlocks,guardReasons,errors,escalated,escalationReason,aiAgentCalls,responseLatency`。`generatedAt` 和 DescribeContact 派生字段不允许回填。

启动摘要会隐藏 bucket/table 名；Contact 仅显示短摘要。字符串 diff 只显示空值状态、长度和摘要，不打印日志消息、完整 Contact ID、凭证或原始字段正文。失败报告会保留失败 S3 key 和错误类别以便精确重试，但不包含事件正文，文件权限为 `0600`。

### 分页和批次

按 S3 分页和批次进行小批量 apply（每个实例各自计页、计批次）：

```bash
python3 backfill_ddb_contact_summaries.py \
  --instance-name <config-key> --max-pages 2 --page-size 100 \
  --batch-number 1 --batch-size 25 --rate-limit 3 --apply
```

单个实例还可用 `--start-after-key`/`--end-key` 设置字典序 key 范围（如 `events/<InstanceAlias>/<contactId>.json.gz`）。目录占位、`index` 和非 `.json.gz` 对象会跳过；单个损坏 gzip/JSON、读取错误或缺失 item 不会终止后续 Contact。

### Checkpoint、恢复和失败重试

状态文件默认写到 `cloudfront/backfill-state/`（可用 `--state-dir` 修改），每个实例独立：`backfill-checkpoint-<label>.json` 和 `backfill-failures-<label>.json`。`<label>` 是 config key（非 `A-Za-z0-9._-` 字符替换为 `_`）；用 `--source-prefix` 或无 config 模式时为 `alias-<摘要>`。文件权限 `0600`，原子写入。显式 `--checkpoint`/`--failures-file` 只能用于单个实例。该目录含 S3 key 和失败信息，请勿提交到 git（仓库没有 `.gitignore`，需自行排除 `cloudfront/backfill-state/`）。

工具按 `--batch-size` 间隔并在退出时原子更新 checkpoint，记录参数指纹、最后完成的 S3 key、正在处理但尚未提交的 pending key、已处理候选数、分页游标、累计统计和失败项；不记录消息或字段值。只有 Contact 完成分类后才提交页内游标，因此进程在 `GetObject`、`GetItem` 或 `UpdateItem` 期间中断时，恢复会先重放 pending key，而不会静默跳过。listing 失败按操作类型和分页 cursor 独立记录；恢复成功列举同一页后会自动清除该 listing 失败，不会把最后成功的 Contact 当作失败项重算。页内游标会在 `--max-pages` 范围内精确恢复，不会因恢复后重新计页而越过原范围。

参数指纹按实例计算，包含解析后的 profile、region、桶、表、前缀、字段、模式（dry-run/apply）、范围参数和 `--all`。中断后用与原来相同的参数并增加 `--resume`：

```bash
python3 backfill_ddb_contact_summaries.py --apply --limit 100 --resume
```

`--resume` 对每个实例分别处理：checkpoint 已完成（列举结束或已达 `--limit`）的实例跳过并提示；进行中的实例从断点继续，若存在 pending Contact 则先重放；没有 checkpoint 的实例提示后从头开始（仅 `--apply --resume` 无其他范围时例外，见“写入确认”）。任一实例的 checkpoint 无法读取、版本不兼容或参数指纹不匹配时，在任何 S3/DynamoDB 访问之前整体拒绝（退出码 2），避免换表、换字段或从 dry-run 意外切到 apply。旧版本工具生成的 checkpoint（版本 2）不兼容，需重新开始一个受限批次。

已有 Contact 失败清单会明确保留，增加 `--retry-failures`（需配合 `--resume`）会先重试该实例自己的 Contact 失败，再处理新 key；listing 失败不进入 Contact 重试队列，而是在恢复分页列举成功时自动结算。checkpoint 不是旧值备份。

### 验证和回滚

1. 先在测试表或 staging 表运行 dry-run，核对 `changed/missing/failed` 统计和脱敏 diff。
2. 对一个已知 Contact 执行 apply，并通过 DynamoDB 只读查询验证选定字段；确认其他字段与 `generatedAt` 未变化。
3. 再以低 `--rate-limit` 和小 batch 扩大范围，保留 checkpoint/失败报告并检查 CloudWatch/DynamoDB 节流指标。
4. apply 前启用并确认 DynamoDB PITR/按需备份，或单独导出所选字段旧值。回滚应从该备份/字段快照恢复；checkpoint 无法回滚值。

本工具不会删除或重建表、重放 CloudWatch、覆盖完整 item、改写 S3 events、部署基础设施，也不会把 apply 设为默认值。它不会实际恢复已被删除的 DynamoDB item；此类 item 会报告为 `missing`，需先从备份恢复 item 后再回填。

## 7. 注意事项

- 区域固定为 us-east-1,账号 ID 取自第一个实例 ARN;config 中的实例应同属一个账号,且需有名为该账号 ID 的 AWS profile。
- 拉取某个日志组失败时,该实例会被跳过并在汇总中显示错误,其余实例继续;修复后重跑即可。
- Lambda 解析报错时,脚本会提示查看日志:`aws logs tail /aws/lambda/connect-ai-logs-<account_id>-splitter --region us-east-1 --follow`。
- `raw/` 与 `trigger/` 下的原始数据 7 天后自动删除;解析结果保存在 DynamoDB 与 `events/` 前缀下。
- 新建 CloudFront 分配后通常需要几分钟才变为 `Deployed`,期间站点可能无法访问。
- 脚本**不提供**删除/清理资源的功能,如需移除需自行在 AWS 中逐项删除上述资源。
- `config-*.json` 含邮箱与账号信息,请勿提交到公开仓库。
