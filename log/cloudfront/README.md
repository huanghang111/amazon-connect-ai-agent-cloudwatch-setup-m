# setup_cloudfront.py 使用说明

## 1. 脚本作用

`setup_cloudfront.py` 是「Connect AI Agent 日志分析」Web 应用的一键部署脚本。它把 Amazon Connect AI Agent 日志(以及可选的 AgentCore Gateway 日志)从 CloudWatch Logs 归档到 S3,由云端 Lambda 解析后写入 DynamoDB / S3,并把前端站点通过 CloudFront 发布出来,用 Cognito 登录鉴权。支持多个 Connect 实例,数据按实例名(`InstanceAlias`,即 DynamoDB 分区键 `instanceName`)隔离。全程不在本地处理日志文件。

### 创建/配置的 AWS 资源

以下名称中的 `<account_id>` 取自 config 中**第一个实例**的 `instance_arn`。所有资源统一部署在 **us-east-1**(区域在代码中固定)。

| 资源 | 名称 / 说明 |
| --- | --- |
| S3 日志桶 | `connect-ai-logs-<account_id>`;`raw/`、`trigger/` 前缀 7 天后自动过期;配置了 CORS(GET/HEAD);`trigger/*.json` 的创建事件触发解析 Lambda |
| S3 Web 桶 | `connect-ai-logs-<account_id>-web`;关闭公共访问,仅允许该 CloudFront 分配经 OAC 读取 |
| DynamoDB 表 | `connect-ai-contacts-<account_id>`(分区键 `instanceName`,排序键 `contactId`;GSI `by-instance-time`:`instanceName` + `minTs`;按需计费) |
| 解析 Lambda | `connect-ai-logs-<account_id>-splitter`(python3.12,超时 900s,内存由 `--lambda-memory` 控制,异步重试次数 0);角色 `connect-ai-logs-<account_id>-lambda-role` |
| 查询 Lambda | `connect-ai-logs-<account_id>-query`(python3.12,超时 30s,内存 1024MB);角色 `connect-ai-logs-<account_id>-query-role` |
| API Gateway | REST API `connect-ai-logs-<account_id>-api`,stage `prod`,Cognito Authorizer `cognito-auth` |
| Cognito 用户池 | `connect-ai-agent-logs-<account_id>`(仅管理员可创建用户,邮箱作用户名,含自定义属性 `custom:instances`)及应用客户端 `connect-ai-agent-logs-<account_id>-web`(无 secret,启用 `USER_PASSWORD_AUTH`) |
| CloudFront | OAC `connect-ai-logs-<account_id>-oac`;分配的 Comment 为 `connect-ai-agent-logs-<account_id>`,HTTPS 重定向,默认根对象 `index.html`,403/404 回落到 `/index.html` |

API 路由(均转发到查询 Lambda):`GET /instances`、`GET /contacts`、`GET /contacts/{contactId}/events`、`GET /contacts/{contactId}/describe`(以上需 Cognito IdToken),以及 `POST /translate`(代码中为无 API 鉴权,Bedrock 权限由查询 Lambda 角色控制)。

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

浏览器 → CloudFront(静态站点, auth.js 登录 Cognito)
       → API Gateway (IdToken) → 查询 Lambda → DynamoDB / S3 预签名 URL / Connect DescribeContact / Bedrock 翻译
```

## 2. 前置条件

- **Python**:`python3`(代码未声明最低版本;解析 Lambda 运行时为 python3.12)。
- **依赖包**:`boto3`(`pip install boto3`)。
- **AWS 凭证**:脚本把 **AWS profile 名固定为账号 ID**(即 config 第一个实例 ARN 中的账号),主脚本和 `fetch-to-s3.py` 子进程都使用 `--profile <account_id>`。因此本机 `~/.aws/config` / `credentials` 中需要存在名为 `<account_id>` 的 profile。
- **权限**:该 profile 需要能操作以下服务(脚本里实际调用到的):S3、IAM(创建/更新角色及内联策略)、Lambda、DynamoDB、Cognito User Pools、API Gateway、CloudFront(OAC、分配、失效)、CloudWatch Logs(`filter_log_events`)、Amazon Connect(`DescribeInstance`、`ListInstanceStorageConfigs`)。
- **需要预先存在的资源**:
  - 各 Connect 实例,且能查询到其 `InstanceAlias`(查不到会直接退出)。
  - `ai_log_arn` 对应的 AI Agent 日志组。可用上一级目录的 `../setup_connect_ai_agent_logs.py` 创建(它会建立日志投递并创建日志组 `/aws/connect/ai-agent-logs-<InstanceAlias>`,并读取本目录下的 `config-*.json`)。
  - `gateway_log_arn`(可选)对应的 AgentCore Gateway 应用日志组。
- **依赖文件**(缺失会报错退出,见下方目录结构):`lib/fetch-to-s3.py`、`lib/lambda/handler.py`、`lib/parse-connect-ai-logs.py`、`lib/web-cloudfront/auth.js`;另外会用到 `lib/lambda/query_handler.py` 与 `lib/web/*`。
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
| `--out-dir <dir>` | 系统临时目录 | 临时部署产物目录(`lambda.zip`、`query_lambda.zip`) |
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
2. 创建/复用日志桶,配置 Lifecycle 与 CORS;创建/复用 DynamoDB 表(缺 GSI 时补建)。
3. 创建/更新解析 Lambda 及其角色,配置 S3 `trigger/*.json` 事件通知。
4. 对每个实例查询 `InstanceAlias`(失败则退出),并尝试查询通话录音桶(失败只忽略)。
5. 并发启动 `fetch-to-s3.py` 拉取各实例的 AI Agent / Gateway 日志到 `raw/<key>/`;任一拉取失败的实例会被跳过。
6. 删除旧的 `<key>/index.json`,写入 `trigger/<key>.json` 触发 Lambda,轮询 `index.json`(最长约 16 分钟,超时则提示 Lambda 仍在后台处理)。
7. 创建/复用 Cognito 用户池与应用客户端;创建/更新查询 Lambda;配置并部署 API Gateway(`prod`);创建登录用户。
8. 上传前端文件到 Web 桶,创建/复用 OAC 与 CloudFront 分配,写 Web 桶策略,发起 `/*` 失效,等待 CloudFront 变为 `Deployed`(最长等 1200 秒,超时则提示稍后用 `aws cloudfront get-distribution` 查看)。

### 幂等性 / 重复执行

脚本可重复执行:S3 桶、DynamoDB 表、IAM 角色、Lambda、Cognito 用户池/客户端、API Gateway、OAC、CloudFront 分配均按名称检查并复用或更新。需要注意:

- 重跑会**重新拉取日志并重新触发解析**,已存在的 Contact 摘要与事件明细会被覆盖写入。
- 重跑会重新部署 Lambda 代码与配置,并更新前端文件和 `aws-config.js`。
- 已存在的登录用户不会重新创建(不会重发一次性密码)。
- 日志桶的事件通知配置会被整体设置为本脚本的配置。

## 4. 执行后的产出

脚本结束时在终端打印汇总,包括:

- CloudFront 访问地址 `https://<xxxx>.cloudfront.net`、分配 ID 与部署状态。
- 登录邮箱、每个实例的处理结果(Contact 数量 / 解析报错 / Lambda 处理中)。
- 资源清单:日志桶、Web 桶、解析/查询 Lambda 名、Cognito 用户池 ID 与应用客户端 ID、API Gateway URL。

**生成的内容**:

- 本地:仅有临时的 `lambda.zip`、`query_lambda.zip`(默认执行结束即删除,`--keep` 保留)。脚本不会修改本目录下的任何源文件。
- Web 桶中的文件:`index.html`(由 `lib/web/index.html` 改写:移除 `data.js` 引用,改为加载 AWS SDK、`aws-config.js`、`auth.js`,并给脚本加版本号)、`app.js`、`i18n.js`、`site-config.js`、`auth.js`、`aws-config.js`,以及 `connect-enrich.js`(`lib/web/` 下没有该文件时上传一个空占位)。
- `aws-config.js` 由脚本即时生成并直接上传(不落本地),包含区域、用户池 ID、应用客户端 ID、API URL、第一个实例的 ID/ARN/区域、`csatAttribute` 等运行时配置。

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
| `lib/lambda/sync_log_handler.py` | 独立的 Lambda 入口,按实例拉取日志并触发解析,前置条件是本脚本已创建好基础设施;本脚本不部署它 |
| `lib/lambda/faq_retrieval_log.py` | 定时 FAQ 分析 Lambda(查询 CloudWatch 日志并上传到外部 FAQ 检索日志服务);本脚本不部署它 |
| `lib/web/` | 前端文件:`index.html`、`app.js`、`i18n.js`、`site-config.js` |
| `lib/web-cloudfront/auth.js` | CloudFront 版登录门禁与数据加载脚本 |

上一级目录的 `../setup_connect_ai_agent_logs.py`:为 Connect 实例配置 AI Agent 日志投递到 CloudWatch Logs,是本脚本的上游(先有日志,再用本脚本部署分析站点)。

## 6. 注意事项

- 区域固定为 us-east-1,账号 ID 取自第一个实例 ARN;config 中的实例应同属一个账号,且需有名为该账号 ID 的 AWS profile。
- 拉取某个日志组失败时,该实例会被跳过并在汇总中显示错误,其余实例继续;修复后重跑即可。
- Lambda 解析报错时,脚本会提示查看日志:`aws logs tail /aws/lambda/connect-ai-logs-<account_id>-splitter --region us-east-1 --follow`。
- `raw/` 与 `trigger/` 下的原始数据 7 天后自动删除;解析结果保存在 DynamoDB 与 `events/` 前缀下。
- 新建 CloudFront 分配后通常需要几分钟才变为 `Deployed`,期间站点可能无法访问。
- 脚本**不提供**删除/清理资源的功能,如需移除需自行在 AWS 中逐项删除上述资源。
- `config-*.json` 含邮箱与账号信息,请勿提交到公开仓库。
