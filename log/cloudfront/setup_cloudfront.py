#!/usr/bin/env python3
"""
setup_cloudfront.py

用途:
  把 Connect AI Agent 日志分析 Web 应用部署到 Amazon CloudFront，
  并使用 Amazon Cognito 做登录鉴权。支持多 Connect 实例，日志按
  instanceName(InstanceAlias)隔离写入 DynamoDB。

  流程概览(全程不在本地处理任何日志文件):
    1) 从 config.json 读取所有 Connect 实例配置（email、hours、每个实例的 ARN）。
    2) 创建共享基础设施（一次性）：日志桶、DynamoDB 两张表、Lambda（解析函数）、
       Cognito 用户池/身份池/鉴权角色、CloudFront 分配。
    3) 对每个实例循环执行：
         a. 查询实例的 InstanceAlias 作为 instanceName（DynamoDB 分区键）。
         b. 把 AI Agent 日志与 Gateway 日志直传到日志桶的 raw/<inst-key>/ 前缀。
         c. 写 trigger/<inst-key>.json（含 instanceName、connect/gateway S3 key、prefix）。
         d. S3 事件触发 Lambda 在云端解析，按 instanceName 写入 DynamoDB。
         e. 等待 Lambda 生成 <prefix>index.json 完成。
    4) 把 Web 应用发布到独立 S3 桶并通过 CloudFront(OAC) 对外提供。
       登录成功后用 Cognito Identity Pool 换取临时凭证，从 DynamoDB 按
       instanceName 加载 Contact 摘要列表。

用法:
  python3 setup_cloudfront.py [--config config.json] [--out-dir <dir>] [--keep]
      [--lambda-memory <MB>] [--csat-attr <key>] [-h]

参数:
  --config <file>        实例配置文件路径；默认 config.json
                         格式: {"email":"...", "hours": 6,
                                "connect": {"key": {"instance_arn":"...",
                                                    "ai_log_arn":"...",
                                                    "gateway_log_arn":"..."}}}
  --out-dir <dir>        临时部署产物目录；默认系统临时目录
  --keep                 保留临时产物目录(默认结束后自动清理)
  --lambda-memory <MB>   解析 Lambda 内存；默认 1024
  --csat-attr <key>      CSAT 满意度评分对应的联系人属性键名；默认 botevaluation
  -h, --help             显示帮助

依赖: python3、boto3(pip install boto3)。
"""

import argparse
import atexit
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# CloudFront 托管缓存策略 CachingOptimized 的固定 ID
CF_CACHE_POLICY_ID = "658327ea-f89d-4fab-a63d-7e88639e58f6"
# 浏览器版 AWS SDK v2
AWS_SDK_URL = "https://sdk.amazonaws.com/js/aws-sdk-2.1691.0.min.js"

# 脚本所在目录的相对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LIB_DIR = os.path.join(SCRIPT_DIR, "lib")
WEB_DIR = os.path.join(LIB_DIR, "web")
WEB_CF_DIR = os.path.join(LIB_DIR, "web-cloudfront")
FETCHER = os.path.join(LIB_DIR, "fetch-to-s3.py")
LAMBDA_HANDLER = os.path.join(LIB_DIR, "lambda", "handler.py")
QUERY_HANDLER = os.path.join(LIB_DIR, "lambda", "query_handler.py")
PARSER_MODULE = os.path.join(LIB_DIR, "parse-connect-ai-logs.py")


# ---------------------------------------------------------------------------
# boto3 客户端工厂
# ---------------------------------------------------------------------------

_AWS_PROFILE = None  # main() 里根据 account_id 设置


def _session(region=None):
    """根据 region 和全局 profile 创建 boto3 Session。"""
    kw = {}
    if region:
        kw["region_name"] = region
    if _AWS_PROFILE:
        kw["profile_name"] = _AWS_PROFILE
    return boto3.Session(**kw)


def _client(service, region=None):
    return _session(region).client(service)


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def arn_region(arn):
    return arn.split(":")[3]


def arn_account(arn):
    return arn.split(":")[4]


def arn_log_group(arn):
    # arn:aws:logs:<region>:<account>:log-group:<name>[:*]
    rest = arn.split(":log-group:", 1)[1]
    return rest.rstrip(":*").rstrip(":")


def validate_bucket(name):
    if not re.match(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$", name):
        print(f"错误: 生成的桶名不合法: {name}", file=sys.stderr)
        print("       请调整资源命名常量(代码顶部)。", file=sys.stderr)
        sys.exit(1)


def bucket_exists(s3, name):
    """检查 S3 桶是否存在。"""
    try:
        s3.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def create_bucket(s3, name, region):
    """创建 S3 桶并关闭公共访问。"""
    print(f"==> 创建 S3 桶: {name}")
    if region == "us-east-1":
        s3.create_bucket(Bucket=name)
    else:
        s3.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    # 关闭公共访问(站点经 CloudFront/OAC，日志经 Cognito 凭证)
    s3.put_public_access_block(
        Bucket=name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def s3_put(s3, bucket, key, body, content_type="text/plain; charset=utf-8"):
    """向 S3 上传内容(bytes 或 str)。"""
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def s3_upload_file(s3, bucket, key, local_path, content_type):
    """上传本地文件到 S3。"""
    with open(local_path, "rb") as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f.read(), ContentType=content_type)


def s3_object_exists(s3, bucket, key):
    """检查 S3 对象是否存在。"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def iam_role_exists(iam, role_name):
    try:
        iam.get_role(RoleName=role_name)
        return True
    except ClientError:
        return False


def lambda_function_exists(lam, name):
    try:
        lam.get_function(FunctionName=name)
        return True
    except ClientError:
        return False


# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------


def parse_args():
    ap = argparse.ArgumentParser(
        description="将 Connect AI Agent 日志分析部署到 CloudFront(Cognito 鉴权，多实例支持)。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
        add_help=False,
    )
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--lambda-memory", default="1024")
    ap.add_argument(
        "--csat-attr", default=os.environ.get("CSAT_ATTRIBUTE_KEY", "botevaluation")
    )
    ap.add_argument("-h", "--help", action="help")
    return ap.parse_args()


# ---------------------------------------------------------------------------
# 主函数
# ---------------------------------------------------------------------------


def main():
    args = parse_args()

    # --- 依赖检查 ---
    for f, label in [
        (FETCHER, "lib/fetch-to-s3.py"),
        (LAMBDA_HANDLER, "lib/lambda/handler.py"),
        (PARSER_MODULE, "lib/parse-connect-ai-logs.py"),
        (os.path.join(WEB_CF_DIR, "auth.js"), "lib/web-cloudfront/auth.js"),
    ]:
        if not os.path.isfile(f):
            print(f"错误: 找不到 {label}，路径: {f}", file=sys.stderr)
            sys.exit(1)

    # --- 选择 config 文件 ---
    # 若 --config 未显式指定（仍为默认值 config.json），则扫描目录下 config-*.json 供交互选择
    config_arg = args.config
    if config_arg == "config.json":
        candidates = sorted(
            f
            for f in os.listdir(SCRIPT_DIR)
            if f.startswith("config-") and f.endswith(".json")
        )
        if not candidates:
            print(
                "错误: 未找到任何 config-*.json 文件，请先创建账号配置文件。",
                file=sys.stderr,
            )
            sys.exit(1)
        if len(candidates) == 1:
            config_arg = candidates[0]
            print(f"==> 自动使用唯一配置文件: {config_arg}")
        else:
            print("请选择要部署的账号配置文件:")
            for i, name in enumerate(candidates, 1):
                print(f"  {i}) {name}")
            while True:
                raw = input(f"请输入序号 [1-{len(candidates)}]: ").strip()
                if raw.isdigit() and 1 <= int(raw) <= len(candidates):
                    config_arg = candidates[int(raw) - 1]
                    break
                print(f"  请输入 1 到 {len(candidates)} 之间的数字。")
            print(f"==> 已选择: {config_arg}")

    # --- 读取 config 文件 ---
    config_path = os.path.join(SCRIPT_DIR, config_arg)
    if not os.path.isfile(config_path):
        print(f"错误: 找不到配置文件: {config_path}", file=sys.stderr)
        sys.exit(1)
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)

    email = cfg.get("email", "").strip()
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        print(f"错误: config.json 中 email 格式不正确: {email}", file=sys.stderr)
        sys.exit(1)

    hours = str(cfg.get("hours", 0))

    connect_instances = cfg.get("connect", {})
    if not connect_instances:
        print(
            "错误: config.json 中 connect 字典为空，没有可处理的实例。", file=sys.stderr
        )
        sys.exit(1)

    lambda_memory = int(args.lambda_memory)
    csat_attribute_key = args.csat_attr

    # --- 部署区域固定为 us-east-1；账号 ID 从第一个实例 ARN 解析 ---
    # 所有基础设施（S3/DDB/Lambda/Cognito/CloudFront）统一部署在 us-east-1。
    # fetch-to-s3.py 通过 --s3-region 参数将任意 region 的日志直传到 us-east-1 的 S3 桶。
    region = "us-east-1"
    first_inst_arn = next(iter(connect_instances.values()))["instance_arn"]
    account_id = arn_account(first_inst_arn)

    # 切换到对应账号的 AWS profile（profile 名 = account_id）
    global _AWS_PROFILE
    _AWS_PROFILE = account_id
    print(f"==> Using AWS profile: {account_id}")

    # --- 资源命名: 以账号 ID 为后缀，同一套代码部署多个账号时互不冲突 ---
    acct = account_id  # 完整账号 ID，如 154305462659
    logs_bucket = f"connect-ai-logs-{acct}"
    web_bucket = f"connect-ai-logs-{acct}-web"
    user_pool_name = f"connect-ai-agent-logs-{acct}"
    identity_pool_name = f"connect_ai_agent_logs_{acct}"
    auth_role_name = f"connect-ai-logs-{acct}-auth-role"
    lambda_name = f"connect-ai-logs-{acct}-splitter"
    lambda_role_name = f"connect-ai-logs-{acct}-lambda-role"
    query_lambda_name = f"connect-ai-logs-{acct}-query"
    query_role_name = f"connect-ai-logs-{acct}-query-role"
    api_name = f"connect-ai-logs-{acct}-api"
    contacts_table = f"connect-ai-contacts-{acct}"
    # events_table 已废弃，事件明细改为存 S3 events/{instanceName}/{contactId}.json.gz

    validate_bucket(logs_bucket)
    validate_bucket(web_bucket)

    print("===================================================================")
    print(" 部署配置")
    print(f"   区域(region)      : {region}")
    print(f"   账号(account)     : {account_id}")
    print(f"   实例数量          : {len(connect_instances)}")
    for k, v in connect_instances.items():
        print(f"     · {k}  ({arn_region(v['instance_arn'])})")
    print(f"   日志存储桶        : {logs_bucket}")
    print(f"   Web 存储桶        : {web_bucket}")
    print(f"   解析 Lambda 内存  : {lambda_memory} MB")
    print(f"   登录邮箱          : {email}")
    print(
        f"   拉取范围          : {'全部历史' if hours == '0' else f'最近 {hours} 小时'}"
    )
    print("===================================================================")

    # --- 临时产物目录 ---
    out_dir = args.out_dir.strip()
    if out_dir:
        work = out_dir
        os.makedirs(work, exist_ok=True)
    else:
        work = tempfile.mkdtemp(prefix="connect-ai-cf.")

    def cleanup():
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)

    atexit.register(cleanup)

    # 初始化常用 boto3 客户端
    s3 = _client("s3", region)
    iam = _client("iam")  # IAM 是全局服务，不需要指定 region
    lam = _client("lambda", region)
    cf = _client("cloudfront")  # CloudFront 是全局服务
    cog_idp = _client("cognito-idp", region)
    apigw = _client("apigateway", region)
    ddb = _client("dynamodb", region)

    # -----------------------------------------------------------------------
    # 1. 计算起始时间
    # -----------------------------------------------------------------------
    start_ms = ""
    if hours != "0":
        start_ms = str(int((time.time() - int(hours) * 3600) * 1000))

    # -----------------------------------------------------------------------
    # 2. 日志桶 + Lifecycle(TTL)
    # -----------------------------------------------------------------------
    if bucket_exists(s3, logs_bucket):
        print(f"==> 日志桶已存在，复用: {logs_bucket}")
    else:
        create_bucket(s3, logs_bucket, region)

    # 设置 Lifecycle 规则: raw/ 和 trigger/ 前缀的对象 7 天后自动删除
    print("==> 配置日志桶 Lifecycle(raw/ + trigger/ 7天后自动删除)...")
    s3.put_bucket_lifecycle_configuration(
        Bucket=logs_bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-raw",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "raw/"},
                    "Expiration": {"Days": 7},
                },
                {
                    "ID": "expire-trigger",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "trigger/"},
                    "Expiration": {"Days": 7},
                },
            ]
        },
    )

    # 设置日志桶 CORS: 允许 CloudFront 前端跨域 fetch presigned URL(events/ 前缀)
    print("==> 配置日志桶 CORS(允许浏览器跨域 fetch events/)...")
    s3.put_bucket_cors(
        Bucket=logs_bucket,
        CORSConfiguration={
            "CORSRules": [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag", "Content-Encoding", "Content-Length"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        },
    )

    # -----------------------------------------------------------------------
    # 2b. 创建 DynamoDB 表(Contact 摘要表 + 事件明细表)
    # -----------------------------------------------------------------------
    def _ensure_ddb_table(table_name, key_schema, attr_defs, gsi_list=None):
        """创建 DynamoDB 表(不存在则创建，存在则跳过)，并等待其变为 ACTIVE。
        gsi_list: 需要存在的 GSI 列表，每项为 dict，含 IndexName/KeySchema/Projection。
        表已存在时检查缺失的 GSI 并补建。
        """
        try:
            desc = ddb.describe_table(TableName=table_name)["Table"]
            status = desc["TableStatus"]
            print(f"==> DynamoDB 表已存在，复用: {table_name} (状态: {status})")

            # 检查并补建缺失的 GSI
            if gsi_list:
                existing_gsi_names = {
                    g["IndexName"] for g in desc.get("GlobalSecondaryIndexes", [])
                }
                for gsi in gsi_list:
                    if gsi["IndexName"] in existing_gsi_names:
                        continue
                    print(f"    补建 GSI: {gsi['IndexName']} ...")
                    # 收集 GSI 键属性定义（主表 attr_defs 里可能已有，去重后合并）
                    existing_attr_names = {a["AttributeName"] for a in attr_defs}
                    extra_attrs = [
                        a
                        for a in gsi.get("AttributeDefinitions", [])
                        if a["AttributeName"] not in existing_attr_names
                    ]
                    ddb.update_table(
                        TableName=table_name,
                        AttributeDefinitions=attr_defs + extra_attrs,
                        GlobalSecondaryIndexUpdates=[
                            {
                                "Create": {
                                    "IndexName": gsi["IndexName"],
                                    "KeySchema": gsi["KeySchema"],
                                    "Projection": gsi["Projection"],
                                }
                            }
                        ],
                    )
                    # 等待 GSI 变为 ACTIVE
                    for _ in range(60):
                        d = ddb.describe_table(TableName=table_name)["Table"]
                        gsi_statuses = {
                            g["IndexName"]: g["IndexStatus"]
                            for g in d.get("GlobalSecondaryIndexes", [])
                        }
                        if gsi_statuses.get(gsi["IndexName"]) == "ACTIVE":
                            break
                        print(f"    等待 GSI {gsi['IndexName']} 变为 ACTIVE …")
                        time.sleep(5)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            print(f"==> 创建 DynamoDB 表: {table_name}")
            create_kwargs = dict(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attr_defs,
                BillingMode="PAY_PER_REQUEST",
            )
            if gsi_list:
                # 合并所有 GSI 需要的额外属性定义
                all_attr_names = {a["AttributeName"] for a in attr_defs}
                extra = []
                for gsi in gsi_list:
                    for a in gsi.get("AttributeDefinitions", []):
                        if a["AttributeName"] not in all_attr_names:
                            extra.append(a)
                            all_attr_names.add(a["AttributeName"])
                if extra:
                    create_kwargs["AttributeDefinitions"] = attr_defs + extra
                create_kwargs["GlobalSecondaryIndexes"] = [
                    {
                        "IndexName": gsi["IndexName"],
                        "KeySchema": gsi["KeySchema"],
                        "Projection": gsi["Projection"],
                    }
                    for gsi in gsi_list
                ]
            ddb.create_table(**create_kwargs)

        # 等待表变为 ACTIVE(最长 120 秒)
        for _ in range(24):
            st = ddb.describe_table(TableName=table_name)["Table"]["TableStatus"]
            if st == "ACTIVE":
                break
            print(f"    等待 {table_name} 变为 ACTIVE(当前: {st})…")
            time.sleep(5)

    # Contact 摘要表: 分区键 instanceName(S) + 排序键 contactId(S)
    # GSI by-instance-time: 分区键 instanceName + 排序键 minTs(N)，用于按时间倒序列表查询
    _ensure_ddb_table(
        contacts_table,
        key_schema=[
            {"AttributeName": "instanceName", "KeyType": "HASH"},
            {"AttributeName": "contactId", "KeyType": "RANGE"},
        ],
        attr_defs=[
            {"AttributeName": "instanceName", "AttributeType": "S"},
            {"AttributeName": "contactId", "AttributeType": "S"},
        ],
        gsi_list=[
            {
                "IndexName": "by-instance-time",
                "KeySchema": [
                    {"AttributeName": "instanceName", "KeyType": "HASH"},
                    {"AttributeName": "minTs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "AttributeDefinitions": [
                    {"AttributeName": "minTs", "AttributeType": "N"},
                ],
            }
        ],
    )
    # 事件明细表已废弃，事件改为写 S3 events/{instanceName}/{contactId}.json.gz

    # -----------------------------------------------------------------------
    # 2c. Lambda: 执行角色 + 部署包 + 函数 + S3 触发权限 + 桶事件通知
    # -----------------------------------------------------------------------
    lambda_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    lambda_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": [f"arn:aws:s3:::{logs_bucket}/*"],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:Scan",
                    "dynamodb:UpdateItem",
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{contacts_table}",
                ],
            },
            {
                "Effect": "Allow",
                "Action": ["connect:DescribeContact"],
                "Resource": [f"arn:aws:connect:*:{account_id}:instance/*/contact/*"],
            },
        ],
    }

    if iam_role_exists(iam, lambda_role_name):
        print(f"==> 复用 Lambda 执行角色: {lambda_role_name}")
        iam.update_assume_role_policy(
            RoleName=lambda_role_name,
            PolicyDocument=json.dumps(lambda_trust),
        )
        lambda_role_arn = iam.get_role(RoleName=lambda_role_name)["Role"]["Arn"]
    else:
        print(f"==> 创建 Lambda 执行角色: {lambda_role_name}")
        lambda_role_arn = iam.create_role(
            RoleName=lambda_role_name,
            AssumeRolePolicyDocument=json.dumps(lambda_trust),
        )["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=lambda_role_name,
        PolicyName="logs-bucket-rw",
        PolicyDocument=json.dumps(lambda_policy),
    )

    # 打包 Lambda 部署包(handler.py + connect_parser.py)
    lambda_zip_path = os.path.join(work, "lambda.zip")
    with zipfile.ZipFile(lambda_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(LAMBDA_HANDLER, "handler.py")
        zf.write(PARSER_MODULE, "connect_parser.py")
    with open(lambda_zip_path, "rb") as f:
        lambda_zip_bytes = f.read()

    # 等待 IAM 角色最终一致性
    print("    等待 IAM 角色生效(8s)...")
    time.sleep(8)

    # Lambda 环境变量: 仅含表名，不含 INSTANCE_NAME(多实例共用，instanceName 由 trigger JSON 传入)
    lambda_env_vars = {
        "CONTACTS_TABLE": contacts_table,
        "LOGS_BUCKET": logs_bucket,
    }

    if lambda_function_exists(lam, lambda_name):
        print(f"==> 更新 Lambda: {lambda_name}")
        lam.update_function_code(FunctionName=lambda_name, ZipFile=lambda_zip_bytes)
        # 必须等到 LastUpdateStatus == Successful 才能再次调用 update_function_configuration
        for _ in range(60):
            cfg = lam.get_function_configuration(FunctionName=lambda_name)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        lam.update_function_configuration(
            FunctionName=lambda_name,
            Runtime="python3.12",
            Role=lambda_role_arn,
            Handler="handler.handler",
            Timeout=900,
            MemorySize=lambda_memory,
            Environment={"Variables": lambda_env_vars},
        )
        for _ in range(60):
            cfg = lam.get_function_configuration(FunctionName=lambda_name)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
    else:
        print(f"==> 创建 Lambda: {lambda_name}")
        for attempt in range(1, 7):
            try:
                lam.create_function(
                    FunctionName=lambda_name,
                    Runtime="python3.12",
                    Role=lambda_role_arn,
                    Handler="handler.handler",
                    Timeout=900,
                    MemorySize=lambda_memory,
                    Code={"ZipFile": lambda_zip_bytes},
                    Environment={"Variables": lambda_env_vars},
                )
                break
            except ClientError as e:
                if attempt >= 6:
                    print(
                        "错误: 创建 Lambda 失败(执行角色可能尚未生效)。请稍后重跑本脚本。",
                        file=sys.stderr,
                    )
                    raise
                print(f"    等待执行角色生效，重试 ({attempt}/6) ...")
                time.sleep(5)

    lambda_arn = lam.get_function(FunctionName=lambda_name)["Configuration"][
        "FunctionArn"
    ]

    # 等待 Lambda 完全就绪(State=Active + LastUpdateStatus=Successful)
    print(f"    等待 Lambda {lambda_name} 就绪...")
    for _ in range(60):
        cfg = lam.get_function_configuration(FunctionName=lambda_name)
        if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(3)

    # 关闭异步调用重试(失败快速失败，不重跑整份解析)
    try:
        lam.put_function_event_invoke_config(
            FunctionName=lambda_name,
            MaximumRetryAttempts=0,
        )
    except ClientError:
        pass

    # 允许该日志桶触发此 Lambda(幂等: 先删同名声明，再添加)
    try:
        lam.remove_permission(FunctionName=lambda_name, StatementId="s3invoke")
        time.sleep(3)
    except ClientError:
        pass
    lam.add_permission(
        FunctionName=lambda_name,
        StatementId="s3invoke",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn=f"arn:aws:s3:::{logs_bucket}",
        SourceAccount=account_id,
    )

    # 轮询确认 resource policy 已写入，再继续配置桶事件通知
    print("    等待 Lambda resource policy 生效...")
    for _ in range(30):
        try:
            lam.get_policy(FunctionName=lambda_name)
            break
        except ClientError:
            time.sleep(3)

    # 配置桶事件通知: trigger/ 前缀且 .json 后缀 -> 触发 Lambda
    # S3 会验证 Lambda resource policy，权限刚写入时可能尚未传播，重试最多 6 次
    print("==> 配置日志桶事件通知 -> Lambda")
    notification_cfg = {
        "LambdaFunctionConfigurations": [
            {
                "Id": "process-raw-logs",
                "LambdaFunctionArn": lambda_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "trigger/"},
                            {"Name": "suffix", "Value": ".json"},
                        ]
                    }
                },
            }
        ]
    }
    for attempt in range(1, 7):
        try:
            s3.put_bucket_notification_configuration(
                Bucket=logs_bucket,
                NotificationConfiguration=notification_cfg,
            )
            break
        except ClientError as e:
            if attempt >= 6:
                print(f"错误: 配置桶事件通知失败: {e}", file=sys.stderr)
                raise
            print(f"    Lambda 权限尚未传播，重试 ({attempt}/6)，等待 10s ...")
            time.sleep(10)

    # -----------------------------------------------------------------------
    # 3. 对每个 Connect 实例拉取日志 -> 触发 Lambda
    # -----------------------------------------------------------------------
    def build_fetch_args(fetch_region, log_group, key, label):
        base = [
            sys.executable,
            FETCHER,
            "--region",
            fetch_region,
            "--log-group",
            log_group,
            "--bucket",
            logs_bucket,
            "--key",
            key,
            "--s3-region",
            region,
            "--label",
            label,
            "--profile",
            account_id,
        ]
        if start_ms:
            base += ["--start-ms", start_ms]
        return base

    # 收集录音桶（供 Query Lambda 读 Contact Lens 分析文件），以及所有 Connect 实例 ARN（供鉴权角色 Connect 权限）
    all_recordings_buckets = []
    all_connect_arns = []

    # 逐实例查询 InstanceAlias（预检，失败则退出，不要到拉日志阶段才失败）
    instance_names = {}  # inst_key -> InstanceAlias
    for inst_key, inst_cfg in connect_instances.items():
        inst_arn = inst_cfg["instance_arn"]
        inst_region = arn_region(inst_arn)
        inst_id = inst_arn.split("/")[-1]
        all_connect_arns.append(inst_arn)

        print(f"==> [{inst_key}] 查询 InstanceAlias ...")
        try:
            connect_cli = _client("connect", inst_region)
            resp = connect_cli.describe_instance(InstanceId=inst_id)
            alias = resp.get("Instance", {}).get("InstanceAlias", "") or ""
        except Exception:
            alias = ""
        if not alias:
            print(
                f"错误: [{inst_key}] 无法查询 InstanceAlias，请确认实例 ARN 与权限。",
                file=sys.stderr,
            )
            sys.exit(1)
        instance_names[inst_key] = alias
        print(f"    InstanceAlias: {alias}")

        # 查询录音桶（非关键，失败只警告）
        try:
            resp = connect_cli.list_instance_storage_configs(
                InstanceId=inst_id,
                ResourceType="CALL_RECORDINGS",
            )
            rb = (resp.get("StorageConfigs") or [{}])[0].get("S3Config", {}).get(
                "BucketName", ""
            ) or ""
            if rb and rb not in all_recordings_buckets:
                all_recordings_buckets.append(rb)
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # 3a. 并发拉取所有实例日志 -> 上传 S3
    # -----------------------------------------------------------------------
    # 每个实例同时启动子进程拉取 connect + gateway 日志，互不阻塞。
    # fetch_plan: inst_key -> {instance_name, prefix, raw_connect_key, raw_gateway_key,
    #                          procs: [(label, Popen), ...]}
    fetch_plan = {}
    for inst_key, inst_cfg in connect_instances.items():
        inst_arn = inst_cfg["instance_arn"]
        inst_region = arn_region(inst_arn)
        ai_log_arn = inst_cfg.get("ai_log_arn", "")
        gw_log_arn = inst_cfg.get("gateway_log_arn", "")
        instance_name = instance_names[inst_key]
        prefix = f"{inst_key}/"

        connect_lg = arn_log_group(ai_log_arn) if ai_log_arn else ""
        connect_region = arn_region(ai_log_arn) if ai_log_arn else inst_region
        gateway_lg = arn_log_group(gw_log_arn) if gw_log_arn else ""
        gateway_region = arn_region(gw_log_arn) if gw_log_arn else ""

        procs = []
        raw_connect_key = ""
        raw_gateway_key = ""

        if connect_lg:
            raw_connect_key = f"raw/{inst_key}/connect.ndjson"
            print(
                f"==> [{inst_key}] 启动拉取 AI Agent 日志 -> s3://{logs_bucket}/{raw_connect_key}"
            )
            procs.append(
                (
                    f"{inst_key}-connect",
                    subprocess.Popen(
                        build_fetch_args(
                            connect_region,
                            connect_lg,
                            raw_connect_key,
                            f"{inst_key}-connect",
                        ),
                    ),
                )
            )
        else:
            print(f"    [{inst_key}] 未配置 ai_log_arn，跳过 AI Agent 日志拉取。")

        if gw_log_arn and gateway_lg:
            raw_gateway_key = f"raw/{inst_key}/gateway.ndjson"
            print(
                f"==> [{inst_key}] 启动拉取 Gateway 日志  -> s3://{logs_bucket}/{raw_gateway_key}"
            )
            procs.append(
                (
                    f"{inst_key}-gateway",
                    subprocess.Popen(
                        build_fetch_args(
                            gateway_region,
                            gateway_lg,
                            raw_gateway_key,
                            f"{inst_key}-gateway",
                        ),
                    ),
                )
            )

        fetch_plan[inst_key] = {
            "instance_name": instance_name,
            "prefix": prefix,
            "raw_connect_key": raw_connect_key,
            "raw_gateway_key": raw_gateway_key,
            "procs": procs,
        }

    # 等待所有子进程完成
    print()
    print("==> 等待所有实例日志拉取完成...")
    fetch_failed = []
    for inst_key, plan in fetch_plan.items():
        for label, proc in plan["procs"]:
            rc = proc.wait()
            if rc != 0:
                print(f"  警告: [{label}] 拉取失败(exit={rc})", file=sys.stderr)
                fetch_failed.append(label)
            else:
                print(f"  [{label}] 拉取完成")
    if fetch_failed:
        print(f"  以下拉取任务失败，将跳过触发 Lambda: {fetch_failed}", file=sys.stderr)

    # -----------------------------------------------------------------------
    # 3b. 并发触发 Lambda（写 trigger JSON）并并发轮询结果
    # -----------------------------------------------------------------------
    instance_results = {}  # inst_key -> {"contact_count": ..., "error": ...}
    lock = threading.Lock()

    def trigger_and_poll(inst_key, plan):
        """触发单个实例的 Lambda 并轮询结果，写入 instance_results。"""
        raw_connect_key = plan["raw_connect_key"]
        raw_gateway_key = plan["raw_gateway_key"]
        instance_name = plan["instance_name"]
        prefix = plan["prefix"]

        if not raw_connect_key:
            with lock:
                instance_results[inst_key] = {
                    "contact_count": 0,
                    "error": "no ai_log_arn",
                }
            return

        # 检查拉取是否成功（有失败的 label 则跳过）
        failed_labels = [lbl for lbl, _ in plan["procs"] if lbl in fetch_failed]
        if failed_labels:
            with lock:
                instance_results[inst_key] = {
                    "contact_count": 0,
                    "error": f"fetch failed: {failed_labels}",
                }
            return

        # 删除旧 index.json
        try:
            s3.delete_object(Bucket=logs_bucket, Key=f"{prefix}index.json")
        except ClientError:
            pass

        # 写 trigger JSON
        inst_arn = connect_instances[inst_key]["instance_arn"]
        inst_id = inst_arn.split("/")[-1]
        inst_region = arn_region(inst_arn)
        trigger_body = {
            "instanceName": instance_name,
            "instanceId": inst_id,
            "connectRegion": inst_region,
            "csatAttrKey": csat_attribute_key,
            "connect": raw_connect_key,
            "prefix": prefix,
        }
        if raw_gateway_key:
            trigger_body["gateway"] = raw_gateway_key

        trigger_key = f"trigger/{inst_key}.json"
        print(f"==> [{inst_key}] 写入触发对象 {trigger_key}")
        s3_put(
            s3, logs_bucket, trigger_key, json.dumps(trigger_body), "application/json"
        )

        # 轮询等待 Lambda 生成 index.json（最长约 16 分钟）
        print(f"==> [{inst_key}] 等待 Lambda 生成 {prefix}index.json...")
        contact_count = "?"
        parse_error = ""
        for _ in range(200):
            if s3_object_exists(s3, logs_bucket, f"{prefix}index.json"):
                try:
                    idx = json.loads(
                        s3.get_object(Bucket=logs_bucket, Key=f"{prefix}index.json")[
                            "Body"
                        ].read()
                    )
                    contact_count = str(idx.get("contactCount", "?"))
                    parse_error = idx.get("error", "")
                except Exception:
                    pass
                break
            time.sleep(5)

        if parse_error:
            print(f"  [{inst_key}] Lambda 解析报错: {parse_error}")
            print(
                f"  查看日志: aws logs tail /aws/lambda/{lambda_name} "
                f"--region {region} --follow"
            )
        elif contact_count == "?":
            print(
                f"  [{inst_key}] Lambda 仍在后台处理，index.json 生成后站点即可加载数据。"
            )
        elif contact_count == "0":
            print(f"  [{inst_key}] 提示: 未解析出任何 Contact。")
        else:
            print(f"  [{inst_key}] 完成: {contact_count} 个 Contact")

        with lock:
            instance_results[inst_key] = {
                "contact_count": contact_count,
                "error": parse_error,
            }

    # 并发触发 + 轮询（每个实例一个线程）
    threads = [
        threading.Thread(target=trigger_and_poll, args=(k, v), daemon=True)
        for k, v in fetch_plan.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # -----------------------------------------------------------------------
    # 4. Amazon Cognito: 用户池 + 应用客户端 + 身份池 + 鉴权角色
    # -----------------------------------------------------------------------

    # 4.1 用户池
    resp = cog_idp.list_user_pools(MaxResults=60)
    existing_up = next(
        (p["Id"] for p in resp.get("UserPools", []) if p["Name"] == user_pool_name),
        None,
    )
    if existing_up:
        print(f"==> 复用已存在的用户池: {existing_up}")
        user_pool_id = existing_up
        # 确保 custom:instances 属性已存在（旧用户池可能没有）
        try:
            existing_schema = cog_idp.describe_user_pool(UserPoolId=user_pool_id)[
                "UserPool"
            ].get("SchemaAttributes", [])
            has_instances = any(
                a["Name"] == "custom:instances" for a in existing_schema
            )
            if not has_instances:
                print("    追加 custom:instances 自定义属性...")
                cog_idp.add_custom_attributes(
                    UserPoolId=user_pool_id,
                    CustomAttributes=[
                        {
                            "Name": "instances",
                            "AttributeDataType": "String",
                            "Mutable": True,
                            "StringAttributeConstraints": {
                                "MinLength": "0",
                                "MaxLength": "2048",
                            },
                        }
                    ],
                )
        except ClientError as e:
            print(
                f"    警告: 无法检查/追加 custom:instances 属性: {e}", file=sys.stderr
            )
    else:
        print(f"==> 创建 Cognito 用户池: {user_pool_name}")
        user_pool_id = cog_idp.create_user_pool(
            PoolName=user_pool_name,
            UsernameAttributes=["email"],
            AutoVerifiedAttributes=["email"],
            AdminCreateUserConfig={"AllowAdminCreateUserOnly": True},
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": False,
                }
            },
            Schema=[
                {
                    "Name": "instances",
                    "AttributeDataType": "String",
                    "Mutable": True,
                    "StringAttributeConstraints": {
                        "MinLength": "0",
                        "MaxLength": "2048",
                    },
                }
            ],
        )["UserPool"]["Id"]

    # 4.2 应用客户端(公共客户端，无 secret，启用 USER_PASSWORD_AUTH)
    # ReadAttributes 必须包含 custom:instances，否则 IdToken 里不会有该 claim
    resp = cog_idp.list_user_pool_clients(UserPoolId=user_pool_id, MaxResults=60)
    existing_client = next(
        (
            c["ClientId"]
            for c in resp.get("UserPoolClients", [])
            if c["ClientName"] == f"{user_pool_name}-web"
        ),
        None,
    )
    if existing_client:
        print(f"==> 复用已存在的应用客户端: {existing_client}")
        client_id = existing_client
        # 确保 ReadAttributes 含 custom:instances
        cog_idp.update_user_pool_client(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            ReadAttributes=["email", "custom:instances"],
            WriteAttributes=["custom:instances"],
            ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
            RefreshTokenValidity=1440,
        )
    else:
        print("==> 创建用户池应用客户端")
        client_id = cog_idp.create_user_pool_client(
            UserPoolId=user_pool_id,
            ClientName=f"{user_pool_name}-web",
            GenerateSecret=False,
            ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"],
            ReadAttributes=["email", "custom:instances"],
            WriteAttributes=["custom:instances"],
            RefreshTokenValidity=1440,
        )["UserPoolClient"]["ClientId"]

    # 4.3 Query Lambda: 执行角色 + 部署包 + 函数
    all_instance_names = list(instance_names.values())  # 全量实例名，供 * 权限展开

    # instanceName(alias) -> {instanceId, region, configKey}，供 Query Lambda / 前端显示使用
    instances_meta = {}
    for inst_key, alias in instance_names.items():
        inst_arn = connect_instances[inst_key]["instance_arn"]
        instances_meta[alias] = {
            "instanceId": inst_arn.split("/")[-1],
            "region": arn_region(inst_arn),
            "configKey": inst_key,
        }

    query_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    query_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
            {
                "Effect": "Allow",
                "Action": ["dynamodb:Query", "dynamodb:GetItem"],
                "Resource": [
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{contacts_table}",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{contacts_table}/index/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{logs_bucket}/events/*"],
            },
            {
                "Effect": "Allow",
                "Action": ["connect:DescribeContact"],
                # 各实例分布在不同 region，用通配符覆盖所有实例
                "Resource": [
                    f"arn:aws:connect:*:{account_id}:instance/*/contact/*",
                ],
            },
            # 调用 Bedrock 翻译（nova-lite 或其他模型）
            {
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel"],
                "Resource": [
                    "arn:aws:bedrock:*::foundation-model/*",
                    f"arn:aws:bedrock:*:{account_id}:inference-profile/*",
                ],
            },
            # 读取 Contact Lens 分析文件（与录音同桶，路径 Analysis/Voice/ 或 Analysis/Chat/）
            *(
                [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:ListBucket"],
                        "Resource": [
                            *[f"arn:aws:s3:::{rb}" for rb in all_recordings_buckets],
                            *[f"arn:aws:s3:::{rb}/*" for rb in all_recordings_buckets],
                        ],
                    }
                ]
                if all_recordings_buckets
                else []
            ),
        ],
    }

    if iam_role_exists(iam, query_role_name):
        print(f"==> 复用 Query Lambda 执行角色: {query_role_name}")
        iam.update_assume_role_policy(
            RoleName=query_role_name,
            PolicyDocument=json.dumps(query_trust),
        )
        query_role_arn = iam.get_role(RoleName=query_role_name)["Role"]["Arn"]
    else:
        print(f"==> 创建 Query Lambda 执行角色: {query_role_name}")
        query_role_arn = iam.create_role(
            RoleName=query_role_name,
            AssumeRolePolicyDocument=json.dumps(query_trust),
        )["Role"]["Arn"]
        print("    等待 IAM 角色生效(8s)...")
        time.sleep(8)
    iam.put_role_policy(
        RoleName=query_role_name,
        PolicyName="ddb-read-and-logs",
        PolicyDocument=json.dumps(query_policy),
    )

    # 打包 Query Lambda 部署包（只含 query_handler.py）
    query_zip_path = os.path.join(work, "query_lambda.zip")
    with zipfile.ZipFile(query_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(QUERY_HANDLER, "query_handler.py")
    with open(query_zip_path, "rb") as f:
        query_zip_bytes = f.read()

    query_env_vars = {
        "CONTACTS_TABLE": contacts_table,
        "LOGS_BUCKET": logs_bucket,
        "ALL_INSTANCES": ",".join(all_instance_names),
        "INSTANCES_META": json.dumps(instances_meta, ensure_ascii=False),
    }

    if lambda_function_exists(lam, query_lambda_name):
        print(f"==> 更新 Query Lambda: {query_lambda_name}")
        lam.update_function_code(
            FunctionName=query_lambda_name, ZipFile=query_zip_bytes
        )
        # 必须等到 LastUpdateStatus == Successful 才能再次调用 update_function_configuration，
        # 仅检查 State == Active 不够（更新进行中时 State 也是 Active）
        for _ in range(60):
            cfg = lam.get_function_configuration(FunctionName=query_lambda_name)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        lam.update_function_configuration(
            FunctionName=query_lambda_name,
            Runtime="python3.12",
            Role=query_role_arn,
            Handler="query_handler.handler",
            Timeout=30,
            MemorySize=1024,
            Environment={"Variables": query_env_vars},
        )
    else:
        print(f"==> 创建 Query Lambda: {query_lambda_name}")
        for attempt in range(1, 7):
            try:
                lam.create_function(
                    FunctionName=query_lambda_name,
                    Runtime="python3.12",
                    Role=query_role_arn,
                    Handler="query_handler.handler",
                    Timeout=30,
                    MemorySize=1024,
                    Code={"ZipFile": query_zip_bytes},
                    Environment={"Variables": query_env_vars},
                )
                break
            except ClientError:
                if attempt >= 6:
                    raise
                print(f"    等待执行角色生效，重试 ({attempt}/6) ...")
                time.sleep(5)

    query_lambda_arn = lam.get_function(FunctionName=query_lambda_name)[
        "Configuration"
    ]["FunctionArn"]

    # 4.4 API Gateway REST API + Cognito Authorizer + 路由 + 部署
    print("==> 配置 API Gateway REST API...")
    existing_apis = apigw.get_rest_apis().get("items", [])
    existing_api = next((a for a in existing_apis if a["name"] == api_name), None)
    if existing_api:
        print(f"    复用已存在的 API: {existing_api['id']}")
        api_id = existing_api["id"]
    else:
        print(f"    创建 REST API: {api_name}")
        api_id = apigw.create_rest_api(
            name=api_name,
            description=f"Connect AI 日志查询代理 ({acct})",
            endpointConfiguration={"types": ["REGIONAL"]},
        )["id"]

    # Cognito Authorizer（按名称复用）
    authorizers = apigw.get_authorizers(restApiId=api_id).get("items", [])
    existing_authorizer = next(
        (a for a in authorizers if a["name"] == "cognito-auth"), None
    )
    target_pool_arn = (
        f"arn:aws:cognito-idp:{region}:{account_id}:userpool/{user_pool_id}"
    )
    if existing_authorizer:
        authorizer_id = existing_authorizer["id"]
        # providerARNs 不支持 patchOperations replace，若 ARN 不匹配则删除重建
        current_arns = existing_authorizer.get("providerARNs", [])
        if target_pool_arn not in current_arns:
            apigw.delete_authorizer(restApiId=api_id, authorizerId=authorizer_id)
            authorizer_id = apigw.create_authorizer(
                restApiId=api_id,
                name="cognito-auth",
                type="COGNITO_USER_POOLS",
                providerARNs=[target_pool_arn],
                identitySource="method.request.header.Authorization",
            )["id"]
    else:
        authorizer_id = apigw.create_authorizer(
            restApiId=api_id,
            name="cognito-auth",
            type="COGNITO_USER_POOLS",
            providerARNs=[target_pool_arn],
            identitySource="method.request.header.Authorization",
        )["id"]

    # 允许 API Gateway 调用 Query Lambda（幂等：先删再加）
    try:
        lam.remove_permission(
            FunctionName=query_lambda_name, StatementId="apigw-invoke"
        )
    except ClientError:
        pass
    lam.add_permission(
        FunctionName=query_lambda_name,
        StatementId="apigw-invoke",
        Action="lambda:InvokeFunction",
        Principal="apigateway.amazonaws.com",
        SourceArn=f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*/*",
    )

    # 构建资源树 + 方法（幂等：已存在则复用）
    def _get_or_create_resource(parent_id, path_part):
        resources = apigw.get_resources(restApiId=api_id).get("items", [])
        existing = next(
            (
                r
                for r in resources
                if r.get("parentId") == parent_id and r.get("pathPart") == path_part
            ),
            None,
        )
        if existing:
            return existing["id"]
        return apigw.create_resource(
            restApiId=api_id, parentId=parent_id, pathPart=path_part
        )["id"]

    def _put_lambda_method(resource_id, http_method):
        """为资源添加 GET 方法（Cognito 鉴权 + Lambda proxy 集成）。"""
        try:
            apigw.get_method(
                restApiId=api_id, resourceId=resource_id, httpMethod=http_method
            )
        except ClientError:
            apigw.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=http_method,
                authorizationType="COGNITO_USER_POOLS",
                authorizerId=authorizer_id,
            )
        lambda_uri = (
            f"arn:aws:apigateway:{region}:lambda:path/2015-03-31"
            f"/functions/{query_lambda_arn}/invocations"
        )
        try:
            apigw.get_integration(
                restApiId=api_id, resourceId=resource_id, httpMethod=http_method
            )
        except ClientError:
            apigw.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod=http_method,
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )

    def _put_options_method(resource_id):
        """为资源添加 OPTIONS 方法（CORS 预检，无鉴权）。"""
        try:
            apigw.get_method(
                restApiId=api_id, resourceId=resource_id, httpMethod="OPTIONS"
            )
        except ClientError:
            apigw.put_method(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod="OPTIONS",
                authorizationType="NONE",
            )
        lambda_uri = (
            f"arn:aws:apigateway:{region}:lambda:path/2015-03-31"
            f"/functions/{query_lambda_arn}/invocations"
        )
        try:
            apigw.get_integration(
                restApiId=api_id, resourceId=resource_id, httpMethod="OPTIONS"
            )
        except ClientError:
            apigw.put_integration(
                restApiId=api_id,
                resourceId=resource_id,
                httpMethod="OPTIONS",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=lambda_uri,
            )

    # 获取根资源 id
    root_id = next(
        r["id"]
        for r in apigw.get_resources(restApiId=api_id)["items"]
        if r["path"] == "/"
    )

    # /instances
    instances_id = _get_or_create_resource(root_id, "instances")
    _put_lambda_method(instances_id, "GET")
    _put_options_method(instances_id)

    # /contacts
    contacts_id = _get_or_create_resource(root_id, "contacts")
    _put_lambda_method(contacts_id, "GET")
    _put_options_method(contacts_id)

    # /contacts/{contactId}
    contact_item_id = _get_or_create_resource(contacts_id, "{contactId}")

    # /contacts/{contactId}/events
    events_id = _get_or_create_resource(contact_item_id, "events")
    _put_lambda_method(events_id, "GET")
    _put_options_method(events_id)

    # /contacts/{contactId}/describe
    describe_id = _get_or_create_resource(contact_item_id, "describe")
    _put_lambda_method(describe_id, "GET")
    _put_options_method(describe_id)

    # /translate  (POST, 无鉴权 — Bedrock 权限由 Lambda 执行角色控制)
    translate_id = _get_or_create_resource(root_id, "translate")
    try:
        apigw.get_method(restApiId=api_id, resourceId=translate_id, httpMethod="POST")
    except ClientError:
        apigw.put_method(
            restApiId=api_id,
            resourceId=translate_id,
            httpMethod="POST",
            authorizationType="NONE",
        )
    translate_uri = (
        f"arn:aws:apigateway:{region}:lambda:path/2015-03-31"
        f"/functions/{query_lambda_arn}/invocations"
    )
    try:
        apigw.get_integration(
            restApiId=api_id, resourceId=translate_id, httpMethod="POST"
        )
    except ClientError:
        apigw.put_integration(
            restApiId=api_id,
            resourceId=translate_id,
            httpMethod="POST",
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=translate_uri,
        )
    _put_options_method(translate_id)

    # 部署到 prod stage
    print("    部署 API Gateway 到 prod stage...")
    apigw.create_deployment(restApiId=api_id, stageName="prod")
    api_url = f"https://{api_id}.execute-api.{region}.amazonaws.com/prod"
    print(f"    API URL: {api_url}")

    # 4.5 创建登录用户(Cognito 会把一次性密码发送到邮箱)
    try:
        cog_idp.admin_get_user(UserPoolId=user_pool_id, Username=email)
        user_exists = True
    except ClientError:
        user_exists = False

    if user_exists:
        print(f"==> 用户已存在，跳过创建: {email}")
        print(
            "    如需重新发送一次性密码，可在登录页点击「忘记密码」，或删除该用户后重跑。"
        )
        # 补设 custom:instances（首次部署时用户已存在可能没有此属性）
        try:
            attrs = cog_idp.admin_get_user(UserPoolId=user_pool_id, Username=email).get(
                "UserAttributes", []
            )
            has_instances = any(a["Name"] == "custom:instances" for a in attrs)
            if not has_instances:
                print("    补设 custom:instances = * ...")
                cog_idp.admin_update_user_attributes(
                    UserPoolId=user_pool_id,
                    Username=email,
                    UserAttributes=[{"Name": "custom:instances", "Value": "*"}],
                )
        except ClientError as e:
            print(f"    警告: 无法补设 custom:instances: {e}", file=sys.stderr)
    else:
        print(f"==> 创建登录用户并发送一次性密码到: {email}")
        cog_idp.admin_create_user(
            UserPoolId=user_pool_id,
            Username=email,
            UserAttributes=[
                {"Name": "email", "Value": email},
                {"Name": "email_verified", "Value": "true"},
                {"Name": "custom:instances", "Value": "*"},
            ],
            DesiredDeliveryMediums=["EMAIL"],
        )

    # -----------------------------------------------------------------------
    # 5. 发布 Web 站点(登录门禁版) -> Web 桶
    # -----------------------------------------------------------------------
    if bucket_exists(s3, web_bucket):
        print(f"==> Web 桶已存在，复用: {web_bucket}")
    else:
        create_bucket(s3, web_bucket, region)

    print(f"==> 发布站点到 s3://{web_bucket}/ ...")
    js_ct = "application/javascript; charset=utf-8"

    s3_upload_file(s3, web_bucket, "app.js", os.path.join(WEB_DIR, "app.js"), js_ct)
    s3_upload_file(s3, web_bucket, "i18n.js", os.path.join(WEB_DIR, "i18n.js"), js_ct)
    s3_upload_file(
        s3, web_bucket, "site-config.js", os.path.join(WEB_DIR, "site-config.js"), js_ct
    )
    s3_upload_file(
        s3, web_bucket, "auth.js", os.path.join(WEB_CF_DIR, "auth.js"), js_ct
    )

    connect_enrich = os.path.join(WEB_DIR, "connect-enrich.js")
    if os.path.isfile(connect_enrich):
        s3_upload_file(s3, web_bucket, "connect-enrich.js", connect_enrich, js_ct)
    else:
        s3_put(
            s3,
            web_bucket,
            "connect-enrich.js",
            "window.__CONNECT_CONTACT_ENRICH__ = {};\n",
            js_ct,
        )

    # 生成运行时配置 aws-config.js 并直接上传(不落本地文件)
    # 多实例场景: connectInstanceName 留空，用户在登录页手动输入实例名
    # connectInstanceId/connectInstanceArn/connectRegion 取第一个实例的值
    first_inst_id = first_inst_arn.split("/")[-1]
    first_inst_region = arn_region(first_inst_arn)
    aws_config_js = (
        "/* 由 setup_cloudfront.py 自动生成，请勿手工编辑 */\n"
        "window.__AWS_CONFIG__ = {\n"
        f'  region: "{region}",\n'
        f'  userPoolId: "{user_pool_id}",\n'
        f'  clientId: "{client_id}",\n'
        f'  connectInstanceId: "{first_inst_id}",\n'
        f'  connectInstanceArn: "{first_inst_arn}",\n'
        f'  connectInstanceName: "",\n'
        f'  connectRegion: "{first_inst_region}",\n'
        f'  recordingsBucket: "{all_recordings_buckets[0] if all_recordings_buckets else ""}",\n'
        f'  csatAttribute: "{csat_attribute_key}",\n'
        f'  apiUrl: "{api_url}",\n'
        f'  translateLang: "中文"\n'
        "};\n"
    )
    s3_put(s3, web_bucket, "aws-config.js", aws_config_js, js_ct)

    # 由 lib/web/index.html 生成登录门禁版 index.html 并直接上传
    with open(os.path.join(WEB_DIR, "index.html"), encoding="utf-8") as f:
        html = f.read()
    # 去掉静态 data.js
    html = re.sub(r'[ \t]*<script src="\./data\.js"></script>\s*\n', "", html)
    # 用 SDK + aws-config.js + auth.js 取代 app.js
    deploy_ts = str(int(time.time()))
    replacement = (
        f'<script src="{AWS_SDK_URL}"></script>\n'
        f'<script src="./aws-config.js?v={deploy_ts}"></script>\n'
        f'<script src="./auth.js?v={deploy_ts}"></script>\n'
    )
    html, n = re.subn(
        r'[ \t]*<script src="\./app\.js"></script>\s*\n', replacement, html
    )
    if n == 0:
        print(
            "警告: 在 index.html 模板中未找到 app.js 脚本标签，请检查模板。",
            file=sys.stderr,
        )
    # 给其余 JS 文件也加版本号，强制浏览器拉新
    html = re.sub(
        r'<script src="\./site-config\.js"></script>',
        f'<script src="./site-config.js?v={deploy_ts}"></script>',
        html,
    )
    html = re.sub(
        r'<script src="\./connect-enrich\.js"></script>',
        f'<script src="./connect-enrich.js?v={deploy_ts}"></script>',
        html,
    )
    html = re.sub(
        r'<script src="\./i18n\.js"></script>',
        f'<script src="./i18n.js?v={deploy_ts}"></script>',
        html,
    )
    s3_put(s3, web_bucket, "index.html", html, "text/html; charset=utf-8")

    # -----------------------------------------------------------------------
    # 6. CloudFront: 源访问控制(OAC) + 分配 + 回源桶策略
    # -----------------------------------------------------------------------
    # 6.1 OAC(按名称复用)
    oac_name = f"connect-ai-logs-{acct}-oac"
    existing_oacs = (
        cf.list_origin_access_controls()
        .get("OriginAccessControlList", {})
        .get("Items", [])
    )
    existing_oac = next((o for o in existing_oacs if o["Name"] == oac_name), None)
    if existing_oac:
        print(f"==> 复用已存在的 OAC: {existing_oac['Id']}")
        oac_id = existing_oac["Id"]
    else:
        print(f"==> 创建 CloudFront OAC: {oac_name}")
        oac_id = cf.create_origin_access_control(
            OriginAccessControlConfig={
                "Name": oac_name,
                "Description": f"OAC for {web_bucket}",
                "SigningProtocol": "sigv4",
                "SigningBehavior": "always",
                "OriginAccessControlOriginType": "s3",
            }
        )["OriginAccessControl"]["Id"]

    # 6.2 分配(按 Comment 复用；不存在则创建)
    dist_comment = f"connect-ai-agent-logs-{acct}"
    web_origin_domain = f"{web_bucket}.s3.{region}.amazonaws.com"
    existing_dists = (
        cf.list_distributions().get("DistributionList", {}).get("Items", []) or []
    )
    existing_dist = next(
        (d for d in existing_dists if d.get("Comment") == dist_comment), None
    )

    if existing_dist:
        print(f"==> 复用已存在的分配: {existing_dist['Id']}")
        dist_id = existing_dist["Id"]
        # 检查 Origin 是否指向正确的桶，不一致则更新
        dist_config_resp = cf.get_distribution_config(Id=dist_id)
        dist_etag = dist_config_resp["ETag"]
        dist_config = dist_config_resp["DistributionConfig"]
        current_origin_domain = dist_config["Origins"]["Items"][0]["DomainName"]
        if current_origin_domain != web_origin_domain:
            print(
                f"==> Origin 域名不一致({current_origin_domain} -> {web_origin_domain})，自动更新 ..."
            )
            dist_config["Origins"]["Items"][0]["DomainName"] = web_origin_domain
            cf.update_distribution(
                Id=dist_id, IfMatch=dist_etag, DistributionConfig=dist_config
            )
            print("==> CloudFront Origin 已更新，等待部署 ...")
    else:
        print("==> 创建 CloudFront 分配 ...")
        dist_id = cf.create_distribution(
            DistributionConfig={
                "CallerReference": f"{dist_comment}-{int(time.time())}",
                "Comment": dist_comment,
                "Enabled": True,
                "DefaultRootObject": "index.html",
                "Origins": {
                    "Quantity": 1,
                    "Items": [
                        {
                            "Id": "s3-web",
                            "DomainName": web_origin_domain,
                            "OriginAccessControlId": oac_id,
                            "S3OriginConfig": {"OriginAccessIdentity": ""},
                        }
                    ],
                },
                "DefaultCacheBehavior": {
                    "TargetOriginId": "s3-web",
                    "ViewerProtocolPolicy": "redirect-to-https",
                    "CachePolicyId": CF_CACHE_POLICY_ID,
                    "Compress": True,
                    "AllowedMethods": {
                        "Quantity": 2,
                        "Items": ["GET", "HEAD"],
                        "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]},
                    },
                },
                "CustomErrorResponses": {
                    "Quantity": 2,
                    "Items": [
                        {
                            "ErrorCode": 403,
                            "ResponseCode": "200",
                            "ResponsePagePath": "/index.html",
                            "ErrorCachingMinTTL": 10,
                        },
                        {
                            "ErrorCode": 404,
                            "ResponseCode": "200",
                            "ResponsePagePath": "/index.html",
                            "ErrorCachingMinTTL": 10,
                        },
                    ],
                },
            }
        )["Distribution"]["Id"]

    dist_domain = cf.get_distribution(Id=dist_id)["Distribution"]["DomainName"]
    dist_arn = f"arn:aws:cloudfront::{account_id}:distribution/{dist_id}"

    # 6.3 Web 桶策略: 仅允许该 CloudFront 分配经 OAC 读取
    print("==> 配置 Web 桶策略(仅允许 CloudFront OAC 访问)")
    web_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCloudFrontOAC",
                "Effect": "Allow",
                "Principal": {"Service": "cloudfront.amazonaws.com"},
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{web_bucket}/*",
                "Condition": {"StringEquals": {"AWS:SourceArn": dist_arn}},
            }
        ],
    }
    s3.put_bucket_policy(Bucket=web_bucket, Policy=json.dumps(web_policy))

    # 若为已存在分配，发起一次失效以刷新站点资源
    try:
        cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/*"]},
                "CallerReference": str(int(time.time())),
            },
        )
    except ClientError:
        pass

    # -----------------------------------------------------------------------
    # 6.4 等待 CloudFront 部署完成(InProgress -> Deployed)
    # -----------------------------------------------------------------------
    print("==> 等待 CloudFront 分配部署完成(通常 3~10 分钟)...")
    cf_status = "Unknown"
    waited = 0
    max_wait = 1200
    while waited < max_wait:
        try:
            cf_status = cf.get_distribution(Id=dist_id)["Distribution"]["Status"]
        except Exception:
            cf_status = "Unknown"
        if cf_status == "Deployed":
            print(
                f"\r    CloudFront 状态: Deployed (耗时 {waited}s)                    "
            )
            break
        print(
            f"\r    CloudFront 状态: {cf_status} … 已等待 {waited}s", end="", flush=True
        )
        time.sleep(15)
        waited += 15
    if cf_status != "Deployed":
        print(f"\n    CloudFront 仍在后台部署(状态: {cf_status})。可稍后查看:")
        print(
            f"      aws cloudfront get-distribution --id {dist_id} "
            f"--query 'Distribution.Status' --output text"
        )

    # -----------------------------------------------------------------------
    # 7. 汇总
    # -----------------------------------------------------------------------
    deployed_label = (
        "已完成 (Deployed)"
        if cf_status == "Deployed"
        else f"{cf_status}(后台继续部署中)"
    )

    print()
    print("===================================================================")
    print(" 部署完成")
    print("-------------------------------------------------------------------")
    print(f" 访问地址(CloudFront):  https://{dist_domain}")
    print(f" CloudFront 部署状态:   {deployed_label}")
    print(f" 登录邮箱:              {email}")
    print("   · 首次登录: 使用邮件里收到的一次性密码，登录后按提示设置新密码。")
    print("   · 忘记密码: 登录页点击「忘记密码」，向该邮箱发送新的验证码后重置。")
    print()
    print(" 实例处理结果:")
    for inst_key, res in instance_results.items():
        alias = instance_names.get(inst_key, "")
        cc = res.get("contact_count", "?")
        err = res.get("error", "")
        if err and err != "no ai_log_arn":
            print(f"   · {inst_key} ({alias}): 解析报错 — {err}")
        elif cc == "?":
            print(f"   · {inst_key} ({alias}): Lambda 处理中")
        else:
            print(f"   · {inst_key} ({alias}): {cc} 个 Contact")
    print()
    print(" 资源清单:")
    print(f"   CloudFront 访问地址: https://{dist_domain}")
    print(f"   CloudFront 分配 ID:  {dist_id}")
    print(f"   CloudFront 部署状态: {cf_status}")
    print(f"   日志存储桶:          s3://{logs_bucket}")
    print(f"   解析 Lambda:         {lambda_name}")
    print(f"   Web 存储桶:          s3://{web_bucket}")
    print(f"   Cognito 用户池:      {user_pool_id}")
    print(f"   Cognito 应用客户端:  {client_id}")
    print(f"   API Gateway URL:     {api_url}")
    print(f"   Query Lambda:        {query_lambda_name}")
    print("-------------------------------------------------------------------")
    if cf_status == "Deployed":
        print(" 提示: CloudFront 已部署完成，可直接访问上面的地址。")
    else:
        print(" 提示: CloudFront 仍在后台部署，状态变为 Deployed 后即可访问。")
    print(
        "       原始日志与拆分均在云端完成(S3 事件触发 Lambda)，本地不处理任何日志文件。"
    )
    print(
        "       重跑本脚本会重新拉取归档并触发 Lambda(已存在的 Contact 日志会被覆盖写入)。"
    )
    print("===================================================================")

    if args.keep:
        print(f" 临时产物目录已保留: {work}")


if __name__ == "__main__":
    main()
