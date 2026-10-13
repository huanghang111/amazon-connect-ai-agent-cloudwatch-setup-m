#!/usr/bin/env python3
"""将 MCP Server Agent 部署到 AgentCore Runtime（使用 CodeBuild 构建 Docker 镜像）。

所有部署逻辑均自包含在本文件中，无需依赖其他脚本。

步骤：
  1   ECR 镜像仓库
  2   将源码打包上传至 S3
  3   CodeBuild 项目 + IAM 角色
  4   执行 CodeBuild（构建并推送 Docker 镜像）
  5   Runtime 执行角色 + 权限策略
  6   AgentCore Runtime（创建或更新）
  7   Gateway（服务角色 + 创建/复用 + IAM 权限 + Target）

用法：
    python deploy.py
"""

import json          # 用于序列化/反序列化 JSON（IAM 策略文档等）
import os            # 环境变量读写、进程替换（os.execv）、chdir
import secrets       # 生成密码学安全的随机 token
import sys           # 退出进程、获取当前解释器路径
import tempfile      # 创建临时文件（用于打包 ZIP）
import time          # sleep 等待（IAM 传播、构建轮询）
import zipfile       # 打包源码为 ZIP 文件上传至 S3
from pathlib import Path          # 跨平台文件路径操作
from urllib.parse import quote    # 对 ARN 进行 URL 编码（用于 MCP endpoint 拼接）

import boto3                      # AWS SDK，全量替代 AWS CLI
import botocore.exceptions        # boto3 异常类型（ClientError、NoSuchEntityException 等）

HERE = Path(__file__).parent  # 当前目录（mcp_server/），用于构造其他文件的相对路径
MCP_SERVER_DIR = HERE  # mcp_server/ 目录路径（即本文件所在目录）

# ========================================
# 终端颜色辅助函数
# ========================================

def _green(s: str) -> str:
    """返回绿色 ANSI 着色字符串（用于成功提示）。"""
    return f"\033[0;32m{s}\033[0m"


def _red(s: str) -> str:
    """返回红色 ANSI 着色字符串（用于错误提示）。"""
    return f"\033[0;31m{s}\033[0m"


def _yellow(s: str) -> str:
    """返回黄色加粗 ANSI 着色字符串（用于步骤标题）。"""
    return f"\033[1;33m{s}\033[0m"


def ok(msg: str) -> None:
    """打印绿色「✓」成功消息。"""
    print(_green(f"✓ {msg}"))


def fail(msg: str) -> None:
    """打印红色「✗」错误消息并以 exit code 1 退出脚本。"""
    print(_red(f"✗ {msg}"))
    sys.exit(1)


def info(msg: str) -> None:
    """打印带两空格缩进的普通信息行。"""
    print(f"  {msg}")


def section(msg: str) -> None:
    """打印黄色步骤标题行。"""
    print(_yellow(msg))


def parse_instance_arn(arn: str) -> tuple[str, str, str]:
    """从 Amazon Connect 实例 ARN 中解析 region、account_id、instance_id。

    期望格式: arn:aws:connect:<region>:<account_id>:instance/<instance_id>

    返回 (region, account_id, instance_id)，格式非法时打印错误并退出。
    """
    import re
    pattern = r'^arn:aws:connect:([a-z0-9-]+):(\d{12}):instance/([a-f0-9-]+)$'
    m = re.match(pattern, arn)
    if not m:
        fail(
            f"Invalid CONNECT_INSTANCE_ARN: {arn}\n"
            "  Expected: arn:aws:connect:<region>:<account_id>:instance/<instance_id>"
        )
    return m.group(1), m.group(2), m.group(3)


# ========================================
# 步骤 1 – ECR 镜像仓库
# ========================================

def step1_ecr(ecr, ecr_repo_name: str, region: str) -> None:
    """步骤 1：确保 ECR 仓库存在，不存在则创建（并启用推送时自动扫描漏洞）。"""
    section(f"Step 1/7 ECR Repository: {ecr_repo_name}")
    try:
        ecr.describe_repositories(repositoryNames=[ecr_repo_name])  # 查询仓库是否存在
        ok("Already exists")
    except ecr.exceptions.RepositoryNotFoundException:
        ecr.create_repository(
            repositoryName=ecr_repo_name,
            imageScanningConfiguration={"scanOnPush": True},  # 推送镜像时自动扫描漏洞
        )
        ok("Created")
    print()


# ========================================
# 步骤 2 – 上传源码到 S3
# ========================================

def step2_upload_source(s3, s3_bucket: str, region: str, env_path: Path) -> None:
    """步骤 2：将 mcp_server/ 目录下所有文件打包为 ZIP 并上传至 S3。
    
    同时将对应的环境变量文件打包到 mcp_server/.env 中。

    S3 存储桶不存在时自动创建；临时 ZIP 文件在上传后立即删除。
    """
    section(f"Step 2/7 Upload source to S3: {s3_bucket}")

    # 检查存储桶是否存在，不存在则创建
    try:
        s3.head_bucket(Bucket=s3_bucket)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            if region == "us-east-1":
                s3.create_bucket(Bucket=s3_bucket)  # us-east-1 不能指定 LocationConstraint
            else:
                s3.create_bucket(
                    Bucket=s3_bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
        else:
            raise

    mcp_server_dir = MCP_SERVER_DIR  # mcp_server/ 目录路径
    if not mcp_server_dir.is_dir():
        fail(f"mcp_server directory not found: {mcp_server_dir}")
    
    # 检查环境变量文件是否存在
    if not env_path.exists():
        fail(f"Environment file not found: {env_path}")

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # 1. 打包 mcp_server/ 目录下所有文件
            for fpath in mcp_server_dir.rglob("*"):     # 递归遍历 mcp_server/ 下所有文件（含子目录）
                if fpath.is_file() and "__pycache__" not in fpath.parts and fpath.name != "deploy.py":
                    arcname = fpath.relative_to(mcp_server_dir)  # 保持相对目录结构
                    zf.write(fpath, arcname)
                    info(f"  + {arcname}")
            
            # 2. 将环境变量文件打包为 .env (放在根目录)
            zf.write(env_path, ".env")
            info(f"  + .env (from {env_path.name})")
            
        s3.upload_file(tmp_path, s3_bucket, "source.zip")  # 上传 ZIP 至 S3
    finally:
        os.unlink(tmp_path)  # 无论上传成功与否，始终删除临时 ZIP 文件

    ok(f"Source uploaded (with {env_path.name})")
    print()


# ========================================
# 步骤 3 – CodeBuild 项目
# ========================================

def step3_codebuild(
    iam,
    cb,
    codebuild_project_name: str,
    codebuild_role_name: str,
    codebuild_role_arn: str,
    s3_bucket: str,
    ecr_uri: str,
    account_id: str,
    region: str,
) -> None:
    """步骤 3：确保 CodeBuild IAM 角色和项目存在，不存在则分别创建。"""
    section(f"Step 3/7 CodeBuild Project: {codebuild_project_name}")

    # 检查 IAM 角色是否存在，不存在则创建
    try:
        iam.get_role(RoleName=codebuild_role_name)
    except iam.exceptions.NoSuchEntityException:
        # 信任策略：允许 CodeBuild 服务扮演此角色
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "codebuild.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }],
        })
        iam.create_role(
            RoleName=codebuild_role_name,
            AssumeRolePolicyDocument=trust,
        )

        # 内联权限策略：ECR 推拉镜像 + S3 读取源码 + CloudWatch Logs 写日志
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "ecr:GetAuthorizationToken",
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                        "ecr:PutImage",
                        "ecr:InitiateLayerUpload",
                        "ecr:UploadLayerPart",
                        "ecr:CompleteLayerUpload",
                    ],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": "*",
                },
            ],
        })
        iam.put_role_policy(
            RoleName=codebuild_role_name,
            PolicyName="codebuild-policy",
            PolicyDocument=policy,
        )
        info("IAM role created, waiting 10s...")
        time.sleep(10)  # 等待 IAM 权限全球传播

    # 检查 CodeBuild 项目是否存在，不存在则创建
    resp = cb.batch_get_projects(names=[codebuild_project_name])
    if resp.get("projects"):
        ok("Project exists")
    else:
        cb.create_project(
            name=codebuild_project_name,
            source={"type": "S3", "location": f"{s3_bucket}/source.zip"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={
                "type": "ARM_CONTAINER",
                "image": "aws/codebuild/amazonlinux2-aarch64-standard:3.0",
                "computeType": "BUILD_GENERAL1_SMALL",
                "privilegedMode": True,  # docker build 需要特权模式
                "environmentVariables": [
                    {"name": "ACCOUNT_ID",         "value": account_id,  "type": "PLAINTEXT"},
                    {"name": "ECR_REPO_URI",        "value": ecr_uri,     "type": "PLAINTEXT"},
                    {"name": "AWS_DEFAULT_REGION",  "value": region,      "type": "PLAINTEXT"},
                ],
            },
            serviceRole=codebuild_role_arn,
        )
        ok("Project created")
    print()


# ========================================
# 步骤 4 – 执行 CodeBuild 构建
# ========================================

def step4_codebuild_run(
    cb,
    codebuild_project_name: str,
    s3_bucket: str,
    region: str,
) -> None:
    """步骤 4：触发 CodeBuild 构建并轮询等待完成（最长 10 分钟，每 10 秒查询一次）。"""
    section("Step 4/7: Build Docker image via CodeBuild")

    # 启动构建，覆盖源码位置确保使用最新上传的 source.zip
    resp = cb.start_build(
        projectName=codebuild_project_name,
        sourceLocationOverride=f"{s3_bucket}/source.zip",
    )
    build_id = resp["build"]["id"]
    info(f"Build ID: {build_id}")
    info("Waiting for build...")

    # 轮询构建状态：最多 60 次 × 10 秒 = 10 分钟
    for i in range(1, 61):
        status = cb.batch_get_builds(ids=[build_id])["builds"][0]["buildStatus"]
        if status == "SUCCEEDED":
            ok("Build succeeded")
            break
        elif status in ("FAILED", "FAULT", "STOPPED"):
            fail(f"Build failed: {status}")
        else:
            info(f"Status: {status} ({i}/60)")
            time.sleep(10)
    else:
        fail("Build did not complete within 10 minutes")
    print()


# ========================================
# 步骤 5 – Runtime 执行角色
# ========================================

def step5_execution_role(iam, role_name: str) -> None:
    """步骤 5：确保 AgentCore Runtime 执行角色存在并附加所有必需权限策略。"""
    section("Step 5/7: Runtime Execution Role")

    try:
        iam.get_role(RoleName=role_name)
        ok("Already exists")
    except iam.exceptions.NoSuchEntityException:
        # 信任策略：允许 bedrock-agentcore.amazonaws.com 扮演此角色
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }],
        })
        iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)

        # 附加 AWS 托管策略：AgentCore 完整权限
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/BedrockAgentCoreFullAccess",
        )

        # 内联策略：ECR 拉取镜像 + CloudWatch Logs 写日志
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="ecr-and-logs",
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ecr:GetDownloadUrlForLayer",
                            "ecr:BatchGetImage",
                            "ecr:GetAuthorizationToken",
                        ],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                        "Resource": "*",
                    },
                ],
            }),
        )
        info("Waiting 10s for IAM propagation...")
        time.sleep(10)
        ok("Created")

    # 幂等：确保 bedrock:InvokeModel 权限存在
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="bedrock-invoke-model",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                "Resource": [
                    "arn:aws:bedrock:*::foundation-model/*",
                    "arn:aws:bedrock:*:*:inference-profile/*",
                    "arn:aws:bedrock:*:*:application-inference-profile/*",
                ],
            }],
        }),
    )
    ok("bedrock:InvokeModel granted")

    # 幂等：OTEL / X-Ray 可观测性权限
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="otel-observability",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "xray:PutTraceSegments",
                    "xray:PutSpans",
                    "xray:PutSpansForIndexing",
                    "xray:GetSamplingRules",
                    "xray:GetSamplingTargets",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogStreams",
                    "logs:DescribeLogGroups",
                    "cloudwatch:PutMetricData",
                ],
                "Resource": "*",
            }],
        }),
    )
    ok("OTEL/X-Ray permissions granted")

    # 幂等：DynamoDB 访问权限（用于会话存储）
    account_id = iam.get_role(RoleName=role_name)["Role"]["Arn"].split(":")[4]
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="dynamodb-access",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                ],
                "Resource": f"arn:aws:dynamodb:*:{account_id}:table/connect-ai-agent-store",
            }],
        }),
    )
    ok("DynamoDB permissions granted")
    print()


# ========================================
# 步骤 6 – AgentCore Runtime
# ========================================

def step6_runtime(
    control,              # boto3 bedrock-agentcore-control 客户端
    agent_name: str,      # Runtime 名称（下划线格式，如 connect_repair_mcp_server）
    ecr_uri: str,         # ECR 仓库 URI（不含 tag）
    role_arn: str,        # Runtime 执行角色 ARN
    region: str,          # 部署区域
    env_vars: dict,       # 注入 Runtime 容器的环境变量字典
) -> tuple[str, str]:
    """步骤 6：创建或更新 AgentCore Runtime，等待状态变为 READY。

    返回 (runtime_id, runtime_arn) 二元组。
    """
    section("Step 6/7: AgentCore Runtime (MCP)")

    # 遍历所有 Runtime，查找同名 Runtime（用于判断创建还是更新）
    runtime_id = __checkRuntimeId(control, agent_name)

    # 构建 Runtime 参数（创建和更新共用同一份配置，更新时排除 agentRuntimeName）
    runtime_params = dict(
        agentRuntimeName=agent_name,                  # Runtime 名称（仅创建时使用）
        agentRuntimeArtifact={
            "containerConfiguration": {"containerUri": f"{ecr_uri}:latest"}  # 使用最新镜像 tag
        },
        roleArn=role_arn,                             # 执行角色
        networkConfiguration={"networkMode": "PUBLIC"},   # 公网访问模式（供 Gateway 调用）
        protocolConfiguration={"serverProtocol": "MCP"},  # 使用 MCP 协议
        lifecycleConfiguration={
            "idleRuntimeSessionTimeout": 3600,        # 空闲会话超时 1 小时（秒）
            "maxLifetime": 28800,                     # 最大会话生命周期 8 小时（秒）
        },
        environmentVariables=env_vars,                # 注入容器的环境变量
    )

    if runtime_id:                                    # Runtime 已存在，执行更新
        info(f"Runtime exists: {runtime_id}")
        update_params = {k: v for k, v in runtime_params.items() if k != "agentRuntimeName"}  # 更新不传名称
        control.update_agent_runtime(agentRuntimeId=runtime_id, **update_params)
        ok("Updated")
    else:                                             # Runtime 不存在，执行创建
        resp = control.create_agent_runtime(**runtime_params)
        runtime_id = resp["agentRuntimeId"]           # 获取新创建的 Runtime ID
        ok(f"Created: {runtime_id}")

    # 轮询等待 Runtime 进入 READY 状态（最长 10 分钟）
    info("Waiting for READY...")
    for i in range(60):
        resp = control.get_agent_runtime(agentRuntimeId=runtime_id)
        status = resp["status"]
        if status == "READY":                         # 已就绪，退出轮询
            ok("Runtime READY")
            break
        elif "FAILED" in status:                      # 创建/更新失败，终止部署
            fail(f"Runtime failed: {status} — {resp.get('failureReason', 'unknown')}")
        info(f"Status: {status} ({i + 1}/60)")
        time.sleep(10)                                # 每 10 秒查询一次
    else:
        fail("Timeout waiting for Runtime READY")     # for 循环正常结束表示超时

    print()
    return runtime_id  # 返回 ID 供后续步骤使用


def __checkRuntimeId(control, agent_name):
    paginator = control.get_paginator("list_agent_runtimes")  # 分页列举所有 Runtime
    for page in paginator.paginate():
        for rt in page.get("agentRuntimes", []):
            if rt["agentRuntimeName"] == agent_name:  # 找到同名 Runtime
                return rt["agentRuntimeId"]
    return None

# ========================================
# 步骤 7 – Gateway（服务角色 + 创建 + IAM 权限 + Target）
# ========================================

def step7_gateway(
    control,                    # boto3 bedrock-agentcore-control 客户端
    iam,                        # boto3 IAM 客户端
    gateway_name: str,          # Gateway / Target 名称
    gateway_role_name: str,     # 自动创建时使用的 Gateway 角色名
    agent_name: str,            # Agent 名称，用于命名和描述
    runtime_arn: str,           # Runtime ARN（用于 IAM 权限和 MCP endpoint）
    region: str,                # 部署区域
    jwt_discovery_url: str,     # JWT 授权器的 OIDC discovery URL
) -> tuple[str, str, str]:
    """步骤 7：配置 AgentCore Gateway。

    幂等逻辑：
      - Gateway + Target 均已存在 → 直接跳过，无任何写操作
      - Gateway 已存在但 Target 不存在 → 仅创建 Target
      - Gateway 不存在 → 创建服务角色 + Gateway + IAM 授权 + Target

    返回 (gateway_id, mcp_endpoint) 三元组。
    """
    section("Step 7/7: AgentCore Gateway")

    # 预先计算 mcp_endpoint（无论是否跳过都需要返回）
    encoded_arn = quote(runtime_arn, safe="")
    mcp_endpoint = (
        f"https://bedrock-agentcore.{region}.amazonaws.com"
        f"/runtimes/{encoded_arn}/invocations?qualifier=DEFAULT"
    )

    # ── 查找已存在的 Gateway ──────────────────────────────────────────────────
    gateway_id = None
    # 按名称翻页查找（前次部署可能已自动创建）
    paginator = control.get_paginator("list_gateways")
    for page in paginator.paginate():
        for gw in page.get("items", []):
            if gw.get("name") == gateway_name:
                gateway_id = gw["gatewayId"]
                info(f"Found existing Gateway: {gateway_id}")
                break
        if gateway_id:
            break

    # ── 若 Gateway 已存在，检查 Target 是否也存在 ────────────────────────────
    if gateway_id:
        targets_resp = control.list_gateway_targets(gatewayIdentifier=gateway_id)
        for t in targets_resp.get("items", []):
            if t["name"] == gateway_name:
                # Gateway + Target 均已存在
                existing_target_id = t["targetId"]
                info(f"Gateway and Target already exist. Synchronizing tools...") # 同步最新工具列表到 Gateway         
                try:
                    control.synchronize_gateway_targets(
                        gatewayIdentifier=gateway_id,
                        targetIdList=[existing_target_id],  # 只同步当前 Target
                    )
                    ok("Sync triggered")
                except Exception as e:
                    fail(f"Sync error: {e}")
                print()
                return gateway_id, mcp_endpoint
        # Gateway 存在但 Target 不存在，跳到 Target 创建
        info(f"Gateway exists ({gateway_id}), Target missing — creating Target only")
        _create_gateway_target(control, gateway_id, gateway_name, mcp_endpoint, region)
        print()
        return gateway_id, mcp_endpoint

    # ── Gateway 不存在，执行完整创建流程 ─────────────────────────────────────
    if not jwt_discovery_url:
        fail(
            "GATEWAY_ID is empty and GATEWAY_JWT_DISCOVERY_URL is not set.\n"
            "  Either set GATEWAY_ID to use an existing Gateway, or provide JWT\n"
            "  authorizer config (GATEWAY_JWT_DISCOVERY_URL plus AUDIENCE/CLIENTS)\n"
            "  so the script can create one."
        )

    # 7a：确保服务角色存在
    try:
        role_arn = iam.get_role(RoleName=gateway_role_name)["Role"]["Arn"]
        info(f"Gateway service role exists: {gateway_role_name}")
    except iam.exceptions.NoSuchEntityException:
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }],
        })
        role_arn = iam.create_role(
            RoleName=gateway_role_name,
            AssumeRolePolicyDocument=trust,
            Description=f"Service role for {agent_name} AgentCore Gateway",
        )["Role"]["Arn"]
        iam.put_role_policy(
            RoleName=gateway_role_name,
            PolicyName="gateway-default",
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": [
                        "bedrock-agentcore:InvokeAgentRuntime",
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    "Resource": "*",
                }],
            }),
        )
        ok(f"Gateway service role created: {gateway_role_name}")
        info("Waiting 10s for IAM propagation...")
        time.sleep(10)

    # 7b：创建 Gateway（先用占位符 audience，因为 aud = Gateway ID，创建前未知）
    jwt_cfg: dict = {"discoveryUrl": jwt_discovery_url, "allowedAudience": ["__placeholder__"]}

    info(f"Creating Gateway: {gateway_name}")
    resp = control.create_gateway(
        name=gateway_name,
        description=f"Auto-created Gateway for {agent_name}",
        roleArn=role_arn,
        protocolType="MCP",
        authorizerType="CUSTOM_JWT",
        authorizerConfiguration={"customJWTAuthorizer": jwt_cfg},
    )
    gateway_id = resp["gatewayId"]
    ok(f"Gateway created: {gateway_id}")

    # 等待 Gateway 离开 CREATING 状态后再执行 update
    info("Waiting for Gateway to become stable...")
    for i in range(60):
        gw_status = control.get_gateway(gatewayIdentifier=gateway_id).get("status", "")
        if gw_status not in ("CREATING", "UPDATING"):
            ok(f"Gateway status: {gw_status}")
            break
        info(f"Gateway status: {gw_status} ({i + 1}/60)")
        time.sleep(10)
    else:
        fail("Timeout waiting for Gateway to become stable before update")

    # 将 allowedAudience 更新为真实 Gateway ID（Connect JWT 的 aud 字段等于 Gateway ID）
    final_aud = [gateway_id]
    final_jwt: dict = {"discoveryUrl": jwt_discovery_url, "allowedAudience": final_aud}
    control.update_gateway(
        gatewayIdentifier=gateway_id,
        name=gateway_name,
        description=f"Auto-created Gateway for {agent_name}",
        roleArn=role_arn,
        protocolType="MCP",
        authorizerType="CUSTOM_JWT",
        authorizerConfiguration={"customJWTAuthorizer": final_jwt},
    )
    ok(f"allowedAudience set to {final_aud}")

    # 7c：为服务角色附加调用 Runtime 的 IAM 权限
    iam.put_role_policy(
        RoleName=gateway_role_name,
        PolicyName="InvokeAgentRuntimePolicy",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "AllowInvokeAgentRuntime",
                "Effect": "Allow",
                "Action": "bedrock-agentcore:InvokeAgentRuntime",
                "Resource": [runtime_arn, f"{runtime_arn}/*"],
            }],
        }),
    )
    ok("InvokeAgentRuntime permission added")
    time.sleep(10)  # 等待 IAM 权限传播

    # 7d：创建 Gateway Target
    _create_gateway_target(control, gateway_id, gateway_name, mcp_endpoint, region)

    print()
    return gateway_id, mcp_endpoint


def _create_gateway_target(
    control,           # boto3 bedrock-agentcore-control 客户端
    gateway_id: str,   # Gateway ID
    gateway_name: str, # Target 名称
    mcp_endpoint: str, # MCP Server 调用端点 URL
    region: str,       # 部署区域
) -> None:
    """创建 Gateway Target 并轮询等待 READY（最长 5 分钟）。"""
    resp = control.create_gateway_target(
        gatewayIdentifier=gateway_id,
        name=gateway_name,
        description="Connect Repair MCP Server on AgentCore Runtime",
        targetConfiguration={"mcp": {"mcpServer": {"endpoint": mcp_endpoint}}},
        credentialProviderConfigurations=[
            {
                "credentialProviderType": "GATEWAY_IAM_ROLE",
                "credentialProvider": {
                    "iamCredentialProvider": {
                        "service": "bedrock-agentcore",
                        "region": region,
                    }
                },
            }
        ],
    )
    target_id = resp["targetId"]
    ok(f"Target created: {target_id}")

    info("Waiting for Target READY...")
    for i in range(30):
        t_resp = control.get_gateway_target(gatewayIdentifier=gateway_id, targetId=target_id)
        t_status = t_resp["status"]
        if t_status == "READY":
            ok("Target READY")
            return
        elif t_status == "FAILED":
            fail(f"Target failed: {t_resp.get('statusReasons', [])}")
        info(f"Status: {t_status} ({i + 1}/30)")
        time.sleep(10)
    fail("Timeout waiting for target READY")


# ========================================
# 主函数
# ========================================

def main(instance_arn: str, env_path: Path, encrypt_key: str) -> None:
    region, account_id, instance_id = parse_instance_arn(instance_arn)

    # 使用 account_id 作为 AWS profile 名称，切换到对应账号凭证
    session = boto3.Session(profile_name=account_id, region_name=region)
    info(f"Using AWS profile: {account_id}")

    try:
        _connect = session.client("connect", region_name=region)
        instance_name = _connect.describe_instance(InstanceId=instance_id)["Instance"]["InstanceAlias"]
        jwt_discovery_url = f"https://{instance_name}.my.connect.aws/.well-known/openid-configuration"
        info(f"Resolved JWT discovery URL: {jwt_discovery_url}")
    except Exception as e:
        fail(
            f"Failed to resolve GATEWAY_JWT_DISCOVERY_URL from Connect instance: {e}\n"
            "  Set GATEWAY_JWT_DISCOVERY_URL explicitly in .env to override."
        )

    info(f"Instance ARN  : {instance_arn}")
    info(f"Region        : {region}")
    info(f"Account ID    : {account_id}")
    info(f"Instance ID   : {instance_id}")
    info(f"Instance NAME   : {instance_name}")

    agent_name             = f"agentcore_mcp_{instance_name}".replace("-", "_")   # Agent 名称
    ecr_repo_name          = agent_name                                           # ECR 镜像仓库名
    codebuild_project_name = f"{agent_name}_build"                                # CodeBuild 项目名
    s3_bucket              = f"{agent_name}-{account_id}".replace("_", "-")  # S3 存储桶名（只允许小写字母、数字和连字符）
    ecr_uri                = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{ecr_repo_name}"  # ECR 完整 URI
    codebuild_role_name    = f"agentcore_mcp_codebuild_role"                       # CodeBuild IAM 角色名
    codebuild_role_arn     = f"arn:aws:iam::{account_id}:role/{codebuild_role_name}"   # CodeBuild IAM 角色 ARN
    execution_role_name    = f"agentcore_mcp_execution_role"                       # Runtime 执行角色名
    execution_role_arn     = f"arn:aws:iam::{account_id}:role/{execution_role_name}"   # Runtime 执行角色 ARN
    gateway_name           = f"{agent_name}_gateway".replace("_", "-")  # Gateway 名称（只允许小写字母、数字和连字符）
    gateway_role_name      = f"agentcore_mcp_gateway_role"                         # Gateway 服务角色名
    # env_path 由调用方传入，对应 env/<name>.env

    # 初始化 boto3 客户端（均使用同一 session，确保 profile 一致）
    control   = session.client("bedrock-agentcore-control", region_name=region)
    iam       = session.client("iam")
    ecr       = session.client("ecr", region_name=region)
    s3        = session.client("s3", region_name=region)
    codebuild = session.client("codebuild", region_name=region)

    print(_yellow("=== MCP Agent Deployment ===\n"))

    # ── 步骤 1-5：基于 boto3 的基础设施步骤 ────────────────────────────────
    runtime_id = None
    step1_ecr(ecr, ecr_repo_name, region)                 # 确保 ECR 仓库存在
    step2_upload_source(s3, s3_bucket, region, env_path)  # 打包上传源码和环境变量至 S3
    step3_codebuild(                                 # 确保 CodeBuild 项目及 IAM 角色存在
        iam,
        codebuild,
        codebuild_project_name,
        codebuild_role_name,
        codebuild_role_arn,
        s3_bucket,
        ecr_uri,
        account_id,
        region,
    )
    step4_codebuild_run(codebuild, codebuild_project_name, s3_bucket, region)  # 触发构建并等待完成
    step5_execution_role(iam, execution_role_name)        # 确保执行角色及权限策略存在

    # ── 步骤 6-7：基于 boto3 的 AgentCore 步骤 ───────────────────────────────

    # 构建注入 Runtime 容器的环境变量（可选项仅在非空时注入，避免覆盖容器内默认值）
    runtime_env: dict[str, str] = {
        "OTEL_SERVICE_NAME": agent_name,        # OTEL 服务名（用于 CloudWatch GenAI 观测 span 标签）
        "REGION": region,                       # 区域
        "OTEL_TRACES_EXPORTER": "none",         # 禁用 trace 导出（避免 OTLP 400 错误）
        "ENV_ENCRYPT_KEY": encrypt_key,         # .env 文件加密密钥
    }

    # 步骤 6：创建或更新 AgentCore Runtime
    runtime_id = step6_runtime(
        control, agent_name, ecr_uri,
        execution_role_arn, region, runtime_env,
    )

    # 获取 Runtime ARN（创建/更新后均需重新查询）
    runtime_arn = control.get_agent_runtime(agentRuntimeId=runtime_id)["agentRuntimeArn"]

    # 步骤 7：完整配置 Gateway（创建/复用 + IAM 权限 + Target）
    gateway_id, mcp_endpoint = step7_gateway(
        control, iam,
        gateway_name, gateway_role_name, agent_name,
        runtime_arn, region,
        jwt_discovery_url,
    )

    # ── 部署摘要（打印到终端并写入日志文件）─────────────────────────────────
    print("=" * 50)
    print("Deployment Complete!")
    print("=" * 50)
    print(f"Agent Runtime ID:  {runtime_id}")     # Runtime 资源 ID
    print(f"Agent Runtime ARN: {runtime_arn}")    # Runtime 资源 ARN
    print(f"MCP Endpoint:      {mcp_endpoint}")   # MCP 调用端点 URL
    print(f"Gateway ID:        {gateway_id}")     # Gateway 资源 ID
    print(f"Gateway Role:      {gateway_role_name}")  # Gateway 服务角色名
    print(f"Target Name:       {gateway_name}")    # Gateway Target 名称
    print()
    print(_green("=== Done ==="))  # 部署完成提示


if __name__ == "__main__":
    # 列举 env/ 目录下所有 .env 文件
    ENV_DIR = HERE.parent / "env"
    if not ENV_DIR.is_dir():
        print(f"✗ env directory not found: {ENV_DIR}", file=sys.stderr)
        sys.exit(1)

    env_files = sorted(p for p in ENV_DIR.iterdir() if p.suffix == ".env" and p.is_file())
    if not env_files:
        print(f"✗ No .env files found in: {ENV_DIR}", file=sys.stderr)
        sys.exit(1)

    # 显示可选名称（去掉 .env 后缀）
    names = [p.stem for p in env_files]
    print("Available instances:")
    for name in names:
        print(f"  {name}")

    bu_name = None
    while not bu_name:
        bu_name = input("Enter instance name: ").strip()
        if bu_name not in names:
            print(f"✗ '{bu_name}' not found. Available: {names}")
            bu_name = None

    # 读取对应 .env 文件，解析 INSTANCE_ARN
    env_path = ENV_DIR / f"{bu_name}.env"
    instance_arn = None
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("INSTANCE_ARN="):
                instance_arn = line.split("=", 1)[1].strip()
                break

    if not instance_arn:
        print(f"✗ INSTANCE_ARN not found in {env_path}", file=sys.stderr)
        sys.exit(1)

    # 读取 ENV_ENCRYPT_KEY（用于解密 .env 中的加密字段，并注入到 Runtime 环境变量）
    encrypt_key = input("Enter ENV_ENCRYPT_KEY: ").strip()
    if not encrypt_key:
        print("✗ ENV_ENCRYPT_KEY 不能为空", file=sys.stderr)
        sys.exit(1)

    print(f"  Loaded instance ARN: {instance_arn}")
    main(instance_arn, env_path, encrypt_key)
