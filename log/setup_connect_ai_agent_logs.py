#!/usr/bin/env python3
"""
setup_connect_ai_agent_logs.py

用法:
  python setup_connect_ai_agent_logs.py <amazon-connect-instance-arn>

示例:
  python setup_connect_ai_agent_logs.py \
    arn:aws:connect:us-west-2:991727053196:instance/abcd1234-5678-90ab-cdef-1234567890ab

功能:
  只需提供 Amazon Connect 实例 ARN，脚本会自动完成 Connect AI Agent
  的 CloudWatch Logs 日志投递配置:
    1. 解析实例 ARN，得到 region / account-id / instance-id
    2. 通过 Connect 集成关联找到底层的 Q in Connect (Wisdom) assistant ARN
    3. 创建/复用投递源 (PutDeliverySource, logType=EVENT_LOGS)
    4. 创建目标日志组
    5. 创建/更新投递目标 (PutDeliveryDestination, 输出 json 到 CloudWatch Logs)
    6. 创建投递关系 (CreateDelivery)，将源与目标关联

依赖: boto3 （已配置好 AWS 凭证）
"""

import re
import sys
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


# ----------------------------------------------------------------------------
# 命名约定（与文档一致）
# ----------------------------------------------------------------------------
ARN_PATTERN = re.compile(
    r"^arn:aws:connect:([a-z0-9-]+):(\d+):instance/([0-9a-fA-F-]+)$"
)


def parse_instance_arn(instance_arn: str) -> tuple[str, str, str]:
    """解析 Connect 实例 ARN，返回 (region, account_id, instance_id)。"""
    m = ARN_PATTERN.match(instance_arn)
    if not m:
        print("错误: 这不是合法的 Connect 实例 ARN:", file=sys.stderr)
        print(f"  {instance_arn}", file=sys.stderr)
        print(
            "期望形如: arn:aws:connect:us-west-2:111122223333:instance/<instance-id>",
            file=sys.stderr,
        )
        sys.exit(1)
    return m.group(1), m.group(2), m.group(3)


def get_wisdom_assistant_arn(connect_client, instance_id: str) -> str:
    """通过集成关联找到 Wisdom assistant ARN。"""
    print("==> 查询该实例关联的 Q in Connect (Wisdom) assistant ...")
    try:
        resp = connect_client.list_integration_associations(
            InstanceId=instance_id,
            IntegrationType="WISDOM_ASSISTANT",
        )
        associations = resp.get("IntegrationAssociationSummaryList", [])
        if not associations:
            raise ValueError("未找到关联")
        return associations[0]["IntegrationArn"]
    except (ClientError, ValueError, KeyError):
        print("错误: 在该 Connect 实例上未找到 WISDOM_ASSISTANT 集成关联。", file=sys.stderr)
        print("      请先在 Connect 控制台为该实例启用 AI agent / Q in Connect。", file=sys.stderr)
        sys.exit(1)


def ensure_delivery_source(logs_client, assistant_arn: str, source_name: str) -> str:
    """创建或复用指向 assistant_arn 的投递源，返回投递源名称。"""
    print("==> 检查是否已存在指向该 assistant 的投递源 ...")
    try:
        resp = logs_client.describe_delivery_sources()
        for source in resp.get("deliverySources", []):
            if assistant_arn in source.get("resourceArns", []):
                name = source["name"]
                print(f"    已存在投递源，直接复用: {name}")
                return name
    except ClientError:
        pass

    print(f"    未找到，创建投递源: {source_name}")
    logs_client.put_delivery_source(
        name=source_name,
        logType="EVENT_LOGS",
        resourceArn=assistant_arn,
    )
    print("    投递源已创建。")
    return source_name


def ensure_log_group(logs_client, log_group: str) -> None:
    """创建日志组，已存在则忽略。"""
    print(f"==> 确保日志组存在: {log_group}")
    try:
        logs_client.create_log_group(logGroupName=log_group)
        print("    日志组已创建。")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print("    日志组已存在，跳过创建。")
        else:
            raise


def ensure_delivery_destination(
    logs_client, dest_resource_arn: str, dest_name: str
) -> str:
    """创建或更新投递目标，返回目标 ARN。"""
    print(f"==> 创建/更新投递目标: {dest_name}")
    resp = logs_client.put_delivery_destination(
        name=dest_name,
        outputFormat="json",
        deliveryDestinationConfiguration={"destinationResourceArn": dest_resource_arn},
    )
    dest_arn = resp["deliveryDestination"]["arn"]
    print(f"    投递目标 ARN: {dest_arn}")
    return dest_arn


def ensure_delivery(logs_client, source_name: str, dest_arn: str) -> None:
    """创建投递关系，已存在则跳过。"""
    print("==> 检查投递关系是否已存在 ...")
    try:
        resp = logs_client.describe_deliveries()
        for delivery in resp.get("deliveries", []):
            if (
                delivery.get("deliverySourceName") == source_name
                and delivery.get("deliveryDestinationArn") == dest_arn
            ):
                delivery_id = delivery["id"]
                print(f"    投递关系已存在 (id={delivery_id})，跳过创建。")
                return
    except ClientError:
        pass

    print("    创建投递关系 ...")
    resp = logs_client.create_delivery(
        deliverySourceName=source_name,
        deliveryDestinationArn=dest_arn,
    )
    delivery_id = resp["delivery"]["id"]
    print(f"    投递关系已创建 (id={delivery_id})。")


def main(arn: str) -> None:
    # ----------------------------------------------------------------------------
    # 1. 解析实例 ARN
    # ----------------------------------------------------------------------------
    region, account_id, instance_id = parse_instance_arn(arn)

    # 使用 account_id 作为 AWS profile 名称，切换到对应账号凭证
    session = boto3.Session(profile_name=account_id, region_name=region)
    print(f"==> Using AWS profile: {account_id}")

    try:
        _connect = session.client("connect", region_name=region)
        instance_name = _connect.describe_instance(InstanceId=instance_id)["Instance"]["InstanceAlias"]
    except Exception as e:
        print('instance_name find error')
        return

    print("==> 解析实例 ARN")
    print(f"    Region      : {region}")
    print(f"    Account ID  : {account_id}")
    print(f"    Instance ID : {instance_id}")
    print(f"    Instance NAME : {instance_name}")

    source_name = f"connect-ai-agent-delivery-source-{instance_name}".replace("_", "-")
    dest_name = f"connect-ai-agent-delivery-destination-{instance_name}"
    log_group = f"/aws/connect/ai-agent-logs-{instance_name}"

    # ----------------------------------------------------------------------------
    # 初始化 boto3 客户端（均使用同一 session，确保 profile 一致）
    # ----------------------------------------------------------------------------
    connect_client = session.client("connect", region_name=region)
    logs_client = session.client("logs", region_name=region)

    # ----------------------------------------------------------------------------
    # 2. 找到 Wisdom assistant ARN
    # ----------------------------------------------------------------------------
    assistant_arn = get_wisdom_assistant_arn(connect_client, instance_id)
    print(f"    Assistant ARN: {assistant_arn}")

    # ----------------------------------------------------------------------------
    # 3. 创建/复用投递源
    # ----------------------------------------------------------------------------
    source_name = ensure_delivery_source(logs_client, assistant_arn, source_name)

    # ----------------------------------------------------------------------------
    # 4. 创建日志组
    # ----------------------------------------------------------------------------
    ensure_log_group(logs_client, log_group)

    # ----------------------------------------------------------------------------
    # 5. 创建/更新投递目标
    # ----------------------------------------------------------------------------
    dest_resource_arn = f"arn:aws:logs:{region}:{account_id}:log-group:{log_group}:*"
    dest_arn = ensure_delivery_destination(logs_client, dest_resource_arn, dest_name)

    # ----------------------------------------------------------------------------
    # 6. 创建投递关系
    # ----------------------------------------------------------------------------
    ensure_delivery(logs_client, source_name, dest_arn)

    # ----------------------------------------------------------------------------
    # 7. 完成提示
    # ----------------------------------------------------------------------------
    print()
    print("===================================================================")
    print("配置完成！")
    print(f"  投递源     : {source_name}")
    print(f"  投递目标   : {dest_name}")
    print(f"  日志组     : {log_group} (region: {region})")
    print()
    print("产生真实会话事件后，可这样查看日志:")
    print(f'  aws logs tail "{log_group}" --region {region} --follow')
    print("===================================================================")


if __name__ == "__main__":
    # 从 cloudfront/ 目录扫描所有 config-*.json，合并 connect 字典
    CLOUDFRONT_DIR = Path(__file__).parent / "cloudfront"
    config_files = sorted(CLOUDFRONT_DIR.glob("config-*.json"))
    if not config_files:
        print(f"✗ No config-*.json found in {CLOUDFRONT_DIR}", file=sys.stderr)
        sys.exit(1)

    config: dict = {}
    for cf in config_files:
        with open(cf, encoding="utf-8") as f:
            data = json.load(f)
        for name, entry in data.get("connect", {}).items():
            config[name] = entry

    if not config:
        print("✗ No instances found in cloudfront config files.", file=sys.stderr)
        sys.exit(1)

    print("Available instances:")
    for name in config:
        print(f"  {name}")

    bu_name = None
    while not bu_name:
        bu_name = input("Enter instance name: ").strip()
        if bu_name not in config:
            print(f"✗ '{bu_name}' not found. Available: {list(config.keys())}")
            bu_name = None

    instance_arn = config[bu_name]["instance_arn"]
    print(f"  Loaded instance ARN: {instance_arn}")
    main(instance_arn)
