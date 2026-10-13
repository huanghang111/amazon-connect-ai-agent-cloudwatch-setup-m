#!/usr/bin/env python3
"""
sync_log_handler.py

用途:
  AWS Lambda 入口。按实例执行 Connect / Gateway 日志同步，并触发已有的
  云端解析流程。

执行流程:
  1) 读取部署包中携带的配置文件 `config-<account_id>.json`
  2) 根据事件参数决定同步全部实例，或只同步指定实例
  3) 查询实例的 InstanceAlias
  4) 调用 fetch-to-s3.py 拉取 Connect / Gateway 日志到 S3
  5) 写入 trigger/<inst-key>.json 触发解析 Lambda
  6) 轮询 <inst-key>/index.json 获取解析结果

固定配置:
  - 配置文件: 使用部署时打包进 Lambda 的 `config-<account_id>.json`

Lambda 事件参数:
  - `event.instance`: 指定只同步某个实例（按 connect 配置里的 key 匹配，如 `uat-meg`）
  - `event.hours`: 本次同步最近多少小时的日志；不传时默认 144
  - 不传时默认同步全部实例

前置条件:
  - 基础设施已由 `setup_cloudfront.py` 创建完成
  - 日志桶 `s3://connect-ai-logs-<account_id>` 已存在
  - S3 事件通知 `trigger/*.json -> splitter Lambda` 已配置完成
"""

import json
import os
import re
import subprocess
import sys
import threading
import time

import boto3
from botocore.exceptions import ClientError


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATTERN = re.compile(r"config-\d{12}\.json")
SYNC_HOURS = "144"
IN_LAMBDA = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

_AWS_PROFILE = None if IN_LAMBDA else (os.environ.get("AWS_PROFILE", "").strip() or None)


def _session(region=None):
    kw = {}
    if region:
        kw["region_name"] = region
    if _AWS_PROFILE:
        kw["profile_name"] = _AWS_PROFILE
    return boto3.Session(**kw)


def _client(service, region=None):
    return _session(region).client(service)


def arn_region(arn):
    return arn.split(":")[3]


def arn_log_group(arn):
    rest = arn.split(":log-group:", 1)[1]
    return rest.rstrip(":*").rstrip(":")


def validate_bucket(name):
    if not re.match(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$", name):
        print(f"错误: 生成的桶名不合法: {name}", file=sys.stderr)
        sys.exit(1)


def bucket_exists(s3, name):
    try:
        s3.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def s3_put(s3, bucket, key, body, content_type="text/plain; charset=utf-8"):
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def s3_object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def resolve_config_path():
    search_dirs = [
        SCRIPT_DIR,
        os.path.dirname(os.path.dirname(SCRIPT_DIR)),
    ]
    matches = []
    for folder in search_dirs:
        if not os.path.isdir(folder):
            continue
        for name in os.listdir(folder):
            if CONFIG_PATTERN.fullmatch(name):
                matches.append(os.path.join(folder, name))
    matches = sorted(set(matches))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        print("错误: 找不到配置文件 config-<account_id>.json", file=sys.stderr)
        sys.exit(1)
    print(
        "错误: 找到多个配置文件，请确保 Lambda 部署包中只包含一个 config-<account_id>.json",
        file=sys.stderr,
    )
    for path in matches:
        print(f"  - {path}", file=sys.stderr)
    sys.exit(1)


def parse_account_id(config_path):
    m = re.fullmatch(r"config-(\d{12})\.json", os.path.basename(config_path))
    if not m:
        print(
            f"错误: 配置文件名必须符合 config-<account_id>.json 格式，当前文件: {os.path.basename(config_path)}",
            file=sys.stderr,
        )
        sys.exit(1)
    return m.group(1)


def resolve_fetcher_path():
    candidates = [
        os.path.join(SCRIPT_DIR, "fetch-to-s3.py"),
        os.path.join(os.path.dirname(SCRIPT_DIR), "fetch-to-s3.py"),
        os.path.join(SCRIPT_DIR, "lib", "fetch-to-s3.py"),
    ]
    for fetcher in candidates:
        if os.path.isfile(fetcher):
            return fetcher
    print("错误: 找不到依赖脚本: fetch-to-s3.py", file=sys.stderr)
    sys.exit(1)


def build_fetch_args(fetcher, fetch_region, log_group, bucket, key, s3_region, label, profile, start_ms):
    base = [
        sys.executable,
        fetcher,
        "--region", fetch_region,
        "--log-group", log_group,
        "--bucket", bucket,
        "--key", key,
        "--s3-region", s3_region,
        "--label", label,
    ]
    if profile:
        base += ["--profile", profile]
    if start_ms:
        base += ["--start-ms", start_ms]
    return base


# ---------------------------------------------------------------------------
# Lambda 入口
# ---------------------------------------------------------------------------

def handler(event, context):
    fetcher = resolve_fetcher_path()

    config_path = resolve_config_path()
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)

    connect_instances = cfg.get("connect", {})
    if not connect_instances:
        print("错误: 配置文件中的 connect 字典为空。", file=sys.stderr)
        sys.exit(1)

    account_id = parse_account_id(config_path)

    target_instance = ""
    if isinstance(event, dict):
        target_instance = str(event.get("instance", "")).strip()
    if target_instance and target_instance not in connect_instances:
        print(
            f"错误: 指定实例不存在: {target_instance}。可选值: {', '.join(sorted(connect_instances))}",
            file=sys.stderr,
        )
        sys.exit(1)
    sync_instances = (
        {target_instance: connect_instances[target_instance]}
        if target_instance else connect_instances
    )

    hours = SYNC_HOURS
    if isinstance(event, dict) and event.get("hours") is not None:
        hours = str(event.get("hours")).strip()
    if not hours.isdigit():
        print(f"错误: hours 必须是非负整数，当前值: {hours}", file=sys.stderr)
        sys.exit(1)

    region = "us-east-1"

    logs_bucket = f"connect-ai-logs-{account_id}"
    lambda_name = f"connect-ai-logs-{account_id}-splitter"
    validate_bucket(logs_bucket)

    s3 = _client("s3", region)
    if not bucket_exists(s3, logs_bucket):
        print(
            f"错误: 日志桶不存在: s3://{logs_bucket}。请先运行 setup_cloudfront.py 完成基础设施部署。",
            file=sys.stderr,
        )
        sys.exit(1)

    start_ms = ""
    if hours != "0":
        start_ms = str(int((time.time() - int(hours) * 3600) * 1000))

    print("===================================================================")
    print(" 日志同步配置")
    print(f"   配置文件          : {os.path.basename(config_path)}")
    print(f"   区域(region)      : {region}")
    print(f"   账号(account)     : {account_id}")
    print(f"   实例数量          : {len(connect_instances)}")
    for k, v in connect_instances.items():
        print(f"     · {k}  ({arn_region(v['instance_arn'])})")
    print(f"   本次同步实例      : {target_instance if target_instance else '全部'}")
    print(f"   日志存储桶        : {logs_bucket}")
    print(f"   拉取范围          : {'全部历史' if hours == '0' else f'最近 {hours} 小时'}")
    print("===================================================================")

    profile_name = "" if IN_LAMBDA else (_AWS_PROFILE or "")
    csat_attr_key = os.environ.get("CSAT_ATTRIBUTE_KEY", "botevaluation")

    instance_names = {}
    for inst_key, inst_cfg in sync_instances.items():
        inst_arn = inst_cfg["instance_arn"]
        inst_region = arn_region(inst_arn)
        inst_id = inst_arn.split("/")[-1]

        print(f"==> [{inst_key}] 查询 InstanceAlias ...")
        try:
            connect_cli = _client("connect", inst_region)
            resp = connect_cli.describe_instance(InstanceId=inst_id)
            alias = resp.get("Instance", {}).get("InstanceAlias", "") or ""
        except Exception:
            alias = ""
        if not alias:
            print(f"错误: [{inst_key}] 无法查询 InstanceAlias，请确认实例 ARN 与权限。", file=sys.stderr)
            sys.exit(1)
        instance_names[inst_key] = alias
        print(f"    InstanceAlias: {alias}")

    fetch_plan = {}
    for inst_key, inst_cfg in sync_instances.items():
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
            print(f"==> [{inst_key}] 启动拉取 AI Agent 日志 -> s3://{logs_bucket}/{raw_connect_key}")
            procs.append((
                f"{inst_key}-connect",
                subprocess.Popen(
                    build_fetch_args(
                        fetcher,
                        connect_region,
                        connect_lg,
                        logs_bucket,
                        raw_connect_key,
                        region,
                        f"{inst_key}-connect",
                        profile_name,
                        start_ms,
                    )
                ),
            ))
        else:
            print(f"    [{inst_key}] 未配置 ai_log_arn，跳过 AI Agent 日志拉取。")

        if gw_log_arn and gateway_lg:
            raw_gateway_key = f"raw/{inst_key}/gateway.ndjson"
            print(f"==> [{inst_key}] 启动拉取 Gateway 日志  -> s3://{logs_bucket}/{raw_gateway_key}")
            procs.append((
                f"{inst_key}-gateway",
                subprocess.Popen(
                    build_fetch_args(
                        fetcher,
                        gateway_region,
                        gateway_lg,
                        logs_bucket,
                        raw_gateway_key,
                        region,
                        f"{inst_key}-gateway",
                        profile_name,
                        start_ms,
                    )
                ),
            ))

        fetch_plan[inst_key] = {
            "instance_name": instance_name,
            "prefix": prefix,
            "raw_connect_key": raw_connect_key,
            "raw_gateway_key": raw_gateway_key,
            "procs": procs,
        }

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

    instance_results = {}
    lock = threading.Lock()

    def trigger_and_poll(inst_key, plan):
        raw_connect_key = plan["raw_connect_key"]
        raw_gateway_key = plan["raw_gateway_key"]
        instance_name = plan["instance_name"]
        prefix = plan["prefix"]

        if not raw_connect_key:
            with lock:
                instance_results[inst_key] = {
                    "alias": instance_name,
                    "contact_count": 0,
                    "error": "no ai_log_arn",
                }
            return

        failed_labels = [lbl for lbl, _ in plan["procs"] if lbl in fetch_failed]
        if failed_labels:
            with lock:
                instance_results[inst_key] = {
                    "alias": instance_name,
                    "contact_count": 0,
                    "error": f"fetch failed: {failed_labels}",
                }
            return

        try:
            s3.delete_object(Bucket=logs_bucket, Key=f"{prefix}index.json")
        except ClientError:
            pass

        inst_arn = sync_instances[inst_key]["instance_arn"]
        inst_id = inst_arn.split("/")[-1]
        inst_region = arn_region(inst_arn)
        trigger_body = {
            "instanceName": instance_name,
            "instanceId": inst_id,
            "connectRegion": inst_region,
            "csatAttrKey": csat_attr_key,
            "connect": raw_connect_key,
            "prefix": prefix,
        }
        if raw_gateway_key:
            trigger_body["gateway"] = raw_gateway_key

        trigger_key = f"trigger/{inst_key}.json"
        print(f"==> [{inst_key}] 写入触发对象 {trigger_key}")
        s3_put(s3, logs_bucket, trigger_key, json.dumps(trigger_body), "application/json")

        print(f"==> [{inst_key}] 等待 Lambda 生成 {prefix}index.json...")
        contact_count = "?"
        parse_error = ""
        for _ in range(200):
            if s3_object_exists(s3, logs_bucket, f"{prefix}index.json"):
                try:
                    idx = json.loads(s3.get_object(Bucket=logs_bucket, Key=f"{prefix}index.json")["Body"].read())
                    contact_count = str(idx.get("contactCount", "?"))
                    parse_error = idx.get("error", "")
                except Exception:
                    pass
                break
            time.sleep(5)

        if parse_error:
            print(f"  [{inst_key}] Lambda 解析报错: {parse_error}")
            print(f"  查看日志: aws logs tail /aws/lambda/{lambda_name} --region {region} --follow")
        elif contact_count == "?":
            print(f"  [{inst_key}] Lambda 仍在后台处理，index.json 生成后站点即可加载数据。")
        elif contact_count == "0":
            print(f"  [{inst_key}] 提示: 未解析出任何 Contact。")
        else:
            print(f"  [{inst_key}] 完成: {contact_count} 个 Contact")

        with lock:
            instance_results[inst_key] = {
                "alias": instance_name,
                "contact_count": contact_count,
                "error": parse_error,
            }

    threads = [
        threading.Thread(target=trigger_and_poll, args=(k, v), daemon=True)
        for k, v in fetch_plan.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print()
    print("===================================================================")
    print(" 同步完成")
    print("-------------------------------------------------------------------")
    print(f" 日志存储桶:          s3://{logs_bucket}")
    print(" 实例处理结果:")
    for inst_key, res in instance_results.items():
        alias = res.get("alias", "")
        cc = res.get("contact_count", "?")
        err = res.get("error", "")
        if err and err != "no ai_log_arn":
            print(f"   · {inst_key} ({alias}): 解析报错 — {err}")
        elif cc == "?":
            print(f"   · {inst_key} ({alias}): 后台处理中")
        else:
            print(f"   · {inst_key} ({alias}): {cc} 个 Contact")
    if target_instance:
        print(f" 本次仅同步实例:      {target_instance}")
    else:
        print(" 本次同步实例:        全部")
    print("===================================================================")

    return {
        "config": os.path.basename(config_path),
        "instance": target_instance,
        "hours": hours,
        "logs_bucket": logs_bucket,
        "results": instance_results,
    }
