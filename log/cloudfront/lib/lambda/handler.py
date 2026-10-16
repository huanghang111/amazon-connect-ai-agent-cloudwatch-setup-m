#!/usr/bin/env python3
"""S3-triggered splitter Lambda for Connect AI Agent logs."""
import concurrent.futures
import datetime
import gzip
import json
import os
import traceback
import urllib.parse

import boto3
from botocore.config import Config

import connect_parser as cp
from contact_summary import build_contact_summary

UNATTACHED_GATEWAY = "unattached-gateway-logs"
DOUBLE_UUID_SESSION = "double-uuid-session"
CONTACTS_TABLE = os.environ.get("CONTACTS_TABLE", "")
LOGS_BUCKET = os.environ.get("LOGS_BUCKET", "")
UPLOAD_WORKERS = max(1, int(os.environ.get("UPLOAD_WORKERS", "16")))

s3 = boto3.client("s3", config=Config(
    max_pool_connections=UPLOAD_WORKERS + 4,
    retries={"max_attempts": 5, "mode": "adaptive"},
))
dynamo_res = boto3.resource("dynamodb", config=Config(
    max_pool_connections=UPLOAD_WORKERS + 4,
    retries={"max_attempts": 5, "mode": "adaptive"},
))
_connect_clients = {}


def _connect_client(region):
    if region not in _connect_clients:
        _connect_clients[region] = boto3.client(
            "connect", region_name=region,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _connect_clients[region]


def _read_ndjson(bucket, key, source):
    rows = []
    response = s3.get_object(Bucket=bucket, Key=key)
    for raw in response["Body"].iter_lines():
        if not raw:
            continue
        try:
            event = json.loads(raw.decode("utf-8", "ignore"))
        except (ValueError, TypeError):
            continue
        timestamp, message = event.get("timestamp"), event.get("message")
        if timestamp is None or message is None:
            continue
        rows.append({
            "timestamp": int(timestamp),
            "message": cp.sanitize_message(message),
            "source": source,
        })
    return rows


def _resolve_contact_id(row):
    contact_id = row.get("contactId") or ""
    if contact_id:
        if len(contact_id) > 36 and contact_id[36] == "-":
            return DOUBLE_UUID_SESSION
        return contact_id
    if row.get("source") == "gateway":
        return UNATTACHED_GATEWAY
    _, session_id = cp._extract_connect_ids(row.get("message", ""))
    return session_id or "unknown"


def _write_contact(bucket, instance_name, contact_id, rows, instance_id="",
                   connect_region="", csat_attr_key="botevaluation"):
    """Write one contact's events and complete production summary."""
    contacts_table = dynamo_res.Table(CONTACTS_TABLE)
    events_key = f"events/{instance_name}/{contact_id}.json.gz"
    rows = sorted(rows, key=lambda row: (
        row["timestamp"], row.get("source", "connect"), row["message"]
    ))
    log_summary = build_contact_summary(rows)

    event_items = [{
        "timestamp": row["timestamp"],
        "message": row["message"],
        "source": row.get("source", "connect"),
        "contactId": contact_id,
    } for row in rows]
    s3.put_object(
        Bucket=bucket,
        Key=events_key,
        Body=gzip.compress(json.dumps(event_items, ensure_ascii=False).encode("utf-8"), compresslevel=6),
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    channel = initiation_method = disconnect_reason = did = csat = ""
    if instance_id and connect_region:
        try:
            response = _connect_client(connect_region).describe_contact(
                InstanceId=instance_id, ContactId=contact_id
            )
            contact = response.get("Contact", {})
            channel = contact.get("Channel", "")
            initiation_method = contact.get("InitiationMethod", "")
            disconnect_reason = contact.get("DisconnectReason", "")
            did = (contact.get("SystemEndpoint") or {}).get("Address", "")
            csat = (contact.get("Attributes") or {}).get(csat_attr_key, "")
        except Exception as error:  # noqa: BLE001
            print(f"DescribeContact failed for {contact_id}: {error}")

    ddb_item = {
        "instanceName": instance_name,
        "contactId": contact_id,
        "generatedAt": generated_at,
        "channel": channel,
        "instChannel": f"{instance_name}#{channel}",
        "initiationMethod": initiation_method,
        "disconnectReason": disconnect_reason,
        "did": did,
        "csat": csat,
    }
    ddb_item.update({key: value for key, value in log_summary.items() if value is not None})
    contacts_table.put_item(Item=ddb_item)

    summary = dict(log_summary)
    summary["sourceCounts"] = json.loads(log_summary["sourceCounts"])
    summary.update({
        "contactId": contact_id,
        "channel": channel,
        "instChannel": f"{instance_name}#{channel}",
        "initiationMethod": initiation_method,
        "disconnectReason": disconnect_reason,
        "did": did,
        "csat": csat,
        "_summaryOnly": True,
    })
    summary = {key: value for key, value in summary.items() if value is not None}
    return summary, "created"


def _process(bucket, connect_key, gateway_key, prefix, instance_name,
             instance_id="", connect_region="", csat_attr_key="botevaluation"):
    connect_rows = _read_ndjson(bucket, connect_key, "connect")
    gateway_rows = _read_ndjson(bucket, gateway_key, "gateway") if gateway_key else []
    cp.correlate(connect_rows, gateway_rows)
    connect_rows.extend(gateway_rows)
    connect_rows.sort(key=lambda row: row["timestamp"])
    event_count = len(connect_rows)

    groups = {}
    for row in connect_rows:
        contact_id = _resolve_contact_id(row)
        if contact_id in (UNATTACHED_GATEWAY, DOUBLE_UUID_SESSION):
            continue
        groups.setdefault(contact_id, []).append(row)

    contacts_summary = []
    workers = min(UPLOAD_WORKERS, len(groups)) or 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _write_contact, bucket, instance_name, contact_id, rows,
                instance_id, connect_region, csat_attr_key,
            ): contact_id
            for contact_id, rows in groups.items()
        }
        for future in concurrent.futures.as_completed(futures):
            summary, _ = future.result()
            contacts_summary.append(summary)
    contacts_summary.sort(key=lambda entry: entry["contactId"])

    index = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "instanceName": instance_name,
        "contactCount": len(contacts_summary),
        "eventCount": event_count,
        "storage": "s3",
    }
    s3.put_object(
        Bucket=bucket, Key=prefix + "index.json",
        Body=json.dumps(index, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    print("processed: instance=%s contacts=%d events=%d workers=%d" % (
        instance_name, len(contacts_summary), event_count, workers
    ))
    return index


def _write_error_index(bucket, prefix, error, instance_name):
    try:
        s3.put_object(
            Bucket=bucket, Key=prefix + "index.json",
            Body=json.dumps({
                "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
                "instanceName": instance_name,
                "error": str(error),
                "contactCount": 0,
                "eventCount": 0,
                "storage": "s3",
            }, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:  # noqa: BLE001
        pass


def handler(event, context):
    if not CONTACTS_TABLE or not LOGS_BUCKET:
        raise RuntimeError(
            "环境变量 CONTACTS_TABLE、LOGS_BUCKET 未完整设置。"
            "请通过 setup_cloudfront.py 重新部署以注入正确的值。"
        )
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        trigger = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        prefix = trigger.get("prefix", "")
        instance_name = trigger.get("instanceName", "")
        if not instance_name:
            raise RuntimeError(f"触发 JSON {key} 中缺少 instanceName 字段，无法确定数据归属实例。")
        try:
            _process(
                bucket, trigger["connect"], trigger.get("gateway"), prefix, instance_name,
                trigger.get("instanceId", ""), trigger.get("connectRegion", ""),
                trigger.get("csatAttrKey", os.environ.get("CSAT_ATTRIBUTE_KEY", "botevaluation")),
            )
        except Exception as error:  # noqa: BLE001
            traceback.print_exc()
            _write_error_index(bucket, prefix, error, instance_name)
            raise
    return {"ok": True}
