#!/usr/bin/env python3
"""
query_handler.py — API Gateway 触发的查询 Lambda

职责:
  作为前端与 DynamoDB / S3 之间的鉴权代理。前端不再直接访问 DynamoDB，
  所有查询请求都经过本 Lambda，由本 Lambda 验证用户的实例访问权限后
  再执行查询并返回结果。

鉴权方式:
  API Gateway 配置 Cognito User Pool Authorizer，验证 Authorization
  header 中的 IdToken。Lambda 从 $context.authorizer.claims 读取
  custom:instances 字段，决定当前用户允许访问哪些实例：
    - custom:instances = "*"       → 允许所有实例
    - custom:instances = "a,b,c"  → 只允许访问 a、b、c 三个实例

API 路由（由 API Gateway 路径参数决定）:
  GET /instances
      返回当前用户有权访问的实例列表。
      响应: {"instances": ["uat-sg", "uat-msg", ...]}

  GET /contacts?instanceName=xxx[&nextToken=xxx]
      按 instanceName 查询 Contact 摘要列表（分页）。
      响应: {"items": [...], "nextToken": "..."}

  GET /contacts/{contactId}/events?instanceName=xxx
      生成 S3 presigned URL，前端直接从 S3 下载事件明细(gzip JSON)。
      响应: {"url": "https://..."}

  GET /contacts/{contactId}/describe?instanceName=xxx
      调用 Amazon Connect DescribeContact，返回通话详情。
      响应: {"Contact": {...}}

环境变量（由 setup_cloudfront.py 注入）:
  CONTACTS_TABLE    — Contact 摘要表名
  LOGS_BUCKET       — 日志 S3 桶名（存放 events/{instanceName}/{contactId}.json.gz）
  ALL_INSTANCES     — 逗号分隔的全量实例名列表，供 * 权限展开用
  INSTANCES_META    — JSON 字符串，instanceName -> {instanceId, region}
"""
import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config

# 环境变量
CONTACTS_TABLE  = os.environ.get("CONTACTS_TABLE", "")
LOGS_BUCKET     = os.environ.get("LOGS_BUCKET", "")
ALL_INSTANCES   = os.environ.get("ALL_INSTANCES", "")
INSTANCES_META  = json.loads(os.environ.get("INSTANCES_META", "{}"))
# 翻译功能：使用的 Bedrock 模型 ID，可通过环境变量覆盖
TRANSLATE_MODEL = os.environ.get("TRANSLATE_MODEL", "amazon.nova-lite-v1:0")
# 翻译目标语言，可通过环境变量覆盖（默认中文）
TRANSLATE_LANG  = os.environ.get("TRANSLATE_LANG", "中文")

# presigned URL 有效期（秒）
PRESIGNED_EXPIRES = 300

_ddb_cfg = Config(retries={"max_attempts": 5, "mode": "adaptive"})
_ddb = boto3.resource("dynamodb", config=_ddb_cfg)
_s3  = boto3.client("s3", config=Config(retries={"max_attempts": 3, "mode": "adaptive"}))
_bedrock = boto3.client("bedrock-runtime", region_name="us-east-1",
                        config=Config(retries={"max_attempts": 2, "mode": "standard"}))

# Connect 客户端缓存（按 region 分）
_connect_clients = {}

def _connect(region):
    if region not in _connect_clients:
        _connect_clients[region] = boto3.client("connect", region_name=region)
    return _connect_clients[region]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Authorization,Content-Type",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json",
    }


def _json_default(obj):
    """DynamoDB boto3.resource 返回的数值是 Decimal，需要转换为 int 或 float。"""
    from decimal import Decimal
    import datetime
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _ok(body):
    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps(body, ensure_ascii=False, default=_json_default),
    }


def _err(code, msg):
    return {
        "statusCode": code,
        "headers": _cors_headers(),
        "body": json.dumps({"error": msg}, ensure_ascii=False),
    }


def _allowed_instances(claims):
    """从 JWT claims 解析当前用户允许访问的实例集合。
    custom:instances = "*" → 返回 ALL_INSTANCES 展开的全集（set）。
    custom:instances = "a,b" → 返回 {"a", "b"}。
    未设置或为空 → 返回空集合（拒绝所有）。
    """
    raw = (claims.get("custom:instances") or "").strip()
    if not raw:
        return set()
    if raw == "*":
        return set(x.strip() for x in ALL_INSTANCES.split(",") if x.strip())
    return set(x.strip() for x in raw.split(",") if x.strip())


def _check_instance(allowed, instance_name):
    """检查 instance_name 是否在允许集合中，不在则返回 403 响应，在则返回 None。"""
    if not instance_name:
        return _err(400, "缺少 instanceName 参数")
    if instance_name not in allowed:
        return _err(403, f"无权访问实例: {instance_name}")
    return None


# ---------------------------------------------------------------------------
# 路由处理
# ---------------------------------------------------------------------------

def _handle_instances(allowed):
    """GET /instances — 返回当前用户有权访问的实例列表（按字母排序）。"""
    instances = sorted(allowed)
    instance_labels = {}
    for instance_name in instances:
        config_key = (INSTANCES_META.get(instance_name) or {}).get("configKey", "")
        instance_labels[instance_name] = f"{config_key} ({instance_name})" if config_key else instance_name
    return _ok({"instances": instances, "instanceLabels": instance_labels})


def _handle_contacts(allowed, params):
    """GET /contacts?instanceName=xxx&pageSize=15&nextToken=xxx[&contactId=prefix][&channel=VOICE|CHAT]
    - 无 contactId：分页查 Contact 摘要，按 minTs 倒序。
    - 有 contactId：按主键前缀搜索（最多返回 pageSize 条，不分页，不计 total）。
    - channel：可选，VOICE 或 CHAT，使用 DDB FilterExpression 过滤。
    """
    instance_name = (params.get("instanceName") or "").strip()
    err = _check_instance(allowed, instance_name)
    if err:
        return err

    try:
        page_size = max(1, min(500, int(params.get("pageSize") or 15)))
    except (ValueError, TypeError):
        page_size = 15

    channel_filter = (params.get("channel") or "").strip().upper()
    if channel_filter not in ("VOICE", "CHAT"):
        channel_filter = ""

    table = _ddb.Table(CONTACTS_TABLE)

    # ---- contactId 前缀搜索模式（保持原样，channel 仍用 filter）----
    contact_id_prefix = (params.get("contactId") or "").strip()
    if contact_id_prefix:
        from boto3.dynamodb.conditions import Attr

        kw = {
            "KeyConditionExpression": (
                Key("instanceName").eq(instance_name)
                & Key("contactId").begins_with(contact_id_prefix)
            ),
            "Limit": page_size,
        }
        if channel_filter:
            kw["FilterExpression"] = Attr("channel").eq(channel_filter)
        resp = table.query(**kw)
        return _ok({"items": resp.get("Items", [])})

    # ---- 普通分页模式 ----
    if channel_filter:
        # 走复合键 GSI：channel 作为查询键，Limit 精确、分页正确
        kw = {
            "IndexName": "by-instance-channel-time",
            "KeyConditionExpression": Key("instChannel").eq(
                f"{instance_name}#{channel_filter}"
            ),
            "ScanIndexForward": False,
            "Limit": page_size,
        }
    else:
        # 无 channel：走原有 GSI
        kw = {
            "IndexName": "by-instance-time",
            "KeyConditionExpression": Key("instanceName").eq(instance_name),
            "ScanIndexForward": False,
            "Limit": page_size,
        }

    next_token = (params.get("nextToken") or "").strip()
    if next_token:
        try:
            kw["ExclusiveStartKey"] = json.loads(next_token)
        except (ValueError, TypeError):
            return _err(400, "nextToken 格式不合法")

    resp = table.query(**kw)
    items = resp.get("Items", [])
    result = {"items": items}
    if resp.get("LastEvaluatedKey"):
        result["nextToken"] = json.dumps(
            resp["LastEvaluatedKey"], ensure_ascii=False, default=_json_default
        )

    # 第一页返回总数
    if not next_token:
        if channel_filter:
            count_kw = {
                "IndexName": "by-instance-channel-time",
                "KeyConditionExpression": Key("instChannel").eq(
                    f"{instance_name}#{channel_filter}"
                ),
                "Select": "COUNT",
            }
        else:
            count_kw = {
                "IndexName": "by-instance-time",
                "KeyConditionExpression": Key("instanceName").eq(instance_name),
                "Select": "COUNT",
            }
        count_resp = table.query(**count_kw)
        result["totalContacts"] = count_resp.get("Count", 0)

    return _ok(result)


def _handle_events(allowed, params, contact_id):
    """GET /contacts/{contactId}/events?instanceName=xxx
    生成 S3 presigned URL，前端直接从 S3 下载 gzip JSON 事件明细，
    不再经过 Lambda 中转数据，大幅降低延迟和传输量。
    """
    instance_name = (params.get("instanceName") or "").strip()
    err = _check_instance(allowed, instance_name)
    if err:
        return err

    if not contact_id:
        return _err(400, "缺少 contactId 路径参数")

    events_key = f"events/{instance_name}/{contact_id}.json.gz"

    # 检查文件是否存在
    try:
        _s3.head_object(Bucket=LOGS_BUCKET, Key=events_key)
    except Exception:
        return _err(404, f"未找到事件文件: {contact_id}")

    # 生成 presigned URL，有效期 PRESIGNED_EXPIRES 秒
    url = _s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": LOGS_BUCKET, "Key": events_key},
        ExpiresIn=PRESIGNED_EXPIRES,
    )
    return _ok({"url": url})


def _fetch_contact_summary(contact):
    """从 Contact 对象的 Recordings 推算 Contact Lens 分析文件路径，读取摘要文字。
    取不到时返回空字符串，不抛异常。

    分析文件路径规律（与原版 auth.js __CONNECT_FETCH_SUMMARY__ 保持一致）:
      Recordings[].Location 格式: <bucket>/connect/<alias>/CallRecordings/<subdir>/<file>
      分析文件路径:               Analysis/Voice/<subdir>/<contactId>_analysis_<ts>.json
    """
    try:
        contact_id = contact.get("Id", "")
        channel    = (contact.get("Channel") or "VOICE").upper()
        analysis_root = "Analysis/Chat" if channel == "CHAT" else "Analysis/Voice"

        # 找主录音记录（优先 AUDIO/CHAT，排除辅助流）
        recs = contact.get("Recordings") or []
        cands = []
        for r in recs:
            if r and r.get("StorageType") == "S3" and r.get("Location"):
                mst = r.get("MediaStreamType") or ""
                score = 0 if mst in ("AUDIO", "CHAT", "") else 1
                cands.append((score, r["Location"]))
        cands.sort(key=lambda x: x[0])

        bucket = subdir = ""
        markers = ["CallRecordings/", "ChatTranscripts/"]
        for _, loc in cands:
            slash = loc.find("/")
            if slash < 0:
                continue
            b    = loc[:slash]
            rest = loc[slash + 1:]
            for marker in markers:
                idx = rest.find(marker)
                if idx >= 0:
                    sub  = rest[idx + len(marker):]   # ivr/2026/08/06/<file>
                    last = sub.rfind("/")
                    if last > 0:
                        bucket = b
                        subdir = sub[:last]            # ivr/2026/08/06
                        break
            if bucket:
                break

        if not bucket or not subdir:
            return ""

        prefix = f"{analysis_root}/{subdir}/{contact_id}_analysis_"
        out = _s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = sorted(c["Key"] for c in (out.get("Contents") or []) if c.get("Key"))
        if not keys:
            return ""

        body = _s3.get_object(Bucket=bucket, Key=keys[-1])["Body"].read()
        j    = json.loads(body)
        cc   = (j.get("ConversationCharacteristics") or {})
        cs   = (cc.get("ContactSummary") or {})
        ais  = (cs.get("AutomatedInteractionSummary") or {})
        return (ais.get("Content") or "").strip()
    except Exception:
        return ""


def _handle_translate(body_raw):
    """POST /translate — 调用 Amazon Bedrock 批量翻译对话文本。

    请求体: {"texts": ["text1", "text2", ...], "lang": "中文"}
    响应:   {"translations": ["译文1", "译文2", ...], "lang": "中文"}

    每次最多翻译 100 条，单条文本超过 2000 字符的截断处理。
    """
    try:
        body = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw or {})
    except Exception:
        return _err(400, "请求体不是合法的 JSON")

    texts = body.get("texts") or []
    if not isinstance(texts, list):
        return _err(400, "texts 必须是数组")
    if len(texts) > 100:
        return _err(400, "单次最多翻译 100 条文本")

    target_lang = (body.get("lang") or TRANSLATE_LANG).strip() or "中文"

    # 过滤空文本，记录非空索引，翻译后回填
    indexed = [(i, t[:2000]) for i, t in enumerate(texts) if t and t.strip()]
    if not indexed:
        return _ok({"translations": [""] * len(texts), "lang": target_lang})

    # 把所有文本一次性发给模型，用编号分隔，减少调用次数
    lines = "\n".join(f"[{i}] {t}" for i, t in indexed)
    prompt = (
        f"You are a professional translator. Translate each numbered text below into {target_lang}. "
        "Return ONLY the translations in the same numbered format [N] <translation>. "
        "Do not add explanations or change the numbering.\n\n"
        f"{lines}"
    )

    try:
        resp = _bedrock.invoke_model(
            modelId=TRANSLATE_MODEL,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({
                "messages": [{"role": "user", "content": [{"text": prompt}]}],
                "inferenceConfig": {"maxTokens": 4096, "temperature": 0.1},
            }),
        )
        result_body = json.loads(resp["body"].read())
        output_text = ""
        for block in (result_body.get("output", {}).get("message", {}).get("content") or []):
            if isinstance(block, dict) and block.get("text"):
                output_text += block["text"]
    except Exception as e:
        return _err(502, f"Bedrock 调用失败: {e}")

    # 解析编号译文回填
    import re as _re
    translations = [""] * len(texts)
    for orig_i, _ in indexed:
        # 匹配 [orig_i] 行（贪婪到下一个 [数字] 或末尾）
        m = _re.search(
            r"\[" + str(orig_i) + r"\]\s*(.*?)(?=\n\[\d+\]|\Z)",
            output_text,
            _re.DOTALL,
        )
        if m:
            translations[orig_i] = m.group(1).strip()

    return _ok({"translations": translations, "lang": target_lang})


def _handle_describe(allowed, params, contact_id):
    """GET /contacts/{contactId}/describe?instanceName=xxx — 调用 Connect DescribeContact。
    同时读取 Contact Lens S3 分析文件，将摘要附加在响应的 contactSummary 字段中。
    """
    instance_name = (params.get("instanceName") or "").strip()
    err = _check_instance(allowed, instance_name)
    if err:
        return err

    if not contact_id:
        return _err(400, "缺少 contactId 路径参数")

    # 从 INSTANCES_META 取该实例的 instanceId 和 region
    meta = INSTANCES_META.get(instance_name)
    if not meta:
        return _err(400, f"未找到实例 {instance_name} 的元数据")
    instance_id = meta.get("instanceId", "")
    region      = meta.get("region", "us-east-1")
    if not instance_id:
        return _err(400, f"实例 {instance_name} 未配置 instanceId")

    try:
        resp = _connect(region).describe_contact(
            InstanceId=instance_id,
            ContactId=contact_id,
        )
        contact = resp.get("Contact", {})
        summary = _fetch_contact_summary(contact)
        result  = {"Contact": contact}
        if summary:
            result["contactSummary"] = summary
        return _ok(result)
    except Exception as e:
        return _err(502, f"DescribeContact 失败: {e}")


# ---------------------------------------------------------------------------
# Lambda 入口
# ---------------------------------------------------------------------------

def handler(event, context):
    # OPTIONS 预检（CORS）
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": _cors_headers(), "body": ""}

    path   = event.get("path", "")
    method = event.get("httpMethod", "GET")

    # POST /translate — 无需 Cognito 鉴权（前端已持有 Cognito IdToken，但翻译是只读操作，
    # 且 Lambda 执行角色决定 Bedrock 权限；如需鉴权可自行开启下方注释）
    if method == "POST" and path == "/translate":
        return _handle_translate(event.get("body") or "")

    # 从 Cognito Authorizer 注入的 claims 解析用户权限
    req_ctx = event.get("requestContext", {})
    claims  = req_ctx.get("authorizer", {}).get("claims", {})
    if not claims:
        return _err(401, "未授权：缺少认证信息")

    allowed = _allowed_instances(claims)

    params = event.get("queryStringParameters") or {}

    # 路由分发
    # GET /instances
    if method == "GET" and path == "/instances":
        return _handle_instances(allowed)

    # GET /contacts
    if method == "GET" and path == "/contacts":
        return _handle_contacts(allowed, params)

    # GET /contacts/{contactId}/events 或 /contacts/{contactId}/describe
    # path 形如 /contacts/abc-123/events
    parts = [p for p in path.split("/") if p]
    if method == "GET" and len(parts) == 3 and parts[0] == "contacts":
        if parts[2] == "events":
            return _handle_events(allowed, params, parts[1])
        if parts[2] == "describe":
            return _handle_describe(allowed, params, parts[1])

    return _err(404, f"未知路径: {path}")
