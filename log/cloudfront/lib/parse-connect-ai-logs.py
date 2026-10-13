#!/usr/bin/env python3
"""
parse-connect-ai-logs.py

把两路 CloudWatch 日志归一化、关联成前端可消费的数据文件 data.js:
  1) Amazon Connect AI Agent 日志       (source = "connect")
  2) Bedrock AgentCore Gateway 应用日志  (source = "gateway")

输入(每路二选一格式，按扩展名/内容自动识别):
  --connect FILE   Connect AI Agent 日志: filter-log-events 的 JSON，或控制台导出的 CSV
  --gateway FILE   Bedrock AgentCore Gateway 日志: 同上(可选)

输出:
  --out FILE       生成 data.js: window.__CONNECT_AI_LOG_DATA__ = [...]
                   每个元素形如:
                     {"timestamp": <epoch_ms>, "message": "<原始消息字符串>",
                      "source": "connect"|"gateway", "contactId": "<可选,关联到的会话>"}

关联逻辑(把 gateway 事件挂到正确的 Contact 下):
  - Connect 日志里 session_name == Amazon Connect 的 Contact ID，
    session_id 是底层 Q in Connect 会话 ID。
  - 对每条 gateway 事件:
      a) 若其消息文本中直接出现某个已知 contactId / sessionId(子串匹配) -> 关联到该 Contact;
      b) 否则按时间落入某个 Contact 的活动时间窗 -> 关联到该 Contact;
      c) 都不满足 -> contactId 留空，前端归入「未关联的 Gateway 日志」。
  真正的结构化展示交给前端 app.js。
"""
import argparse
import csv
import datetime
import json
import re
import sys


# ---------------------------------------------------------------------------
# 通用: 把单条 message 归一化成"保证能被 JSON.parse 的字符串"(若它本来就是 JSON)
# ---------------------------------------------------------------------------
def sanitize_message(msg):
    """修复模型多行输出导致的非法 JSON 转义;非 JSON 文本原样返回。

    这些日志的 message 常常是一段 JSON 文本，但模型多行输出会被写成
    "反斜杠 + 真实换行" 等非法 JSON 转义，导致 JSON.parse / json.loads 失败。
    策略: 折叠非法转义 -> 解析 -> 重新 json.dumps，确保前端拿到合法 JSON;
    若本就不是 JSON(如 gateway 的纯文本日志)，原样返回。
    """
    if not isinstance(msg, str):
        return msg

    # 先尝试原样解析(已经是合法 JSON 就别动它)
    try:
        return json.dumps(json.loads(msg), ensure_ascii=False)
    except (ValueError, TypeError):
        pass

    fixed = msg
    # 反斜杠 + 换行 -> \n
    fixed = re.sub(r"\\(?:\r\n|\r|\n)", r"\\n", fixed)
    # 残留裸控制字符 -> 合法转义
    fixed = (fixed.replace("\r\n", r"\n").replace("\n", r"\n")
                  .replace("\r", r"\n").replace("\t", r"\t"))
    try:
        return json.dumps(json.loads(fixed), ensure_ascii=False)
    except (ValueError, TypeError):
        # 不是 JSON(纯文本日志) -> 原样返回
        return msg


# ---------------------------------------------------------------------------
# 读取: 自动识别 filter-log-events JSON / 控制台导出 CSV
# ---------------------------------------------------------------------------
def load_any(path, source):
    """返回 [{timestamp, message, source}]，自动识别 JSON / 制表符文本(.log) / CSV。"""
    with open(path, "r", encoding="utf-8") as f:
        head = f.read(1)
    if head == "{" or head == "[":
        return _load_events_json(path, source)
    # 探测 load-cloudwatch-logs.sh 生成的 events.log:
    #   "<datetime>\t<logStreamName>\t<message>"，制表符分隔且首列是时间戳
    with open(path, "r", encoding="utf-8") as f:
        first = f.readline()
    if "\t" in first and _parse_dt_ms(first.split("\t", 1)[0]) is not None:
        return _load_text_log(path, source)
    return _load_csv(path, source)


def _parse_dt_ms(s):
    """把可读时间字符串解析成 epoch 毫秒；无法解析返回 None。"""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.datetime.strptime(s, fmt).replace(tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def _load_text_log(path, source):
    """读取制表符分隔的 events.log；跨多行的 message 会被并回同一条记录。"""
    rows = []
    cur = None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t", 2)
            ms = _parse_dt_ms(parts[0]) if parts else None
            if ms is not None and len(parts) == 3:
                if cur is not None:
                    rows.append(cur)
                cur = {"timestamp": ms, "message": parts[2], "source": source}
            elif cur is not None:
                # 多行 message 的续行，拼回上一条
                cur["message"] += "\n" + line
        if cur is not None:
            rows.append(cur)
    for r in rows:
        r["message"] = sanitize_message(r["message"])
    return rows


def _load_events_json(path, source):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        events = data.get("events", [])
    elif isinstance(data, list):
        events = data
    else:
        events = []
    rows = []
    for e in events:
        ts = e.get("timestamp")
        msg = e.get("message")
        if ts is None or msg is None:
            continue
        rows.append({"timestamp": int(ts), "message": sanitize_message(msg), "source": source})
    return rows


def _load_csv(path, source):
    rows = []
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # 跳过表头 timestamp,message
        for rec in reader:
            if not rec or len(rec) < 2:
                continue
            try:
                ts = int(rec[0])
            except (ValueError, TypeError):
                continue
            rows.append({"timestamp": ts, "message": sanitize_message(rec[1]), "source": source})
    return rows


# ---------------------------------------------------------------------------
# 关联: 给每条 connect 行算出 contactId/sessionId，并据此把 gateway 行挂上去
# ---------------------------------------------------------------------------
def _extract_connect_ids(msg):
    """从一条 connect 消息里取 (contactId, sessionId)。"""
    contact_id, session_id = "", ""
    try:
        o = json.loads(msg)
    except (ValueError, TypeError):
        return contact_id, session_id
    session_id = o.get("session_id", "") or ""
    if o.get("session_name"):
        contact_id = o["session_name"]
    elif o.get("span"):
        m = re.search(r"session_name=([^,}]+)", str(o["span"]))
        if m:
            contact_id = m.group(1).strip()
    return contact_id, session_id


def correlate(connect_rows, gateway_rows):
    """就地为 connect_rows / gateway_rows 补充 contactId。"""
    # 1. connect: 建 sid->cid 映射 + 每个 contact 的时间窗
    sid_to_cid = {}
    for r in connect_rows:
        cid, sid = _extract_connect_ids(r["message"])
        r["contactId"] = cid  # 可能为空，前端也会自行解析兜底
        r["_sid"] = sid
        if cid and sid:
            sid_to_cid[sid] = cid
    # 用 sid 兜底补 cid
    for r in connect_rows:
        if not r.get("contactId") and r.get("_sid") and sid_to_cid.get(r["_sid"]):
            r["contactId"] = sid_to_cid[r["_sid"]]

    # 已知 id 集合(contactId 与 sessionId)，用于 gateway 子串匹配
    known = {}  # id字符串 -> contactId
    contact_windows = {}  # cid -> [min_ts, max_ts]
    for r in connect_rows:
        cid = r.get("contactId") or r.get("_sid") or ""
        if not cid:
            continue
        if r.get("contactId"):
            known[r["contactId"]] = r["contactId"]
        if r.get("_sid"):
            known[r["_sid"]] = r.get("contactId") or r["_sid"]
        w = contact_windows.setdefault(cid, [r["timestamp"], r["timestamp"]])
        w[0] = min(w[0], r["timestamp"])
        w[1] = max(w[1], r["timestamp"])
    for r in connect_rows:
        r.pop("_sid", None)

    # 2. gateway: 先子串匹配，再时间窗
    PAD = 5000  # 时间窗左右各放宽 5s
    for g in gateway_rows:
        assigned = ""
        msg = g["message"]
        for idstr, cid in known.items():
            if idstr and idstr in msg:
                assigned = cid
                break
        if not assigned:
            ts = g["timestamp"]
            best, best_gap = "", None
            for cid, (lo, hi) in contact_windows.items():
                if lo - PAD <= ts <= hi + PAD:
                    # 落入窗口，按到窗口中心的距离挑最近的
                    center = (lo + hi) / 2.0
                    gap = abs(ts - center)
                    if best_gap is None or gap < best_gap:
                        best, best_gap = cid, gap
            assigned = best
        g["contactId"] = assigned


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="归一化并关联 Connect AI Agent + AgentCore Gateway 日志，生成 data.js")
    ap.add_argument("--connect", required=True, help="Connect AI Agent 日志文件(filter-log-events JSON 或 CSV)")
    ap.add_argument("--gateway", help="Bedrock AgentCore Gateway 日志文件(filter-log-events JSON 或 CSV)")
    ap.add_argument("--out", required=True, help="输出的 data.js 路径")
    args = ap.parse_args()

    connect_rows = load_any(args.connect, "connect")
    gateway_rows = load_any(args.gateway, "gateway") if args.gateway else []

    correlate(connect_rows, gateway_rows)

    rows = connect_rows + gateway_rows
    rows.sort(key=lambda r: r["timestamp"])

    payload = json.dumps(rows, ensure_ascii=False)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("// 由 parse-connect-ai-logs.py 自动生成，请勿手工编辑\n")
        f.write("window.__CONNECT_AI_LOG_DATA__ = ")
        f.write(payload)
        f.write(";\n")

    n_gw_linked = sum(1 for g in gateway_rows if g.get("contactId"))
    print(
        f"已写入 {args.out}: connect={len(connect_rows)} 条, "
        f"gateway={len(gateway_rows)} 条(已关联 {n_gw_linked} 条)。",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
