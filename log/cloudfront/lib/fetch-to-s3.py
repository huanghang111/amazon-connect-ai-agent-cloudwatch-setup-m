#!/usr/bin/env python3
"""
fetch-to-s3.py — 把一个 CloudWatch 日志组的全部事件以 NDJSON 流式归档到 S3。

特点:
  - 用 boto3 逐页调用 filter_log_events，逐行序列化后流式写入 S3 的 multipart upload，
    本地不落文件、内存占用极小(不累积事件)；
  - 归档的是"原始 message"(不做任何改写)，作为原始数据长期保存；
    后续解析/清洗由云端 Lambda 完成。
  - 控制台实时显示已获取条数；结束后把总条数打印到 stdout 供调用方读取。

用法:
  python3 fetch-to-s3.py --region <r> --log-group <lg> --bucket <b> --key <k> \
      [--s3-region <r>] [--start-ms <ms>] [--profile <p>] [--label <name>]

依赖: boto3
"""
import argparse
import io
import json
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# 分片上传的最小分片大小: 5 MB(S3 要求除最后一片外不得小于 5 MB)
PART_SIZE = 5 * 1024 * 1024


def main():
    ap = argparse.ArgumentParser(description="流式归档 CloudWatch 日志组到 S3(boto3 版)")
    ap.add_argument("--region",    required=True, help="日志组所在区域")
    ap.add_argument("--log-group", required=True, help="日志组名")
    ap.add_argument("--bucket",    required=True, help="目标 S3 桶")
    ap.add_argument("--key",       required=True, help="目标对象键(如 raw/connect.ndjson)")
    ap.add_argument("--s3-region", default="",    help="S3 桶所在区域；空=与日志区域相同")
    ap.add_argument("--start-ms",  default="",    help="起始时间(epoch 毫秒)；空=全部历史")
    ap.add_argument("--profile",   default="",    help="AWS boto3 profile")
    ap.add_argument("--label",     default="",    help="进度显示用的名称")
    args = ap.parse_args()

    label = args.label or args.log_group
    s3_region = args.s3_region or args.region

    # 初始化 boto3 客户端
    session_kw = {}
    if args.profile:
        session_kw["profile_name"] = args.profile
    session = boto3.Session(**session_kw)

    retry_cfg = Config(retries={"max_attempts": 4, "mode": "adaptive"})
    logs_client = session.client("logs", region_name=args.region, config=retry_cfg)
    s3_client   = session.client("s3",   region_name=s3_region,   config=retry_cfg)

    # -----------------------------------------------------------------------
    # 启动 S3 分片上传(Multipart Upload)
    # 用分片上传代替单次 put_object，支持任意大小的日志文件，无 5 GB 限制。
    # -----------------------------------------------------------------------
    mpu = s3_client.create_multipart_upload(
        Bucket=args.bucket,
        Key=args.key,
        ContentType="text/plain; charset=utf-8",
    )
    upload_id = mpu["UploadId"]

    parts = []          # 已完成分片的 ETag 列表
    buf   = io.BytesIO()  # 当前缓冲区(积攒到 PART_SIZE 后上传一片)
    count = 0

    def _flush(final=False):
        """把缓冲区内容上传为一个分片；final=True 时即使不足 5 MB 也强制上传。"""
        data = buf.getvalue()
        if not data:
            return
        if not final and len(data) < PART_SIZE:
            return  # 还不够一片，继续积攒
        part_num = len(parts) + 1
        resp = s3_client.upload_part(
            Bucket=args.bucket,
            Key=args.key,
            UploadId=upload_id,
            PartNumber=part_num,
            Body=data,
        )
        parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
        buf.seek(0)
        buf.truncate(0)

    try:
        # -----------------------------------------------------------------------
        # 逐页拉取 CloudWatch 日志事件，逐行写入缓冲区
        # -----------------------------------------------------------------------
        next_token = None
        while True:
            kw = {
                "logGroupName": args.log_group,
                "interleaved": True,   # 多流按时间交错，确保全局有序
            }
            if args.start_ms:
                kw["startTime"] = int(args.start_ms)
            if next_token:
                kw["nextToken"] = next_token

            sys.stderr.write(f"\r  拉取 {label}… 已获取 {count} 条(正在查询下一页，请稍候)")
            sys.stderr.flush()

            # boto3 自带重试(adaptive 模式)，无需手动循环
            resp = logs_client.filter_log_events(**kw)

            for ev in resp.get("events", []):
                ts  = ev.get("timestamp")
                msg = ev.get("message")
                if ts is None or msg is None:
                    continue
                line = json.dumps({"timestamp": ts, "message": msg}, ensure_ascii=False)
                buf.write((line + "\n").encode("utf-8"))
                count += 1
                _flush()  # 积攒满 5 MB 就上传一片

            sys.stderr.write(f"\r  拉取 {label}… 已获取 {count} 条")
            sys.stderr.flush()

            next_token = resp.get("nextToken")
            if not next_token:
                break

        # 上传最后一片(可能不足 5 MB)
        _flush(final=True)

        # -----------------------------------------------------------------------
        # 完成分片上传
        # -----------------------------------------------------------------------
        if not parts:
            # 日志为空: S3 要求至少有一个分片，上传一个空对象
            parts_resp = s3_client.upload_part(
                Bucket=args.bucket, Key=args.key,
                UploadId=upload_id, PartNumber=1, Body=b"",
            )
            parts.append({"PartNumber": 1, "ETag": parts_resp["ETag"]})

        s3_client.complete_multipart_upload(
            Bucket=args.bucket,
            Key=args.key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    except Exception as exc:
        # 上传失败时中止分片上传，释放 S3 资源
        try:
            s3_client.abort_multipart_upload(
                Bucket=args.bucket, Key=args.key, UploadId=upload_id
            )
        except Exception:
            pass
        sys.stderr.write(f"\n错误: 归档失败: {exc}\n")
        sys.exit(1)

    sys.stderr.write(
        f"\r  拉取 {label} 完成: {count} 条; 已归档到 s3://{args.bucket}/{args.key}\n"
    )
    print(count)  # 供调用方读取总条数


if __name__ == "__main__":
    main()
