#!/usr/bin/env python3
"""Safely backfill selected DynamoDB contact-summary fields from S3 events."""
import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shlex
import sys
import tempfile
import time
from pathlib import Path

import boto3
import botocore.exceptions
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError, ClientError, ConfigNotFound, CredentialRetrievalError,
    InvalidConfigError, NoAuthTokenError, NoCredentialsError, PartialCredentialsError,
    ProfileNotFound, RefreshWithMFAUnsupportedError, SSOError, TokenRetrievalError,
)

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR / "lib" / "lambda"))
from contact_summary import BACKFILLABLE_FIELDS, build_contact_summary  # noqa: E402

# Config discovery and state files are anchored to this script's directory so
# behavior does not depend on the current working directory.
CONFIG_DIR = SCRIPT_DIR
DEFAULT_STATE_DIR = SCRIPT_DIR / "backfill-state"
EXAMPLE_CONFIG = "config-example.json"
# setup_cloudfront.py deploys all shared infrastructure in us-east-1 (L302).
DEFAULT_REGION = "us-east-1"
INSTANCE_ARN_RE = re.compile(r"^arn:aws[\w-]*:connect:[a-z0-9-]+:\d{12}:instance/[^/:]+$")

CHECKPOINT_VERSION = 3
TRANSIENT_CODES = {
    "InternalError", "InternalServerError", "ProvisionedThroughputExceededException",
    "RequestLimitExceeded", "ServiceUnavailable", "SlowDown", "Throttling",
    "ThrottlingException", "RequestTimeout",
}
CONTACT_KEY_RE = re.compile(r"^events/([^/]+)/([^/]+)\.json\.gz$")
RANGE_OPTIONS = (
    ("contact_id", "--contact-id"), ("limit", "--limit"), ("max_pages", "--max-pages"),
    ("batch_number", "--batch-number"), ("start_after_key", "--start-after-key"),
    ("end_key", "--end-key"),
)
# Any of these (all given as None/False) keeps the run non-interactive.
# dry_run_flag is the raw --dry-run value saved before parse_args overwrites it.
INTERACTIVE_BLOCKERS = (
    "instance_name", "source_prefix", "contact_id", "all", "limit", "max_pages",
    "batch_number", "start_after_key", "end_key", "resume", "apply", "dry_run_flag",
)

# Credential problems abort the whole run (exit 2) instead of being recorded as
# a per-instance error or a per-Contact failure. Network errors such as
# EndpointConnectionError are deliberately not listed.
CREDENTIAL_ERRORS = (
    NoCredentialsError, PartialCredentialsError, CredentialRetrievalError,
    TokenRetrievalError, SSOError, ProfileNotFound, InvalidConfigError,
    RefreshWithMFAUnsupportedError, NoAuthTokenError, ConfigNotFound,
)
# process_key() reports failures as type(error).__name__; these are the names
# (including subclasses such as SSOTokenLoadError) that mean "no credentials".
CREDENTIAL_ERROR_NAMES = frozenset(
    name for name, value in vars(botocore.exceptions).items()
    if isinstance(value, type) and issubclass(value, CREDENTIAL_ERRORS)
)


class CredentialsUnavailable(BotoCoreError):
    """Raised when process_key() classified a Contact failure as a credential error."""
    fmt = "{category}"


class InteractiveCancelled(Exception):
    """The interactive menu was cancelled before anything was written."""


def _is_credential_error(error):
    return isinstance(error, CREDENTIAL_ERRORS + (CredentialsUnavailable,))


# The helpers below mirror setup_cloudfront.py instead of importing it (importing
# would compile the whole deploy script and couple this CLI to its internals):
# arn_region/arn_account L103-108, instance id L787, resource names L302-323.
def _arn_region(arn):
    return arn.split(":")[3]


def _arn_account(arn):
    return arn.split(":")[4]


def _instance_id(arn):
    return arn.split("/")[-1]


def derive_defaults(account_id):
    """Resource names exactly as setup_cloudfront.py derives them (L302-323)."""
    return {
        "profile": account_id,  # setup_cloudfront sets _AWS_PROFILE = account_id
        "region": DEFAULT_REGION,
        "bucket": f"connect-ai-logs-{account_id}",
        "table": f"connect-ai-contacts-{account_id}",
    }


def discover_configs(directory):
    """Return config-*.json files in ``directory``, excluding the example."""
    return sorted(
        path for path in Path(directory).glob("config-*.json")
        if path.name != EXAMPLE_CONFIG and path.is_file()
    )


def load_config(path):
    """Load instances in config order; errors name only the redacted file and key."""
    # Real config names embed the account id (config-<account_id>.json).
    name = _redacted_name(Path(path).name)
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as error:
        raise ValueError(f"无法读取配置文件 {name}") from error
    except ValueError as error:
        raise ValueError(f"配置文件 {name} 不是有效 JSON") from error
    connect = data.get("connect") if isinstance(data, dict) else None
    if not isinstance(connect, dict) or not connect:
        raise ValueError(f"配置文件 {name} 缺少非空 connect 字典")
    instances = []
    for key, entry in connect.items():
        arn = entry.get("instance_arn") if isinstance(entry, dict) else None
        if not isinstance(arn, str) or not INSTANCE_ARN_RE.fullmatch(arn):
            raise ValueError(f"配置文件 {name} 中实例 {key} 的 instance_arn 无效")
        instances.append({"key": key, "instance_arn": arn})
    # Account id comes from the first instance ARN, like setup_cloudfront.py L303-304.
    return instances, _arn_account(instances[0]["instance_arn"])


def _state_label(instance_key, alias):
    """File-name-safe per-instance label: config key, else alias digest."""
    if instance_key is None:
        return f"alias-{_digest(alias)}"
    label = re.sub(r"[^A-Za-z0-9._-]", "_", instance_key).lstrip(".")
    return label or f"key-{_digest(instance_key)}"


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "从 S3 events/*.json.gz 重算并回填 DynamoDB Contact 摘要字段（默认 dry-run）。"
            "默认从脚本目录的 config-*.json 推导 profile/region/bucket/table，并处理全部实例。"
        )
    )
    parser.add_argument(
        "--config",
        help="setup_cloudfront.py 使用的 config 文件；默认自动发现脚本目录下唯一的 "
             "config-*.json（排除 config-example.json）；相对路径相对脚本目录",
    )
    parser.add_argument("--profile", help="AWS CLI profile；默认 = config 中的账号 ID")
    parser.add_argument("--region", help="S3/DynamoDB 区域；默认 us-east-1")
    parser.add_argument("--source-bucket", help="默认 connect-ai-logs-<account_id>")
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--instance-name",
        help="一个或多个（逗号分隔）config key 或 InstanceAlias（先匹配 key）；"
             "默认处理 config 中全部实例",
    )
    source.add_argument(
        "--source-prefix",
        help="规范前缀 events/<InstanceAlias>/；只处理该实例且不调用 DescribeInstance",
    )
    parser.add_argument("--target-table", help="默认 connect-ai-contacts-<account_id>")
    parser.add_argument("--fields", default="escalated,escalationReason")
    parser.add_argument("--contact-id", help="只处理一个 Contact（直接 GetObject）；需限定单个实例")
    parser.add_argument("--limit", type=int, help="每个实例最多处理的 Contact 数")
    parser.add_argument("--page-size", type=int, default=100, help="S3 每页对象数 (1-1000)")
    parser.add_argument("--max-pages", type=int, help="每个实例最多列举页数")
    parser.add_argument("--start-after-key", help="只处理字典序晚于此 S3 key 的对象；需限定单个实例")
    parser.add_argument("--end-key", help="处理到此 S3 key（含）；需限定单个实例")
    parser.add_argument("--batch-number", type=int, help="从 1 开始的批次号（每个实例）；需配合 --batch-size")
    parser.add_argument("--batch-size", type=int, default=25, help="批次大小/ checkpoint 写入间隔")
    parser.add_argument("--rate-limit", type=float, default=5.0, help="每秒最多处理的 Contact 数")
    parser.add_argument(
        "--all", action="store_true",
        help="处理所选实例的全部 Contact；--apply 无范围参数时必须显式指定",
    )
    parser.add_argument(
        "--state-dir",
        help="checkpoint/失败报告目录；默认 <脚本目录>/backfill-state/，每个实例独立文件",
    )
    parser.add_argument("--failures-file", help="显式失败报告路径；仅限单个实例")
    parser.add_argument("--checkpoint", help="显式 checkpoint 路径；仅限单个实例")
    parser.add_argument("--resume", action="store_true", help="按每个实例的 checkpoint 恢复")
    parser.add_argument("--retry-failures", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="只比较，不写入（默认）")
    mode.add_argument("--apply", action="store_true", help="显式确认执行字段级 UpdateItem")
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="禁用交互菜单（TTY 中零参数也直接 dry-run 全部实例）",
    )
    return parser


def _validate_selector(args, parser):
    """Validate --instance-name/--source-prefix syntax; return alias from prefix.

    ``--instance-name`` accepts comma-separated names; the de-duplicated list
    (first occurrence wins) is stored in ``args.instance_names``.
    """
    args.instance_names = []
    if args.instance_name is not None:
        names = [name.strip() for name in args.instance_name.split(",")]
        if any(not name or "/" in name for name in names):
            parser.error("--instance-name 不能为空或包含斜杠")
        args.instance_names = list(dict.fromkeys(names))
    if args.source_prefix is not None:
        match = re.fullmatch(r"events/([^/]+)/", args.source_prefix)
        if not match:
            parser.error("--source-prefix 必须严格为 events/<instanceName>/")
        return match.group(1)
    return None


def _resolve_config(args, parser):
    """Return the config path to use, or None for fully explicit (no-config) mode."""
    explicit_ready = bool(
        args.source_bucket and args.target_table and (args.instance_name or args.source_prefix)
    )
    if args.config:
        path = Path(args.config).expanduser()
        if not path.is_absolute():
            path = CONFIG_DIR / path  # same as setup_cloudfront.py L292
        if not path.is_file():
            parser.error(f"找不到配置文件: {_redacted_name(path.name)}")
        return path
    candidates = discover_configs(CONFIG_DIR)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        parser.error(
            "脚本目录下有多个 config 文件，请用 --config 指定其一: "
            + ", ".join(path.name for path in candidates)
        )
    if explicit_ready:
        return None
    parser.error(
        "脚本目录下未找到 config-*.json（config-example.json 除外）；请用 --config 指定，"
        "或同时显式提供 --source-bucket、--target-table 和 --instance-name/--source-prefix"
    )


def _resolve_targets(args, parser, prefix_alias):
    """Resolve profile/region/bucket/table and the instance selection (no AWS calls)."""
    explicit = {
        "profile": args.profile, "region": args.region,
        "bucket": args.source_bucket, "table": args.target_table,
    }
    args.config_path = _resolve_config(args, parser)
    args.alias_match = None
    args.key_match = set()
    names = args.instance_names
    if args.config_path is None:
        # Fully explicit legacy mode: default credential chain, single instance.
        if len(names) > 1:
            parser.error("无 config 模式只能指定一个 --instance-name")
        derived = {"profile": None, "region": DEFAULT_REGION, "bucket": None, "table": None}
        derived_source = "default"
        alias = prefix_alias or names[0]
        args.selection = [{"key": None, "instance_arn": None, "alias": alias, "needs_lookup": False}]
        args.config_instances = []
    else:
        try:
            instances, account_id = load_config(args.config_path)
        except ValueError as error:
            parser.error(str(error))
        derived = derive_defaults(account_id)
        derived_source = "config"
        args.config_instances = instances
        by_key = {item["key"]: item for item in instances}
        if prefix_alias is not None:
            args.selection = [{
                "key": None, "instance_arn": None, "alias": prefix_alias, "needs_lookup": False,
            }]
        elif names and all(name in by_key for name in names):
            # Only config keys: describe just those instances, in config order.
            args.selection = [
                dict(item, alias=None, needs_lookup=True)
                for item in instances if item["key"] in names
            ]
        else:
            if names:
                # Some names are not config keys: verify each is the InstanceAlias
                # of exactly one config instance during preflight so a typo
                # cannot scan an empty prefix.
                args.alias_match = [name for name in names if name not in by_key]
                args.key_match = {name for name in names if name in by_key}
            args.selection = [dict(item, alias=None, needs_lookup=True) for item in instances]
    args.sources = {}
    for name, value in explicit.items():
        if value is not None:
            args.sources[name] = "explicit"
        else:
            args.sources[name] = "default" if name == "region" and derived_source == "default" else derived_source
    args.profile = explicit["profile"] if explicit["profile"] is not None else derived["profile"]
    args.region = explicit["region"] or derived["region"]
    args.source_bucket = explicit["bucket"] or derived["bucket"]
    args.target_table = explicit["table"] or derived["table"]
    args.single_instance = bool(
        len(names) == 1 or prefix_alias is not None
        or args.config_path is None or len(args.config_instances) == 1
    )


def parse_args(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    prefix_alias = _validate_selector(args, parser)
    fields = [field.strip() for field in args.fields.split(",") if field.strip()]
    if not fields or len(fields) != len(set(fields)):
        parser.error("--fields 不能为空或包含重复字段")
    invalid = sorted(set(fields) - set(BACKFILLABLE_FIELDS))
    if invalid:
        parser.error("不支持的字段: " + ", ".join(invalid))
    args.fields = fields
    for name in ("limit", "max_pages", "batch_number", "batch_size"):
        value = getattr(args, name)
        if value is not None and value <= 0:
            parser.error(f"--{name.replace('_', '-')} 必须大于 0")
    if not 1 <= args.page_size <= 1000:
        parser.error("--page-size 必须在 1 到 1000 之间")
    if args.rate_limit <= 0:
        parser.error("--rate-limit 必须大于 0")
    if args.batch_number and "--batch-size" not in (argv or sys.argv[1:]):
        parser.error("--batch-number 必须显式配合 --batch-size")
    if args.contact_id and ("/" in args.contact_id or args.contact_id.lower() == "index"):
        parser.error("--contact-id 不能包含斜杠或使用保留名称 index")
    if args.contact_id and any((args.start_after_key, args.end_key, args.batch_number, args.max_pages)):
        parser.error("--contact-id 不能与页面、批次或 key 范围参数组合")
    if args.start_after_key and args.end_key and args.start_after_key >= args.end_key:
        parser.error("--start-after-key 必须早于 --end-key")
    if args.retry_failures and not args.resume:
        parser.error("--retry-failures 只能与 --resume 一起使用")
    if args.all:
        conflicts = [flag for name, flag in RANGE_OPTIONS if getattr(args, name) is not None]
        if conflicts:
            parser.error("--all 表示全部 Contact，不能与范围参数组合: " + ", ".join(conflicts))
    finite_scope = any((
        args.contact_id, args.limit, args.max_pages, args.batch_number,
        args.end_key, args.resume,
    ))
    if args.apply and not (finite_scope or args.all):
        parser.error(
            "--apply 必须指定有限范围（contact/limit/max-pages/batch/end-key/resume），"
            "或显式使用 --all 确认写入所选实例的全部 Contact"
        )
    _resolve_targets(args, parser, prefix_alias)
    if not args.single_instance:
        single_only = [
            flag for name, flag in (
                ("contact_id", "--contact-id"), ("start_after_key", "--start-after-key"),
                ("end_key", "--end-key"), ("checkpoint", "--checkpoint"),
                ("failures_file", "--failures-file"),
            ) if getattr(args, name) is not None
        ]
        if single_only:
            parser.error(
                ", ".join(single_only) + " 只能用于单个实例；多实例时请用 "
                "--instance-name/--source-prefix 限定单个实例（状态文件请用 --state-dir）"
            )
    known_alias = args.selection[0]["alias"] if len(args.selection) == 1 else None
    if known_alias is not None:
        try:
            _validate_key_range(args, f"events/{known_alias}/")
        except ValueError as error:
            parser.error(str(error))
    args.state_dir = (
        Path(args.state_dir).expanduser().resolve() if args.state_dir else DEFAULT_STATE_DIR
    )
    # Keep the raw --dry-run flag: it disables the interactive menu.
    args.dry_run_flag = args.dry_run
    args.dry_run = not args.apply
    return args


def _validate_key_range(args, prefix):
    if args.start_after_key and not args.start_after_key.startswith(prefix):
        raise ValueError("--start-after-key 必须位于所选实例的 source prefix 下")
    if args.end_key and not args.end_key.startswith(prefix):
        raise ValueError("--end-key 必须位于所选实例的 source prefix 下")


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def _redacted_name(value):
    return f"…{_digest(value)}"


def _redacted_value(value):
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, str):
        return {"empty": not bool(value), "length": len(value), "digest": _digest(value)}
    if isinstance(value, (int, float)):
        return {"type": type(value).__name__}
    return {"type": type(value).__name__, "length": len(value) if hasattr(value, "__len__") else None}


def _fingerprint(args):
    data = {
        "profile": args.profile, "region": args.region,
        "bucket": args.source_bucket, "prefix": args.source_prefix,
        "table": args.target_table, "fields": args.fields, "contact": args.contact_id,
        "limit": args.limit, "pageSize": args.page_size, "maxPages": args.max_pages,
        "start": args.start_after_key, "end": args.end_key,
        "batchNumber": args.batch_number, "batchSize": args.batch_size,
        "apply": args.apply, "all": args.all,
    }
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()


def _atomic_json(path, data):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=destination.name + ".", dir=str(destination.parent))
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _load_checkpoint(args):
    """Load and validate a checkpoint; a missing file under --resume sets
    ``args.checkpoint_missing`` and returns None (caller decides what to do)."""
    args.checkpoint_missing = False
    if not args.resume:
        return None
    if not Path(args.checkpoint).exists():
        args.checkpoint_missing = True
        return None
    label = getattr(args, "label", None)
    where = f"[{label}] " if label else ""
    try:
        with open(args.checkpoint, encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, ValueError) as error:
        raise ValueError(f"{where}无法读取 checkpoint: {type(error).__name__}") from error
    if not isinstance(state, dict) or state.get("version") != CHECKPOINT_VERSION:
        raise ValueError(f"{where}checkpoint 版本不兼容")
    if state.get("fingerprint") != _fingerprint(args):
        raise ValueError(f"{where}checkpoint 与当前参数不匹配")
    return state


def _error_code(error):
    if isinstance(error, ClientError):
        return error.response.get("Error", {}).get("Code", "ClientError")
    if isinstance(error, (gzip.BadGzipFile, EOFError)):
        return "InvalidGzip"
    if isinstance(error, (json.JSONDecodeError, UnicodeDecodeError)):
        return "InvalidJson"
    if isinstance(error, ValueError):
        return "InvalidEvents"
    return type(error).__name__


def _is_transient(error):
    if not isinstance(error, ClientError):
        return False
    code = _error_code(error)
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return code in TRANSIENT_CODES or status >= 500


def _with_retry(operation, attempts=4, sleep=time.sleep):
    for attempt in range(attempts):
        try:
            return operation()
        except ClientError as error:
            if not _is_transient(error) or attempt + 1 == attempts:
                raise
            sleep(min(2 ** attempt, 8))


def _contact_from_key(prefix, key):
    match = CONTACT_KEY_RE.fullmatch(key)
    if not match or key == prefix + "index.json.gz":
        return None
    expected_instance = prefix[len("events/"):-1]
    if match.group(1) != expected_instance or match.group(2).lower() == "index":
        return None
    return match.group(2)


def _listing_cursor(page, continuation_token=None, start_after=None):
    cursor = {"pageNumber": page}
    if continuation_token:
        cursor["continuationToken"] = continuation_token
    elif start_after:
        cursor["startAfter"] = start_after
    return cursor


def _commit_candidate(progress, key):
    """Commit a yielded candidate only after its processing has completed."""
    pending = progress.get("pendingCandidate")
    if not pending or pending.get("key") != key:
        raise ValueError("checkpoint pending candidate does not match completed key")
    progress.update({
        "pageNumber": pending["pageNumber"],
        "continuationToken": pending.get("continuationToken"),
        "startAfter": pending.get("startAfter"),
        "entryIndex": pending["entryIndex"],
        "candidatePosition": pending["candidatePosition"],
        "lastExaminedKey": key,
        "exhausted": bool(pending.get("singleContact")),
    })
    progress.pop("pendingCandidate", None)


def iter_candidate_keys(s3, args, resume_after=None, batch_processed=0, progress=None,
                        listing_succeeded=None):
    """Yield keys while retaining a pending boundary until each key is committed."""
    progress = progress if progress is not None else {}
    pending = progress.get("pendingCandidate")
    if pending:
        key = pending["key"]
        yield key
        if progress.get("pendingCandidate"):
            _commit_candidate(progress, key)
        if pending.get("singleContact"):
            return

    if args.contact_id:
        key = args.source_prefix + args.contact_id + ".json.gz"
        if progress.get("exhausted") or key == resume_after:
            return
        progress.update({
            "pendingCandidate": {
                "key": key,
                "pageNumber": 1,
                "entryIndex": 0,
                "candidatePosition": 1,
                "singleContact": True,
            },
            "exhausted": False,
        })
        yield key
        if progress.get("pendingCandidate"):
            _commit_candidate(progress, key)
        return

    if progress:
        page = progress.get("pageNumber", 1)
        continuation_token = progress.get("continuationToken")
        entry_index = progress.get("entryIndex", -1)
        candidate_number = progress.get("candidatePosition", 0)
        if progress.get("exhausted"):
            return
        initial_start_after = progress.get("startAfter") or args.start_after_key
    else:
        page = 1
        continuation_token = None
        entry_index = -1
        initial_start_after = resume_after or args.start_after_key
        candidate_number = (
            (args.batch_number - 1) * args.batch_size + batch_processed
            if resume_after and args.batch_number else 0
        )

    while not args.max_pages or page <= args.max_pages:
        request = {
            "Bucket": args.source_bucket,
            "Prefix": args.source_prefix,
            "MaxKeys": args.page_size,
        }
        if continuation_token:
            request["ContinuationToken"] = continuation_token
        elif page == 1 and initial_start_after:
            request["StartAfter"] = initial_start_after
        listing_cursor = _listing_cursor(
            page, continuation_token, request.get("StartAfter"),
        )
        progress.update({
            "pageNumber": page,
            "continuationToken": continuation_token,
            "startAfter": request.get("StartAfter"),
            "entryIndex": entry_index,
            "candidatePosition": candidate_number,
            "pendingListing": listing_cursor,
            "exhausted": False,
        })
        response = _with_retry(lambda: s3.list_objects_v2(**request))
        progress.pop("pendingListing", None)
        if listing_succeeded:
            listing_succeeded(listing_cursor)
        contents = response.get("Contents", [])
        for index, entry in enumerate(contents):
            if index <= entry_index:
                continue
            key = entry.get("Key", "")
            if args.end_key and key > args.end_key:
                progress["exhausted"] = True
                return
            contact_id = _contact_from_key(args.source_prefix, key)
            if not contact_id:
                progress.update({
                    "entryIndex": index,
                    "lastExaminedKey": key,
                })
                continue
            next_candidate = candidate_number + 1
            if args.batch_number:
                first = (args.batch_number - 1) * args.batch_size + 1
                last = first + args.batch_size - 1
                if next_candidate < first:
                    candidate_number = next_candidate
                    progress.update({
                        "entryIndex": index,
                        "candidatePosition": candidate_number,
                        "lastExaminedKey": key,
                    })
                    continue
                if next_candidate > last:
                    progress["exhausted"] = True
                    return
            progress["pendingCandidate"] = {
                "key": key,
                "pageNumber": page,
                "continuationToken": continuation_token,
                "startAfter": request.get("StartAfter"),
                "entryIndex": index,
                "candidatePosition": next_candidate,
            }
            yield key
            if progress.get("pendingCandidate"):
                _commit_candidate(progress, key)
            candidate_number = progress["candidatePosition"]

        if not response.get("IsTruncated") or (args.max_pages and page >= args.max_pages):
            progress["exhausted"] = True
            return
        page += 1
        continuation_token = response["NextContinuationToken"]
        initial_start_after = None
        entry_index = -1
        progress.update({
            "pageNumber": page,
            "continuationToken": continuation_token,
            "startAfter": None,
            "entryIndex": entry_index,
            "candidatePosition": candidate_number,
            "exhausted": False,
        })


def read_events(s3, bucket, key):
    response = _with_retry(lambda: s3.get_object(Bucket=bucket, Key=key))
    body = response["Body"]
    try:
        with gzip.GzipFile(fileobj=body, mode="rb") as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8") as text:
                events = json.load(text)
    finally:
        body.close()
    if not isinstance(events, list) or not events:
        raise ValueError("event document must be a non-empty JSON array")
    clean = []
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("event must be an object")
        timestamp = event.get("timestamp")
        message = event.get("message")
        source = event.get("source", "connect")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ValueError("event timestamp must be an integer")
        if not isinstance(message, str) or not isinstance(source, str):
            raise ValueError("event message/source has an invalid type")
        clean.append({"timestamp": timestamp, "message": message, "source": source})
    return clean


def _update_request(args, contact_id, calculated):
    names = {"#pk": "instanceName", "#sk": "contactId"}
    values = {}
    sets = []
    removes = []
    for index, field in enumerate(args.fields):
        name = f"#f{index}"
        names[name] = field
        value = calculated.get(field)
        if value is None:
            removes.append(name)
        else:
            token = f":v{index}"
            values[token] = value
            sets.append(f"{name} = {token}")
    clauses = []
    if sets:
        clauses.append("SET " + ", ".join(sets))
    if removes:
        clauses.append("REMOVE " + ", ".join(removes))
    request = {
        "Key": {"instanceName": args.instance_name, "contactId": contact_id},
        "UpdateExpression": " ".join(clauses),
        "ExpressionAttributeNames": names,
        "ConditionExpression": "attribute_exists(#pk) AND attribute_exists(#sk)",
    }
    if values:
        request["ExpressionAttributeValues"] = values
    return request


def process_key(s3, table, args, key):
    contact_id = _contact_from_key(args.source_prefix, key)
    if not contact_id:
        return "failed", "InvalidKey", None
    try:
        calculated = build_contact_summary(read_events(s3, args.source_bucket, key))
        response = _with_retry(lambda: table.get_item(
            Key={"instanceName": args.instance_name, "contactId": contact_id},
            ConsistentRead=True,
        ))
        current = response.get("Item")
        if current is None:
            return "missing", None, None
        changed = {}
        for field in args.fields:
            old_value = current.get(field)
            new_value = calculated.get(field)
            if ((new_value is None and field in current) or
                    (new_value is not None and old_value != new_value)):
                changed[field] = (old_value, new_value)
        if not changed:
            return "unchanged", None, None
        diff = {field: {"old": _redacted_value(old), "new": _redacted_value(new)}
                for field, (old, new) in changed.items()}
        if args.dry_run:
            return "changed", None, diff
        request = _update_request(args, contact_id, calculated)
        try:
            _with_retry(lambda: table.update_item(**request))
        except ClientError as error:
            if _error_code(error) == "ConditionalCheckFailedException":
                return "missing", None, None
            raise
        return "changed", None, diff
    except Exception as error:  # noqa: BLE001
        return "failed", _error_code(error), None


def _checkpoint_state(args, counts, last_key, failures, processed_candidates=0,
                      cursor=None):
    return {
        "version": CHECKPOINT_VERSION,
        "fingerprint": _fingerprint(args),
        "lastCompletedKey": last_key,
        "processedCandidates": processed_candidates,
        "cursor": cursor or {},
        "counts": counts,
        "failures": failures,
        "updatedAtEpoch": int(time.time()),
    }


def _print_startup(args, contexts, states):
    scope = "single-contact" if args.contact_id else (
        f"batch-{args.batch_number}" if args.batch_number else
        f"limit-{args.limit}" if args.limit else
        f"pages-{args.max_pages}" if args.max_pages else
        "bounded-key-range" if args.end_key else
        "all (--all)" if args.all else
        "resume" if args.resume else "all-matching (dry-run)"
    )
    estimate = (
        "1 contact" if args.contact_id else
        f"at most {args.limit} contacts" if args.limit else
        f"at most {args.batch_size} contacts" if args.batch_number else
        f"at most {args.max_pages * args.page_size} listed objects" if args.max_pages else
        "bounded by end key" if args.end_key else "unknown until listing"
    )
    sources = args.sources
    print("配置摘要:")
    profile = _redacted_name(args.profile) if args.profile else "default-chain"
    # config file names embed the account id, so redact like the profile.
    config_name = (
        f"{_redacted_name(args.config_path.name)} ({'explicit' if args.config else 'auto'})"
        if args.config_path else "none"
    )
    print(f"  mode={'DRY-RUN' if args.dry_run else 'APPLY'} scope={scope} config={config_name}")
    print(f"  estimated-range={estimate} (per instance)")
    print(f"  profile={profile} ({sources['profile']}) region={args.region} ({sources['region']})")
    print(f"  bucket={_redacted_name(args.source_bucket)} ({sources['bucket']}) "
          f"table={_redacted_name(args.target_table)} ({sources['table']})")
    selected = "selected" if (args.instance_name or args.source_prefix) else "all"
    print(f"  fields={','.join(args.fields)} rate={args.rate_limit}/s "
          f"instances={len(contexts)} ({selected}) state-dir={args.state_dir}")
    for ctx, state in zip(contexts, states):
        print(f"  [{ctx.label}] prefix={_redacted_name(ctx.source_prefix)} "
              f"alias-source={ctx.alias_source}")
        if state and state.get("failures"):
            contact_failures = sum(
                item.get("operation", "contact") == "contact"
                for item in state["failures"]
            )
            listing_failures = sum(
                item.get("operation") == "listing" for item in state["failures"]
            )
            if contact_failures and not args.retry_failures:
                print(f"    注意: checkpoint 中 {contact_failures} 个 Contact 失败不会静默丢失；使用 --retry-failures 重试")
            if listing_failures:
                print(f"    注意: checkpoint 中 {listing_failures} 个 listing 失败将在恢复对应分页时自动重试")


def _session_kwargs(region, profile):
    kwargs = {"region_name": region}
    if profile:
        kwargs["profile_name"] = profile
    return kwargs


def _aws_config():
    return Config(retries={"max_attempts": 8, "mode": "adaptive"})


def _print_profile_diagnostics(error):
    """Explain a ProfileNotFound: which profile was tried and which files boto3 read.

    Profile names and file paths are local configuration, not secrets; no
    credential values are read or printed.
    """
    tried = (getattr(error, "kwargs", None) or {}).get("profile") or "<unknown>"
    print(f"  尝试的 profile: {tried}（默认 = config 中的账号 ID，与 setup_cloudfront.py 相同）",
          file=sys.stderr)
    for var, default in (("AWS_CONFIG_FILE", "~/.aws/config"),
                         ("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")):
        value = os.environ.get(var)
        shown = f"{value}  ← 来自环境变量 {var}" if value else f"{default}（默认）"
        print(f"  {var}: {shown}", file=sys.stderr)
    try:
        available = boto3.Session().available_profiles
    except Exception:  # noqa: BLE001
        available = []
    print(f"  当前可见的 profile: {', '.join(available) if available else '（无）'}",
          file=sys.stderr)
    if any(os.environ.get(var) for var in ("AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE")):
        print("  提示: 环境变量让 boto3 读取了其他配置文件；执行 "
              "`unset AWS_CONFIG_FILE AWS_SHARED_CREDENTIALS_FILE` 后重试，"
              "或用 --profile 指定可见的 profile。", file=sys.stderr)
    else:
        print("  提示: 在 ~/.aws/config 中添加该 profile，或用 --profile 指定可见的 profile。",
              file=sys.stderr)


def preflight_aliases(args, session_factory):
    """Resolve InstanceAlias for every selected instance before any data access.

    Mirrors setup_cloudfront.py L784-806: DescribeInstance in the instance ARN's
    region; any error or empty alias aborts the whole run.
    """
    for entry in args.selection:
        if not entry["needs_lookup"]:
            continue
        arn = entry["instance_arn"]
        try:
            connect = session_factory(
                **_session_kwargs(_arn_region(arn), args.profile)
            ).client("connect", config=_aws_config())
            response = connect.describe_instance(InstanceId=_instance_id(arn))
            alias = (response.get("Instance") or {}).get("InstanceAlias") or ""
        except Exception as error:  # noqa: BLE001
            # Profile/credential problems are not a DescribeInstance permission
            # issue; let main() report them as credential errors (exit 2).
            if _is_credential_error(error):
                raise
            raise ValueError(
                f"[{entry['key']}] 无法查询 InstanceAlias（需要 connect:DescribeInstance）: "
                f"{_error_code(error)}"
            ) from error
        if not alias or "/" in alias:
            raise ValueError(f"[{entry['key']}] DescribeInstance 未返回可用的 InstanceAlias")
        entry["alias"] = alias
    if args.alias_match is not None:
        selected_keys = set(args.key_match)
        for name in args.alias_match:
            matched = [entry for entry in args.selection if entry["alias"] == name]
            if len(matched) != 1:
                reason = "未匹配任何" if not matched else "匹配到多个"
                # Keys print in clear; aliases (and the rejected name) stay redacted.
                raise ValueError(
                    f"--instance-name {_redacted_name(name)} {reason} config key 或 "
                    "InstanceAlias；可用 config key (InstanceAlias 脱敏摘要): "
                    + ", ".join(f"{entry['key']} ({_redacted_name(entry['alias'])})"
                                for entry in args.selection)
                    + "；如需跳过 DescribeInstance 请用 --source-prefix events/<InstanceAlias>/"
                )
            selected_keys.add(matched[0]["key"])
        # Union of key and alias matches, in config order, one entry per instance.
        args.selection = [entry for entry in args.selection if entry["key"] in selected_keys]


def build_contexts(args):
    contexts = []
    labels = set()
    for entry in args.selection:
        alias = entry["alias"]
        label = _state_label(entry["key"], alias)
        if label in labels:
            raise ValueError(f"实例状态文件名冲突: {label}")
        labels.add(label)
        ctx = argparse.Namespace(**vars(args))
        ctx.instance_key = entry["key"]
        ctx.instance_name = alias
        ctx.source_prefix = f"events/{alias}/"
        ctx.label = label
        ctx.alias_source = "describe-instance" if entry["needs_lookup"] else "explicit"
        ctx.checkpoint = args.checkpoint or str(
            args.state_dir / f"backfill-checkpoint-{label}.json")
        ctx.failures_file = args.failures_file or str(
            args.state_dir / f"backfill-failures-{label}.json")
        _validate_key_range(ctx, ctx.source_prefix)
        contexts.append(ctx)
    return contexts


def _state_completed(ctx, state):
    cursor = state.get("cursor") or {}
    if cursor.get("pendingCandidate") or cursor.get("pendingListing"):
        return False
    processed = state.get("processedCandidates", 0)
    return bool(cursor.get("exhausted")) or bool(ctx.limit and processed >= ctx.limit)


def _has_contact_failures(state):
    return any(
        item.get("operation", "contact") == "contact" for item in state.get("failures") or []
    )


def _empty_counts():
    return {"unchanged": 0, "changed": 0, "missing": 0, "failed": 0}


def _data_clients(args, session_factory):
    """S3 client and DynamoDB Table for the data region/profile (no API calls)."""
    session = session_factory(**_session_kwargs(args.region, args.profile))
    s3 = session.client("s3", config=_aws_config())
    table = session.resource("dynamodb", config=_aws_config()).Table(args.target_table)
    return s3, table


# ---------------------------------------------------------------------------
# Interactive mode: read-only overview + menu. It only builds extra argv; the
# selection is re-parsed by parse_args() and executed by run(), so every write
# goes through exactly the same checks as a hand-typed command.
# ---------------------------------------------------------------------------

def _should_interact(args, tty):
    return bool(tty) and not args.non_interactive and all(
        not getattr(args, name) for name in INTERACTIVE_BLOCKERS
    )


def _overview_error(key, operation, error, permission):
    return ValueError(
        f"数据概览失败 [{key}] {operation}: {_error_code(error)}（需要 {permission}）"
    )


def _count_ddb(table, alias, sleep=time.sleep):
    """Exact item count for one instanceName (Query Select=COUNT, all pages)."""
    total = 0
    last_key = None
    while True:
        request = {
            "KeyConditionExpression": "#pk = :n",
            "ExpressionAttributeNames": {"#pk": "instanceName"},
            "ExpressionAttributeValues": {":n": alias},
            "Select": "COUNT",
        }
        if last_key:
            request["ExclusiveStartKey"] = last_key
        response = _with_retry(lambda: table.query(**request), sleep=sleep)
        total += response.get("Count", 0)
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return total


def _count_s3(s3, bucket, alias, sleep=time.sleep):
    """Count keys under events/<alias>/ that the backfill would treat as Contacts."""
    prefix = f"events/{alias}/"
    total = 0
    token = None
    while True:
        request = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            request["ContinuationToken"] = token
        response = _with_retry(lambda: s3.list_objects_v2(**request), sleep=sleep)
        total += sum(
            1 for entry in response.get("Contents", [])
            if _contact_from_key(prefix, entry.get("Key", ""))
        )
        if not response.get("IsTruncated"):
            return total
        token = response["NextContinuationToken"]


def collect_overview(args, session_factory, sleep=time.sleep):
    """Preflight aliases, then count DynamoDB items and S3 Contacts per instance."""
    preflight_aliases(args, session_factory)
    s3, table = _data_clients(args, session_factory)
    rows = []
    for index, entry in enumerate(args.selection, 1):
        alias = entry["alias"]
        try:
            ddb = _count_ddb(table, alias, sleep)
        except ClientError as error:
            raise _overview_error(entry["key"], "DynamoDB Query", error,
                                  "dynamodb:Query") from error
        try:
            s3_count = _count_s3(s3, args.source_bucket, alias, sleep)
        except ClientError as error:
            raise _overview_error(entry["key"], "S3 ListObjectsV2", error,
                                  "s3:ListBucket") from error
        rows.append({"index": index, "key": entry["key"], "alias": alias,
                     "ddb": ddb, "s3": s3_count})
    return rows, table


def _print_overview(args, rows):
    print(f"数据概览（只读统计）: table={_redacted_name(args.target_table)} "
          f"bucket={_redacted_name(args.source_bucket)}")
    for row in rows:
        print(f"  {row['index']}) [{row['key']}] alias={_redacted_name(row['alias'])} "
              f"DynamoDB={row['ddb']} S3={row['s3']}")
    ddb_total = sum(row["ddb"] for row in rows)
    s3_total = sum(row["s3"] for row in rows)
    print(f"  合计 DynamoDB={ddb_total} S3={s3_total}")
    for row in rows:
        # Count differences only give a lower bound; no per-Contact comparison.
        if row["s3"] > row["ddb"]:
            print(f"  提示 [{row['key']}]: S3 中至少有 {row['s3'] - row['ddb']} 个 Contact "
                  "在 DynamoDB 不存在，回填会报告为 missing")
        elif row["ddb"] > row["s3"]:
            print(f"  提示 [{row['key']}]: DynamoDB 中至少有 {row['ddb'] - row['s3']} 个 item "
                  "在 S3 没有 events 文件，回填不会处理它们")


def _prompt(input_fn, prompt, parse):
    """Ask until ``parse`` returns a value; EOF/Ctrl-C propagate to the caller."""
    while True:
        value = parse(input_fn(prompt).strip())
        if value is not None:
            return value
        print("无效输入，请重新输入")


def _choice(options, default=None):
    def parse(raw):
        if not raw:
            return default
        return raw if raw in options else None
    return parse


def _prompt_scope(input_fn):
    print("请选择更新范围:")
    print("  1) 所有实例的所有 Contact")
    print("  2) 指定实例（可多选，如 1,2）")
    print("  3) 指定一个 Contact")
    return _prompt(input_fn, "请输入选项 (1-3): ", _choice(("1", "2", "3")))


def _prompt_mode(input_fn):
    print("请选择执行方式:")
    print("  1) dry-run 预览（不写入）  [默认]")
    print("  2) 写入 DynamoDB")
    return _prompt(input_fn, "请输入选项 [1]: ", _choice(("1", "2"), default="1"))


def _prompt_instances(input_fn, rows):
    def parse(raw):
        parts = [part.strip() for part in raw.split(",")]
        if not raw or not all(part.isdigit() and 1 <= int(part) <= len(rows) for part in parts):
            return None
        picked = {int(part) for part in parts}
        return [row for row in rows if row["index"] in picked]  # config order
    return _prompt(input_fn, "请输入实例编号（可多选，如 1,2）: ", parse)


def _prompt_contact_id(input_fn):
    def parse(raw):
        # Same rules as --contact-id in parse_args().
        if not raw or "/" in raw or raw.lower() == "index":
            return None
        return raw
    return _prompt(input_fn, "请输入 Contact ID: ", parse)


def find_contact_instances(table, rows, contact_id, sleep=time.sleep):
    """Return the overview rows whose instance has this Contact in DynamoDB."""
    found = []
    for row in rows:
        try:
            response = _with_retry(lambda: table.get_item(
                Key={"instanceName": row["alias"], "contactId": contact_id},
                ProjectionExpression="#pk",
                ExpressionAttributeNames={"#pk": "instanceName"},
            ), sleep=sleep)
        except ClientError as error:
            raise ValueError(
                f"Contact 查找失败 [{row['key']}] DynamoDB GetItem: {_error_code(error)}"
                "（需要 dynamodb:GetItem）"
            ) from error
        if response.get("Item") is not None:
            found.append(row)
    return found


def _pick_contact_instance(input_fn, rows, contact_id, table, sleep):
    found = find_contact_instances(table, rows, contact_id, sleep)
    if not found:
        raise ValueError(
            f"Contact {_redacted_name(contact_id)} 在所选实例的 DynamoDB 中都不存在"
            f"（已查询 {len(rows)} 个实例）"
        )
    if len(found) == 1:
        print(f"Contact {_redacted_name(contact_id)} 位于实例 [{found[0]['key']}]")
        return found[0]
    print(f"Contact {_redacted_name(contact_id)} 在多个实例中存在:")
    for number, row in enumerate(found, 1):
        print(f"  {number}) [{row['key']}] alias={_redacted_name(row['alias'])}")

    def parse(raw):
        return found[int(raw) - 1] if raw.isdigit() and 1 <= int(raw) <= len(found) else None
    return _prompt(input_fn, "请选择实例编号: ", parse)


def _equivalent_command(args, extra_display):
    """Non-interactive command for the chosen selection, with sensitive values redacted.

    Built from the parsed ``args`` (not raw argv) so abbreviations or ``--x=y``
    forms cannot leak a profile/bucket/table/config name.
    """
    parts = ["python3", "backfill_ddb_contact_summaries.py"]
    if args.config:
        parts += ["--config", f"<config:{_redacted_name(Path(args.config).name)}>"]
    for flag, source, value, kind in (
        ("--profile", "profile", args.profile, "profile"),
        ("--source-bucket", "bucket", args.source_bucket, "bucket"),
        ("--target-table", "table", args.target_table, "table"),
    ):
        if args.sources.get(source) == "explicit":
            parts += [flag, f"<{kind}:{_redacted_name(value)}>"]
    if args.sources.get("region") == "explicit":
        parts += ["--region", args.region]
    defaults = build_parser()
    if ",".join(args.fields) != defaults.get_default("fields"):
        parts += ["--fields", ",".join(args.fields)]
    for flag, name in (("--page-size", "page_size"), ("--batch-size", "batch_size"),
                       ("--rate-limit", "rate_limit")):
        if getattr(args, name) != defaults.get_default(name):
            parts += [flag, str(getattr(args, name))]
    if args.state_dir != DEFAULT_STATE_DIR:
        parts += ["--state-dir", str(args.state_dir)]
    if args.checkpoint:
        parts += ["--checkpoint", args.checkpoint]
    if args.failures_file:
        parts += ["--failures-file", args.failures_file]
    return " ".join(shlex.quote(part) for part in parts + list(extra_display))


def interactive_select(args, argv, session_factory=boto3.Session, input_fn=input,
                       sleep=time.sleep):
    """Overview + menu; return ``argv`` plus the equivalent selection flags."""
    rows, table = collect_overview(args, session_factory, sleep)
    _print_overview(args, rows)
    scope = _prompt_scope(input_fn)
    contact_id = None
    if scope == "1":
        chosen = rows
        names = []
    elif scope == "2":
        chosen = _prompt_instances(input_fn, rows)
        names = ["--instance-name", ",".join(row["key"] for row in chosen)]
    else:
        contact_id = _prompt_contact_id(input_fn)
        chosen = [_pick_contact_instance(input_fn, rows, contact_id, table, sleep)]
        names = ["--instance-name", chosen[0]["key"]]
    write = _prompt_mode(input_fn) == "2"
    if contact_id is not None:
        extra = names + ["--contact-id", contact_id]
        display = names + ["--contact-id", f"<contactId:{_redacted_name(contact_id)}>"]
    else:
        extra = list(names)
        display = list(names)
    # Same flags a user would type: "--apply --all" for a whole scope,
    # "--apply" alone for one Contact (already a finite range).
    tail = (["--apply"] if contact_id else ["--apply", "--all"]) if write else ["--dry-run"]
    extra += tail
    display += tail
    print("等价的非交互命令: " + _equivalent_command(args, display))
    if write:
        print(f"即将写入 DynamoDB（字段级条件 UpdateItem，fields={','.join(args.fields)}）:")
        if contact_id is not None:
            print(f"  [{chosen[0]['key']}] 1 个 Contact (contact={_redacted_name(contact_id)})")
        else:
            for row in chosen:
                print(f"  [{row['key']}] S3 Contact={row['s3']} DynamoDB={row['ddb']}")
            print(f"  合计 {len(chosen)} 个实例 S3 Contact={sum(row['s3'] for row in chosen)} "
                  f"DynamoDB={sum(row['ddb'] for row in chosen)}（按概览时的计数）")
        if input_fn("输入 yes 确认写入，其他任意输入取消: ").strip() != "yes":
            raise InteractiveCancelled()
    return list(argv) + extra


def run(args, session_factory=boto3.Session, sleep=time.sleep):
    # Order matters: alias preflight and checkpoint validation happen before any
    # S3 or DynamoDB client is created, so a failure there touches no data.
    preflight_aliases(args, session_factory)
    contexts = build_contexts(args)
    states = [_load_checkpoint(ctx) for ctx in contexts]
    _print_startup(args, contexts, states)
    resume_only_apply = args.apply and args.resume and not args.all and not any((
        args.contact_id, args.limit, args.max_pages, args.batch_number, args.end_key,
    ))

    # Create the data session/clients before the instance loop (no API calls
    # yet) so credential errors such as ProfileNotFound reach main() and exit 2
    # instead of being recorded as a per-instance error; this matters on the
    # --source-prefix path, which has no DescribeInstance preflight.
    s3, table = _data_clients(args, session_factory)

    results = []
    for ctx, state in zip(contexts, states):
        print(f"== [{ctx.label}] ==")
        result = {"ctx": ctx, "status": "completed", "counts": None, "category": None}
        results.append(result)
        if ctx.checkpoint_missing:
            if resume_only_apply:
                # --resume is the only bound: starting fresh would be an
                # unbounded full write without --all, so refuse this instance.
                result.update(status="failed", category="ResumeWithoutCheckpoint")
                print(f"[{ctx.label}] 无 checkpoint，未执行: --apply --resume 需要该实例已有 "
                      "checkpoint；如需从头写入请增加范围参数或 --all", file=sys.stderr)
                continue
            print(f"[{ctx.label}] 无 checkpoint，从头开始")
        elif state and _state_completed(ctx, state) and not (
                ctx.retry_failures and _has_contact_failures(state)):
            result.update(status="skipped", counts=dict(state.get("counts") or _empty_counts()))
            print(f"[{ctx.label}] checkpoint 已完成，跳过")
            if result["counts"].get("failed"):
                # Unresolved failures still fail the run (exit 1), as before.
                print(f"[{ctx.label}] checkpoint 仍有 {result['counts']['failed']} 个未解决失败；"
                      "使用 --resume --retry-failures 重试", file=sys.stderr)
            continue
        outcome = {}
        try:
            run_instance(ctx, s3, table, state, sleep, outcome)
        except Exception as error:  # noqa: BLE001 - isolate per-instance failures
            if _is_credential_error(error):
                # Missing/expired credentials affect every instance: abort (exit 2).
                raise
            result.update(status="error", category=_error_code(error))
            print(f"[{ctx.label}] 实例处理中止: category={_error_code(error)}", file=sys.stderr)
        result["counts"] = outcome.get("counts")
        if result["status"] == "completed" and result["counts"]["failed"]:
            result["status"] = "failed"

    print("汇总:")
    totals = _empty_counts()
    for result in results:
        ctx, counts = result["ctx"], result["counts"] or _empty_counts()
        for key in totals:
            totals[key] += counts.get(key, 0)
        line = f"  [{ctx.label}] status={result['status']} " + " ".join(
            f"{key}={counts.get(key, 0)}" for key in totals)
        if result["category"]:
            line += f" category={result['category']}"
        print(line + f" checkpoint={ctx.checkpoint} failures={ctx.failures_file}")
    print("  total " + " ".join(f"{key}={totals[key]}" for key in totals))
    return 1 if any(
        result["status"] in ("failed", "error") or (result["counts"] or {}).get("failed")
        for result in results
    ) else 0


def run_instance(args, s3, table, state, sleep=time.sleep, outcome=None):
    """Process one instance; ``args`` is that instance's context namespace."""
    counts = dict((state or {}).get("counts") or _empty_counts())
    if outcome is not None:
        outcome["counts"] = counts
    failures = list((state or {}).get("failures") or [])
    resume_after = (state or {}).get("lastCompletedKey")
    processed_candidates = (state or {}).get("processedCandidates", sum(counts.values()))
    cursor = dict((state or {}).get("cursor") or {})
    retry_keys = []
    if args.retry_failures:
        seen = set()
        for item in failures:
            key = item.get("key", "")
            operation = item.get("operation", "contact")
            if (operation == "contact" and
                    _contact_from_key(args.source_prefix, key) and key not in seen):
                retry_keys.append(key)
                seen.add(key)

    processed_this_run = 0
    last_key = resume_after
    interval = 1.0 / args.rate_limit
    last_started = 0.0

    def save_state():
        state_value = _checkpoint_state(
            args, counts, last_key, failures, processed_candidates, cursor,
        )
        _atomic_json(args.checkpoint, state_value)

    def listing_completed(listing_cursor):
        nonlocal failures
        matched = [
            item for item in failures
            if item.get("operation") == "listing" and
            item.get("cursor") == listing_cursor
        ]
        if matched:
            failures = [item for item in failures if item not in matched]
            counts["failed"] = max(0, counts["failed"] - len(matched))

    def process_one(key, is_retry):
        nonlocal failures, last_key, last_started, processed_candidates
        wait = interval - (time.monotonic() - last_started)
        if wait > 0:
            sleep(wait)
        last_started = time.monotonic()
        status, category, diff = process_key(s3, table, args, key)
        if status == "failed" and category in CREDENTIAL_ERROR_NAMES:
            # Not a Contact failure: leave counts/failures untouched and keep the
            # candidate pending so --resume replays it once credentials work.
            raise CredentialsUnavailable(category=category)
        if is_retry:
            replaced = [
                item for item in failures
                if item.get("operation", "contact") == "contact" and
                item.get("key") == key
            ]
            failures = [item for item in failures if item not in replaced]
            counts["failed"] = max(0, counts["failed"] - len(replaced))
        else:
            processed_candidates += 1
            last_key = key
        counts[status] += 1
        if status == "failed":
            failures.append({
                "operation": "contact",
                "key": key,
                "category": category,
                "transient": category in TRANSIENT_CODES,
            })
        if not is_retry:
            _commit_candidate(cursor, key)
        contact_id = _contact_from_key(args.source_prefix, key) or "invalid"
        line = f"{status}: contact=…{_digest(contact_id)}"
        if category:
            line += f" category={category}"
        if diff:
            line += " diff=" + json.dumps(diff, ensure_ascii=False, sort_keys=True)
        print(line)

    try:
        for key in retry_keys:
            process_one(key, True)
            processed_this_run += 1
            if processed_this_run % args.batch_size == 0:
                save_state()

        new_keys = iter_candidate_keys(
            s3, args, resume_after=resume_after,
            batch_processed=processed_candidates if state else 0,
            progress=cursor, listing_succeeded=listing_completed,
        )
        while not args.limit or processed_candidates < args.limit:
            try:
                key = next(new_keys)
            except StopIteration:
                break
            process_one(key, False)
            processed_this_run += 1
            if processed_this_run % args.batch_size == 0:
                save_state()
    except ClientError as error:
        listing_cursor = cursor.get("pendingListing") or _listing_cursor(
            cursor.get("pageNumber", 1), cursor.get("continuationToken"),
            cursor.get("startAfter"),
        )
        replaced = [
            item for item in failures
            if item.get("operation") == "listing" and
            item.get("cursor") == listing_cursor
        ]
        if replaced:
            failures = [item for item in failures if item not in replaced]
            counts["failed"] = max(0, counts["failed"] - len(replaced))
        failures.append({
            "operation": "listing",
            "cursor": listing_cursor,
            "category": _error_code(error),
            "transient": _is_transient(error),
        })
        counts["failed"] += 1
        print(f"[{args.label}] listing failed: category={_error_code(error)}", file=sys.stderr)
    finally:
        save_state()
        _atomic_json(args.failures_file, {
            "version": CHECKPOINT_VERSION, "count": len(failures), "failures": failures,
        })
    print(f"[{args.label}] 结果: " + " ".join(
        f"{key}={counts[key]}" for key in ("unchanged", "changed", "missing", "failed")))
    print(f"[{args.label}] 失败报告: {args.failures_file} checkpoint: {args.checkpoint}")
    return counts


def _stdio_is_tty():
    return bool(sys.stdin and sys.stdin.isatty() and sys.stdout and sys.stdout.isatty())


def main(argv=None, *, session_factory=boto3.Session, sleep=time.sleep, input_fn=input,
         tty=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = parse_args(argv)
        if _should_interact(args, _stdio_is_tty() if tty is None else tty):
            try:
                argv = interactive_select(args, argv, session_factory, input_fn, sleep)
            except (EOFError, KeyboardInterrupt, InteractiveCancelled):
                print("\n已取消，未写入任何数据", file=sys.stderr)
                return 130
            # Re-parse so the interactive choice passes every regular check.
            args = parse_args(argv)
        return run(args, session_factory=session_factory, sleep=sleep)
    except ValueError as error:
        print(f"错误: {error}", file=sys.stderr)
        return 2
    except BotoCoreError as error:
        if _is_credential_error(error):
            name = (error.kwargs.get("category") if isinstance(error, CredentialsUnavailable)
                    else None) or type(error).__name__
            print(f"错误: {name}（AWS 凭证不可用：检查 profile 或重新 SSO 登录）",
                  file=sys.stderr)
            if name == "ProfileNotFound":
                _print_profile_diagnostics(error)
        else:
            print(f"错误: {type(error).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
