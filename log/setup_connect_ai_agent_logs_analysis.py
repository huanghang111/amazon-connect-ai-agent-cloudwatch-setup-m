#!/usr/bin/env python3
"""setup-connect-ai-agent-logs-analysis.py

用途:
  只从「已有日志文件」构建 Amazon Connect AI Agent 日志排查页面(不连 CloudWatch)。
  输入可以是单个日志文件，也可以是一个目录(加载该目录下的所有日志文件);
  两者都支持本地路径与 S3 地址(s3://...)。
  解析后按 Contact ID 关联成可视化时间线，生成静态站点并本地预览。

用法:
  ./setup-connect-ai-agent-logs-analysis.py <日志文件|日志目录> [选项]
  ./setup-connect-ai-agent-logs-analysis.py --input <日志文件|日志目录> [选项]

输入形式(4 种):
  本地文件      ./contact-xxxx-logs.csv
  本地目录      ./cloudwatch-logs-20260707-021617        (递归加载目录下所有日志文件)
  S3 文件       s3://my-bucket/logs/contact-xxxx-logs.csv
  S3 目录       s3://my-bucket/logs/                     (递归下载该前缀下所有日志文件)

选项:
  --input <path>     同位置参数; 日志文件或目录(本地或 s3://)
  --gateway <path>   额外的 Bedrock AgentCore Gateway 日志(文件或目录, 本地或 s3://),
                     用于跨源关联; 不给则只展示 Connect 一路。
  --region <r>       访问 S3 时使用的区域(可选)
  --profile <p>      访问 S3 时使用的 AWS CLI profile(可选)
  --out-dir <dir>    站点构建输出目录，默认 ./dist
  --no-serve         只构建，不启动本地预览
  --port <n>         本地预览端口，默认 8080
  -h, --help         显示帮助

支持的日志格式(按内容自动识别, 目录模式下按扩展名筛选 .csv/.json/.log/.jsonl/.txt/.gz):
  - 页面「下载 CSV」导出的多列 CSV(timestamp_ms,datetime,source,event_type,message,...)
  - 简单两列 CSV(timestamp,message)
  - aws logs filter-log-events 的 JSON
  - load-cloudwatch-logs.sh 生成的 events.log / events.json
  - 上述格式的 .gz 压缩文件
  文件名中含 gateway / agentcore 的按 Gateway 日志处理; CSV 若带 source 列则以该列为准。

依赖: python3(仅标准库)；输入为 s3:// 时还需 aws cli v2(已配置凭证)。

"""

import errno
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import List, Optional


def _script_dir() -> Path:
    """脚本所在目录。不用 resolve(): 与 bash 的 `cd && pwd` 一样，不解析符号链接。

    若当前目录是经符号链接进入的(如 macOS 的 /tmp -> /private/tmp)，沿用 $PWD 的逻辑路径，
    使提示信息里的路径与 bash 版一致。
    """
    d = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    pwd = os.environ.get("PWD", "")
    try:
        if (pwd and pwd != cwd and os.path.isabs(pwd) and os.path.samefile(pwd, cwd)
                and (d == cwd or d.startswith(cwd + os.sep))):
            d = pwd + d[len(cwd):]
    except OSError:
        pass
    return Path(d)


SCRIPT_DIR = _script_dir()
LIB_DIR = SCRIPT_DIR / "cloudfront" / "lib"
WEB_DIR = LIB_DIR / "web"
PARSER = LIB_DIR / "parse-connect-ai-logs.py"
SERVE_PY = LIB_DIR / "serve.py"

LOG_EXTS = (".csv", ".json", ".jsonl", ".log", ".txt", ".gz")
# index.html 里的「防闪屏」规则; 包含它的整行会从输出副本中删除
ANTI_FLASH = b"body > header, #viewDashboard { display: none !important; }"
STATIC_NOTICE = (
    "⚠️  静态模式: 未找到 lib/serve.py, /api 功能(按需翻译、DescribeContact 实时查询)不可用, "
    "页面以离线/只读模式运行, 日志内容仍可完整查看。"
)
SEP_LINE = "==================================================================="


class ScriptExit(Exception):
    """以指定退出码结束脚本(等价于 bash 里的 exit N)。"""

    def __init__(self, code: int = 1) -> None:
        super().__init__(code)
        self.code = code


class Options:
    """命令行选项及其默认值。"""

    def __init__(self) -> None:
        self.input = ""
        self.gateway = ""
        self.region = ""
        self.profile = ""
        self.out_dir = str(SCRIPT_DIR / "dist")
        self.serve = True
        self.port = "8080"  # 校验前保持字符串


def usage() -> None:
    """把脚本头部的用法说明打印到 stdout。"""
    sys.stdout.write(__doc__ or "")


def err(*lines: str) -> None:
    """逐行打印到 stderr。"""
    for line in lines:
        print(line, file=sys.stderr)


def die(*lines: str, code: int = 1) -> None:
    """打印错误信息到 stderr 并以 code 结束脚本。"""
    err(*lines)
    raise ScriptExit(code)


# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------
def parse_args(argv: List[str]) -> Options:
    """解析命令行(位置参数等价于 --input)。

    与 bash 版一致: 取值型选项无条件吞掉下一个参数; -h 按顺序处理后立即退出。
    """
    opts = Options()
    n = len(argv)
    i = 0
    while i < n:
        arg = argv[i]
        if arg in ("--input", "--log-file", "--file", "--dir", "--gateway",
                   "--gateway-file", "--region", "--profile", "--out-dir", "--port"):
            if i + 1 >= n:
                die("错误: 选项 %s 需要一个参数" % arg)
            value = argv[i + 1]
            if arg in ("--input", "--log-file", "--file", "--dir"):
                opts.input = value
            elif arg in ("--gateway", "--gateway-file"):
                opts.gateway = value
            elif arg == "--region":
                opts.region = value
            elif arg == "--profile":
                opts.profile = value
            elif arg == "--out-dir":
                opts.out_dir = value
            else:
                opts.port = value
            i += 2
        elif arg == "--no-serve":
            opts.serve = False
            i += 1
        elif arg in ("-h", "--help"):
            usage()
            raise ScriptExit(0)
        elif arg.startswith("-"):
            err("未知参数: %s" % arg)
            usage()
            raise ScriptExit(1)
        else:
            if not opts.input:
                opts.input = arg
                i += 1
            else:
                die("多余的位置参数: %s(如需第二路日志请用 --gateway)" % arg)
    return opts


def validate_port(raw: str) -> int:
    """校验端口为 1-65535 的整数(仅 ASCII 数字)，否则报错退出。"""
    if re.fullmatch(r"[0-9]+", raw) is None or not 1 <= int(raw) <= 65535:
        die("错误: --port 必须是 1-65535 之间的整数: %s" % raw)
    return int(raw)


def prompt_for_input() -> str:
    """未给输入时交互式询问；EOF 时静默以 1 退出(与 `read -r` 在 set -e 下一致)。"""
    print("请输入日志文件或日志目录(本地路径或 s3:// 地址)。")
    print("  单个文件: ./contact-xxxx-logs.csv   或  s3://my-bucket/logs/contact-xxxx-logs.csv")
    print("  整个目录: ./cloudwatch-logs-2026    或  s3://my-bucket/logs/")
    sys.stdout.write("日志文件/目录: ")
    sys.stdout.flush()
    line = sys.stdin.readline()
    if not line.endswith("\n"):  # EOF(含无换行的残行): bash 的 read 返回非 0
        raise ScriptExit(1)
    value = line[:-1].strip(" \t")
    if not value:
        die("错误: 未提供日志文件或目录。")
    return value


# ---------------------------------------------------------------------------
# AWS CLI 辅助
# ---------------------------------------------------------------------------
def aws_base(region: str, profile: str) -> List[str]:
    """返回带 --region/--profile 的 aws 命令前缀。"""
    cmd = ["aws"]
    if region:
        cmd += ["--region", region]
    if profile:
        cmd += ["--profile", profile]
    return cmd


def need_aws(url: str) -> None:
    """输入是 S3 地址时要求 aws CLI 存在。"""
    if shutil.which("aws") is None:
        die("错误: 输入是 S3 地址(%s)，需要 aws CLI(已配置凭证)。" % url)


# ---------------------------------------------------------------------------
# 1. 准备站点构建目录
# ---------------------------------------------------------------------------
def strip_anti_flash(src: Path, dst: Path) -> None:
    """复制 index.html，删除含防闪屏规则的整行(按字节处理，保留 CRLF)；源文件不变。"""
    lines = src.read_bytes().split(b"\n")
    kept = [ln for ln in lines if ANTI_FLASH not in ln]
    dst.write_bytes(b"\n".join(kept))


def prepare_output_dir(out_dir: str) -> bool:
    """准备输出目录，返回 lib/serve.py 是否存在。"""
    print("==> 准备站点构建目录: %s" % out_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name in ("index.html", "app.js", "i18n.js", "site-config.js"):
        if not (WEB_DIR / name).is_file():
            die("错误: 找不到站点资源: %s" % (WEB_DIR / name))
    # 只处理输出目录里的副本，源文件(CloudFront 部署用)保持不变
    strip_anti_flash(WEB_DIR / "index.html", out / "index.html")
    for name in ("app.js", "i18n.js", "site-config.js"):
        shutil.copyfile(str(WEB_DIR / name), str(out / name))
    has_serve_py = SERVE_PY.is_file()
    if not has_serve_py:
        # 只写到输出目录的 site-config.js，前端据此跳过 /api 探测
        marker = "\n/* 由构建脚本生成: 静态模式(无 /api 后端) */\nwindow.__API_AVAILABLE__ = false;\n"
        with open(str(out / "site-config.js"), "ab") as fh:
            fh.write(marker.encode("utf-8"))
    # 可选的 Amazon Connect 补充数据; 不存在则生成空桩，避免页面 404
    if (WEB_DIR / "connect-enrich.js").is_file():
        shutil.copyfile(str(WEB_DIR / "connect-enrich.js"), str(out / "connect-enrich.js"))
    elif not (out / "connect-enrich.js").is_file():
        (out / "connect-enrich.js").write_bytes(b"window.__CONNECT_CONTACT_ENRICH__ = {};\n")
    return has_serve_py


def reset_stage_dir(out_dir: str) -> Path:
    """重建 S3 下载暂存目录(等价于 rm -rf)，返回其路径。"""
    stage = Path(out_dir) / "_input"
    if stage.is_symlink() or stage.is_file():
        stage.unlink()
    elif stage.is_dir():
        shutil.rmtree(str(stage))
    return stage


# ---------------------------------------------------------------------------
# 2. 把输入(本地/S3, 文件/目录)解析成一批本地日志文件
# ---------------------------------------------------------------------------
def has_regular_file(root: Path) -> bool:
    """目录下是否至少有一个普通文件(不含符号链接，等价 find -type f)。"""
    for dirpath, _dirs, files in os.walk(str(root)):
        for name in files:
            full = os.path.join(dirpath, name)
            if os.path.isfile(full) and not os.path.islink(full):
                return True
    return False


def s3_fetch(url: str, tag: str, stage: Path, base_cmd: List[str]) -> str:
    """从 s3:// 地址下载到暂存目录，返回本地文件或目录路径。

    注意: bash 版里本函数运行在 $(...) 子 shell 中，set -e 不生效——
    aws s3 cp / sync 本身失败不会立刻中断，后续由「找不到文件/目录下没有对象」的检查报错。
    """
    need_aws(url)
    rest = url[len("s3://"):]
    bucket, _, key = rest.partition("/")  # 修正: bash 的 key==bucket 判断会误伤 s3://b/b
    if not bucket:
        die("错误: S3 地址不合法: %s" % url)

    dest = stage / tag
    dest.mkdir(parents=True, exist_ok=True)

    # 以 / 结尾，或 head-object 不存在 -> 当作前缀(目录)整体同步
    is_object = False
    if key and not url.endswith("/"):
        probe = subprocess.run(
            base_cmd + ["s3api", "head-object", "--bucket", bucket, "--key", key],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        is_object = probe.returncode == 0
    if is_object:
        target = dest / os.path.basename(key)
        print("==> 下载 S3 文件: %s" % url, file=sys.stderr)
        sys.stderr.flush()
        subprocess.run(base_cmd + ["s3", "cp", url, str(target)], stdout=subprocess.DEVNULL)
        return str(target)

    prefix = "s3://%s/%s" % (bucket, key)
    if key:
        prefix = "s3://%s/%s/" % (bucket, key[:-1] if key.endswith("/") else key)
    print("==> 同步 S3 目录: %s" % prefix, file=sys.stderr)
    sys.stderr.flush()
    subprocess.run(base_cmd + ["s3", "sync", prefix, str(dest)], stdout=subprocess.DEVNULL)
    if not has_regular_file(dest):
        die("错误: S3 目录下没有对象: %s" % prefix,
            "      若这是单个对象, 请去掉结尾的 /; 若是前缀, 请确认前缀与权限。")
    return str(dest)


def resolve_input(p: str, tag: str, stage: Path, base_cmd: List[str]) -> str:
    """把输入解析成本地路径(S3 先下载; 本地路径支持相对当前目录或相对脚本目录)。"""
    if p.startswith("s3://"):
        return s3_fetch(p, tag, stage, base_cmd)
    if os.path.exists(p):
        return p
    alt = "%s/%s" % (SCRIPT_DIR, p)
    if os.path.exists(alt):
        return alt
    die("错误: 找不到日志文件或目录: %s" % p,
        "      支持本地文件/目录(相对当前目录或脚本目录)与 s3:// 地址。")
    return ""  # pragma: no cover (die 已抛出)


def collect_files(p: str) -> List[str]:
    """收集日志文件: 单个文件原样返回; 目录则递归筛选常见日志扩展名。

    既不是文件也不是目录时只打印错误并返回空列表(bash 版的 exit 1 在进程替换里，不会中断脚本)。
    """
    if os.path.isfile(p):
        return [p]
    if os.path.isdir(p):
        found = []  # type: List[str]
        for dirpath, _dirs, files in os.walk(p, followlinks=False):
            for name in files:
                full = os.path.join(dirpath, name)
                if os.path.islink(full) or not os.path.isfile(full):
                    continue
                if name.startswith("."):
                    continue
                if name.lower().endswith(LOG_EXTS):
                    found.append(full)
        return sorted(found)
    err("错误: 既不是文件也不是目录: %s" % p)
    return []


# ---------------------------------------------------------------------------
# 3. 解析 -> data.js
# ---------------------------------------------------------------------------
def run_parser(connect: List[str], gateway: List[str], out_dir: str) -> None:
    """调用解析脚本生成 data.js；失败时以其退出码退出(解析器自己已打印原因)。"""
    print("==> 解析日志 -> data.js")
    cmd = [sys.executable, str(PARSER), "--connect"] + connect
    if gateway:
        cmd += ["--gateway"] + gateway
    cmd += ["--out", str(Path(out_dir) / "data.js")]
    sys.stdout.flush()
    sys.stderr.flush()
    rc = subprocess.run(cmd).returncode
    if rc != 0:
        raise ScriptExit(rc)


def count_events(data_js: Path) -> int:
    """统计 data.js 里解析出的事件数；任何异常都按 0 处理。"""
    try:
        text = data_js.read_text(encoding="utf-8")
        m = re.search(r"=\s*(\[.*\]);\s*$", text, re.S)
        return len(json.loads(m.group(1))) if m else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 4. 可选组件检查(全部非致命)
# ---------------------------------------------------------------------------
def check_optional_pieces(has_serve_py: bool, base_cmd: List[str]) -> None:
    """只提示，不会中断构建。"""
    if not has_serve_py:
        print("ℹ️  未找到 lib/serve.py(可选组件): 将以纯静态模式预览。")
        return
    if shutil.which("aws") is None:
        print("⚠️  未找到 aws CLI: 按需翻译与 DescribeContact 实时查询将不可用(页面会自动降级)。")
        return
    # 凭证探测限制重试次数与超时，失败只提示(外层 timeout 为兜底)
    env = dict(os.environ)
    env["AWS_MAX_ATTEMPTS"] = "1"
    ok = False
    try:
        probe = subprocess.run(
            base_cmd + ["--cli-connect-timeout", "3", "--cli-read-timeout", "5",
                        "sts", "get-caller-identity"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
        )
        ok = probe.returncode == 0
    except (OSError, subprocess.SubprocessError):
        ok = False
    if not ok:
        print("⚠️  AWS 凭证不可用或网络不通: 按需翻译与 DescribeContact 实时查询可能失败(页面会自动降级)。")


# ---------------------------------------------------------------------------
# 5. 本地预览
# ---------------------------------------------------------------------------
def port_in_use(port: int) -> bool:
    """用 127.0.0.1 上的 bind 测试判断端口是否被占用。

    bind 因 EADDRINUSE 失败时，再用 connect 确认是否真有监听者:
    刚退出的服务器留下的 TIME_WAIT 连接也会让 bind 失败，但那不影响重新启动。
    """
    if not 1 <= port <= 65535:
        return True
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
        return False
    except OSError as exc:
        if exc.errno != errno.EADDRINUSE:
            return False  # 如权限不足: 交给真正启动服务器时报错
    finally:
        sock.close()
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(2)
    try:
        return probe.connect_ex(("127.0.0.1", port)) == 0
    finally:
        probe.close()


def print_build_done(opts: Options, port: int, has_serve_py: bool) -> None:
    """--no-serve: 打印构建完成与预览方式。"""
    print("")
    print(SEP_LINE)
    print("构建完成！")
    if has_serve_py:
        print("本地预览方式(推荐, 含 /api 接口: 翻译 + DescribeContact 实时查询):")
        print('  python3 "%s" --dir "%s" --port %d' % (SERVE_PY, opts.out_dir, port))
        print("或纯静态预览(无 /api 接口):")
        print('  cd "%s" && python3 -m http.server %d --bind 127.0.0.1' % (opts.out_dir, port))
        print("  然后浏览器访问 http://localhost:%d" % port)
    else:
        print("本地预览方式(纯静态):")
        print('  cd "%s" && python3 -m http.server %d --bind 127.0.0.1' % (opts.out_dir, port))
        print("  然后浏览器访问 http://127.0.0.1:%d" % port)
        print(STATIC_NOTICE)
    print(SEP_LINE)


def find_free_port(port: int) -> Optional[int]:
    """从 port 起最多向后探测 20 次，返回空闲端口；找不到返回 None。"""
    try_port = port
    attempts = 0
    while port_in_use(try_port) and attempts < 20:
        print("    端口 %d 已被占用，尝试 %d ..." % (try_port, try_port + 1))
        try_port += 1
        attempts += 1
    if port_in_use(try_port):
        err("错误: 未能在 %d~%d 找到空闲端口。" % (port, try_port),
            "      请用 --port 指定其它端口，或释放被占用的端口。")
        return None
    return try_port


def serve_with_serve_py(out_dir: str, port: int) -> int:
    """用 lib/serve.py 提供静态站点 + /api；成功时替换当前进程。"""
    print("")
    print("==> 启动本地预览服务器: http://localhost:%d" % port)
    print("    (通过 lib/serve.py 提供静态站点 + /api 接口: 按需翻译、DescribeContact 实时查询)")
    print("    按 Ctrl+C 退出。")
    sys.stdout.flush()
    sys.stderr.flush()
    cmd = [sys.executable, str(SERVE_PY), "--dir", out_dir, "--port", str(port)]
    try:
        os.execv(cmd[0], cmd)
    except (AttributeError, OSError):
        pass  # 无 execv(如 Windows)时退回子进程
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0


def serve_static(out_dir: str, port: int) -> int:
    """兜底: 纯静态预览，仅绑定本机回环地址，避免把含客户对话的日志暴露给局域网。"""
    print("")
    print("==> 启动静态预览服务器: http://127.0.0.1:%d" % port)
    print("    %s" % STATIC_NOTICE)
    print("    按 Ctrl+C 退出。")
    sys.stdout.flush()
    handler = partial(SimpleHTTPRequestHandler, directory=out_dir)
    try:
        server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    except OSError as exc:
        err("错误: 无法启动预览服务器: %s" % exc)
        return 1
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止预览服务器。")
    finally:
        server.server_close()
    return 0


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def run(argv: List[str]) -> int:
    """脚本主流程；失败通过 ScriptExit 传出退出码。"""
    opts = parse_args(argv)
    port = validate_port(opts.port)

    if not PARSER.is_file():
        die("错误: 找不到解析脚本 %s" % PARSER)

    if not opts.input:
        opts.input = prompt_for_input()

    base_cmd = aws_base(opts.region, opts.profile)

    has_serve_py = prepare_output_dir(opts.out_dir)
    stage = reset_stage_dir(opts.out_dir)

    connect_files = []  # type: List[str]
    gateway_files = []  # type: List[str]

    resolved = resolve_input(opts.input, "main", stage, base_cmd)
    if os.path.isdir(resolved):
        print("==> 加载日志目录: %s" % resolved)
    else:
        print("==> 加载日志文件: %s" % resolved)
    for f in collect_files(resolved):
        # 文件名带 gateway / agentcore 的按 Gateway 日志处理(CSV 的 source 列优先级更高)
        low = os.path.basename(f).lower()
        if "gateway" in low or "agentcore" in low:
            gateway_files.append(f)
        else:
            connect_files.append(f)

    if opts.gateway:
        resolved_gw = resolve_input(opts.gateway, "gateway", stage, base_cmd)
        print("==> 加载 Gateway 日志: %s" % resolved_gw)
        gateway_files.extend(collect_files(resolved_gw))

    if not connect_files and not gateway_files:
        die("错误: 在 %s 下没有找到日志文件。" % resolved,
            "      目录模式只收集 .csv / .json / .jsonl / .log / .txt / .gz 文件。")
    print("    Connect 日志 %d 个, Gateway 日志 %d 个" % (len(connect_files), len(gateway_files)))

    # 只有 Gateway 文件时也要让解析器跑起来(--connect 为必填)
    if not connect_files:
        connect_files = gateway_files
        gateway_files = []

    run_parser(connect_files, gateway_files, opts.out_dir)
    print("==> 站点已构建到: %s" % opts.out_dir)

    count = count_events(Path(opts.out_dir) / "data.js")
    if count == 0:
        print("")
        print("⚠️  提示: 未从输入解析出任何事件，页面会是空的。")
        print("    请确认输入是页面导出的 CSV、filter-log-events 的 JSON，")
        print("    或 load-cloudwatch-logs.sh 生成的 events.log / events.json。")
        print("")
    else:
        print("    共 %d 条事件" % count)

    check_optional_pieces(has_serve_py, base_cmd)

    if not opts.serve:
        print_build_done(opts, port, has_serve_py)
        return 0

    try_port = find_free_port(port)
    if try_port is None:
        return 1
    if has_serve_py:
        return serve_with_serve_py(opts.out_dir, try_port)
    return serve_static(opts.out_dir, try_port)


def main(argv: Optional[List[str]] = None) -> int:
    """入口: 统一处理退出码、编码与异常。"""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    try:
        return run(list(sys.argv[1:] if argv is None else argv))
    except ScriptExit as exc:
        return exc.code
    except (OSError, shutil.Error) as exc:
        err("错误: %s" % exc)
        return 1
    except KeyboardInterrupt:
        err("")
        return 130


if __name__ == "__main__":
    sys.exit(main())
