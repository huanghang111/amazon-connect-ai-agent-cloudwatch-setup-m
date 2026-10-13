import json
import logging
import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from util import load_env_file
from session_client import SessionClient
from tools import (
    register_faq_tools,
    register_verification_tools,
    register_repair_tools,
    register_address_tools,
)


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("mcp_server")

# Suppress noisy ping/keepalive logs from MCP internal loggers
logging.getLogger("mcp.server.lowlevel.server").setLevel(logging.WARNING)
logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
mcp = FastMCP(host="0.0.0.0", stateless_http=True)


# ── Load environment variables from .env file ────────────────────────────────────
# Load .env file from the same directory as this script
HERE = Path(__file__).parent
ENV_FILE = HERE / ".env"
load_env_file(ENV_FILE)


# ── environment ──────────────────────────────────────────────────────────────────
_REGION = os.environ.get("REGION")
_FAQ_URL = os.environ.get("FAQ_API_URL")
session_client = SessionClient(_REGION)

# ── Routes ────────────────────────────────────────────────────────────────────


@mcp.custom_route("/ping", methods=["GET"])
async def ping(request: Request) -> JSONResponse:
    return JSONResponse({"status": "Healthy"})


# ── Tools ─────────────────────────────────────────────────────────────────────

# Register all tools
register_faq_tools(
    mcp,
    session_client,
    _FAQ_URL,
)
register_verification_tools(mcp, session_client)
register_repair_tools(mcp, session_client)
register_address_tools(mcp)


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
