import base64
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from pathlib import Path

import httpx

log = logging.getLogger("mcp_server")

# Prefix that marks an encrypted value in .env files
_ENC_PREFIX = "ENC:"


def _decrypt_value(encrypted_val: str, fernet) -> str:
    """Decrypt a Fernet-encrypted value (without the ENC: prefix)."""
    try:
        return fernet.decrypt(encrypted_val.encode()).decode()
    except Exception as e:
        log.error(f"Failed to decrypt value: {e}")
        sys.exit(1)


def _get_fernet(ENV_ENCRYPT_KEY: str=None):
    """Return a Fernet instance using ENV_ENCRYPT_KEY from the environment.
    
    ENV_ENCRYPT_KEY must be a URL-safe base64-encoded 32-byte key,
    which is the format produced by Fernet.generate_key().
    """
    key = ENV_ENCRYPT_KEY or os.environ.get("ENV_ENCRYPT_KEY", "").strip()
    if not key:
        log.error("ENV_ENCRYPT_KEY is not set. Cannot decrypt .env values.")
        sys.exit(1)
    try:
        from cryptography.fernet import Fernet
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as e:
        log.error(f"Invalid ENV_ENCRYPT_KEY: {e}")
        sys.exit(1)


def load_env_file(env_file: Path, ENV_ENCRYPT_KEY: str=None) -> None:
    """Load environment variables from .env file if it exists.

    Uses python-dotenv for standard .env parsing (supports quotes, multiline,
    escape sequences, etc.). Values prefixed with ENC: are Fernet-decrypted
    using ENV_ENCRYPT_KEY (must already be set in the runtime environment).

    Priority: Runtime environment variables > .env file
    This allows runtime to override packaged defaults.

    Args:
        env_file: Path to the .env file
    """
    if not env_file.exists():
        log.error(f"Environment file not found: {env_file}")
        sys.exit(1)

    try:
        from dotenv import dotenv_values
    except ImportError:
        log.error("Missing dependency: pip install python-dotenv")
        sys.exit(1)

    log.info(f"Loading environment from: {env_file}")
    loaded_count = 0
    fernet = None  # lazy-initialised only when an ENC: value is encountered

    # dotenv_values parses the file without touching os.environ
    parsed = dotenv_values(env_file)

    for key, val in parsed.items():
        if val is None:
            log.debug(f"Skipped (no value): {key}")
            continue

        # Decrypt if value is marked as encrypted
        if val.startswith(_ENC_PREFIX):
            if fernet is None:
                fernet = _get_fernet(ENV_ENCRYPT_KEY)
            val = _decrypt_value(val[len(_ENC_PREFIX):], fernet)

        # Only set if not already in environment (runtime env takes precedence)
        os.environ[key] = val
        loaded_count += 1
        log.debug(f"Loaded from .env: {key}")

    log.info(f"Loaded {loaded_count} variables from {env_file.name}")


async def call_api(url: str, payload: dict, headers: dict = None, method: str = 'POST', timeout: int = 60) -> dict:
    """Call external API with POST method (async)."""

    if headers is None:
        headers = {"Content-Type": "application/json; charset=utf-8"}

    start = time.time()
    log.info("call_api POST %s start data=%s", url, payload)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                url,
                content=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers=headers,
            )
            elapsed = time.time() - start
            body = resp.text
            log.info("call_api POST %s ok in %.2fs resp=%s", url, elapsed, body)
            resp.raise_for_status()
            return json.loads(body)

    except httpx.HTTPStatusError as e:
        elapsed = time.time() - start
        log.warning("call_api POST %s HTTP %s in %.2fs body=%s", url, e.response.status_code, elapsed, e.response.text)
        return {"error": f"HTTP {e.response.status_code}", "message": e.response.text}

    except httpx.RequestError as e:
        elapsed = time.time() - start
        log.error("call_api POST %s RequestError in %.2fs: %s", url, elapsed, e)
        return {"error": "RequestError", "message": str(e)}

    except Exception as e:
        elapsed = time.time() - start
        log.exception("call_api POST %s exception in %.2fs", url, elapsed)
        return {"error": str(type(e).__name__), "message": str(e)}


async def call_iservice(path: str, payload: dict) -> dict:
    """Call iService API"""

    api_url = os.environ.get("ISERVICE_API_URL")
    api_key = os.environ.get("ISERVICE_API_KEY")
    api_secret = os.environ.get("ISERVICE_API_SECRET")
    # Build JWT header and payload
    header  = '{"alg":"HS256","typ":"JWT"}'
    jwt_payload = f'{{"iss":"{api_key}","exp":{int(time.time()) + 60}}}'

    # Base64url encode without padding
    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header_b64  = _b64url(header.encode())
    payload_b64 = _b64url(jwt_payload.encode())
    signing_input = f"{header_b64}.{payload_b64}"

    # HMAC-SHA256 signature
    sig = hmac.new(api_secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    token = f"{signing_input}.{_b64url(sig)}"

    headers = {
        "Content-Type":  "application/json; charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    return await call_api(api_url + path, payload, headers=headers)