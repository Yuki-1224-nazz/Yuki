"""Gofile.io URL resolver.

Translates ``https://gofile.io/d/<contentId>`` into direct download
URLs that the download engine can stream.  The flow:

1. Create a guest account via ``POST /accounts``.
2. Fetch the website token (``wt``) from gofile.io's config.js.
3. Call ``GET /contents/<contentId>`` with the bearer token and ``wt``.
4. Extract ``link`` (direct download URL) for every file.
5. Return the list of ``(url, filename, size, token)`` tuples — the
   caller must pass ``Cookie: accountToken=<token>`` when downloading.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

_GOFILE_PATTERN = re.compile(
    r"https?://gofile\.io/d/([A-Za-z0-9]+)", re.IGNORECASE
)

GOFILE_API = "https://api.gofile.io"
GOFILE_CONFIG_URL = "https://gofile.io/dist/js/config.js"


def is_gofile_url(url: str) -> bool:
    return bool(_GOFILE_PATTERN.match(url.strip()))


def _extract_content_id(url: str) -> str:
    m = _GOFILE_PATTERN.match(url.strip())
    if not m:
        raise ValueError(f"not a gofile URL: {url}")
    return m.group(1)


class GofileError(RuntimeError):
    pass


class GofileFile:
    """Resolved file from a gofile folder."""

    __slots__ = ("name", "url", "size", "token")

    def __init__(self, name: str, url: str, size: int, token: str) -> None:
        self.name = name
        self.url = url
        self.size = size
        self.token = token


async def _create_guest_account(session: aiohttp.ClientSession) -> str:
    """Create a throwaway guest account and return the token."""
    async with session.post(f"{GOFILE_API}/accounts") as resp:
        data = await resp.json()
    if data.get("status") != "ok":
        raise GofileError(f"failed to create guest account: {data}")
    return data["data"]["token"]


async def _get_website_token(session: aiohttp.ClientSession) -> str:
    """Scrape the ``appdata.wt`` value from gofile's config.js."""
    async with session.get(GOFILE_CONFIG_URL) as resp:
        text = await resp.text()
    m = re.search(r'appdata\.wt\s*=\s*"([^"]+)"', text)
    if not m:
        log.warning("could not extract websiteToken from config.js")
        return "4fd6sg89d7s6"  # fallback
    return m.group(1)


async def resolve_gofile_url(
    url: str,
    *,
    password: Optional[str] = None,
) -> list[GofileFile]:
    """Resolve a gofile.io sharing URL into direct download links.

    Returns a list of :class:`GofileFile` objects. Each has a ``.url``
    that can be downloaded with ``Cookie: accountToken=<file.token>``.
    """
    content_id = _extract_content_id(url)

    async with aiohttp.ClientSession() as session:
        token = await _create_guest_account(session)
        wt = await _get_website_token(session)

        headers = {
            "Authorization": f"Bearer {token}",
            "x-website-token": wt,
        }

        api_url = f"{GOFILE_API}/contents/{content_id}"
        params: dict[str, str] = {}
        if password:
            params["password"] = hashlib.sha256(
                password.encode("utf-8")
            ).hexdigest()

        async with session.get(
            api_url, headers=headers, params=params
        ) as resp:
            data = await resp.json()

        if data.get("status") != "ok":
            raise GofileError(
                f"gofile API error for {content_id}: "
                f"{data.get('status', 'unknown')}"
            )

        contents = data.get("data", {}).get("children", {})
        if not contents:
            contents = data.get("data", {}).get("contents", {})

        files: list[GofileFile] = []
        for item in contents.values():
            if item.get("type") != "file":
                continue
            link = item.get("link") or item.get("directLink")
            if not link:
                continue
            files.append(
                GofileFile(
                    name=item.get("name", "unknown"),
                    url=link,
                    size=item.get("size", 0),
                    token=token,
                )
            )

        if not files:
            raise GofileError(
                f"no downloadable files found in gofile content {content_id}"
            )

        log.info(
            "resolved gofile %s → %d file(s), total %s bytes",
            content_id,
            len(files),
            sum(f.size for f in files),
        )
        return files
