"""URL resolvers for popular file hosting services.

Converts sharing page URLs into direct download URLs that can be
streamed by the download module. Each resolver is an async function
that takes a page URL and returns a list of ``ResolvedFile`` objects.

Supported services:
- gofile.io
- mediafire.com
- pixeldrain.com
- krakenfiles.com
- send.cm
- 1fichier.com (direct links only)

Plain ``http(s)`` direct-download URLs are passed through unchanged.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import aiohttp

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


@dataclass
class ResolvedFile:
    """A resolved direct download URL with optional metadata."""
    url: str
    filename: Optional[str] = None
    size: Optional[int] = None
    headers: Optional[dict[str, str]] = None


class ResolveError(RuntimeError):
    """Raised when URL resolution fails."""


def _host_matches(url: str, *patterns: str) -> bool:
    """Check if the URL host matches any of the given patterns."""
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return False
    host = host.lower()
    return any(host == p or host.endswith("." + p) for p in patterns)


# ---------------------------------------------------------------------------
# gofile.io
# ---------------------------------------------------------------------------

async def _resolve_gofile(url: str, password: Optional[str] = None) -> list[ResolvedFile]:
    """Resolve gofile.io share links to direct download URLs."""
    content_id = url.rstrip("/").split("/")[-1]
    api = "https://api.gofile.io"

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:
        resp = await session.post(f"{api}/accounts")
        data = await resp.json()
        if data.get("status") != "ok":
            raise ResolveError(f"gofile: failed to create guest account: {data}")
        token = data["data"]["token"]

        params: dict[str, str] = {
            "wt": "4fd6sg89d7s6",
        }
        headers = {
            "Authorization": f"Bearer {token}",
        }
        if password:
            params["password"] = hashlib.sha256(
                password.encode("utf-8")
            ).hexdigest()

        resp = await session.get(
            f"{api}/contents/{content_id}",
            params=params,
            headers=headers,
        )
        data = await resp.json()

        if data.get("status") == "error-passwordRequired":
            raise ResolveError(
                "gofile: this link requires a password. "
                "Send the password when prompted."
            )
        if data.get("status") != "ok":
            raise ResolveError(f"gofile: API error: {data.get('status', data)}")

        children = data.get("data", {}).get("children", {})
        if not children:
            children = data.get("data", {}).get("contents", {})

        files: list[ResolvedFile] = []
        for item in children.values() if isinstance(children, dict) else children:
            if isinstance(item, dict) and item.get("type") == "file":
                dl = item.get("link") or item.get("directLink", "")
                if dl:
                    files.append(ResolvedFile(
                        url=dl,
                        filename=item.get("name"),
                        size=item.get("size"),
                        headers={
                            "Cookie": f"accountToken={token}",
                            "User-Agent": DEFAULT_USER_AGENT,
                        },
                    ))

        if not files:
            raise ResolveError("gofile: no downloadable files found in this link")
        return files


# ---------------------------------------------------------------------------
# mediafire.com
# ---------------------------------------------------------------------------

async def _resolve_mediafire(url: str) -> list[ResolvedFile]:
    """Resolve MediaFire share links by scraping the download page."""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:
        resp = await session.get(url, allow_redirects=True)
        html = await resp.text()

        match = re.search(
            r'href="(https?://download\d*\.mediafire\.com/[^"]+)"',
            html,
        )
        if not match:
            match = re.search(
                r'id="downloadButton"[^>]*href="([^"]+)"',
                html,
            )
        if not match:
            match = re.search(
                r"aria-label=\"Download file\"[^>]*href=\"([^\"]+)\"",
                html,
            )
        if not match:
            raise ResolveError(
                "mediafire: could not find download link on the page. "
                "The file may have been removed or the link is invalid."
            )

        direct_url = match.group(1)
        name_match = re.search(
            r'class="dl-btn-label"[^>]*title="([^"]+)"', html
        )
        filename = name_match.group(1) if name_match else None

        return [ResolvedFile(url=direct_url, filename=filename)]


# ---------------------------------------------------------------------------
# pixeldrain.com
# ---------------------------------------------------------------------------

async def _resolve_pixeldrain(url: str) -> list[ResolvedFile]:
    """Resolve pixeldrain share links to direct download URLs."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    if "/u/" in path:
        file_id = path.split("/u/")[-1]
        return [ResolvedFile(
            url=f"https://pixeldrain.com/api/file/{file_id}",
            filename=None,
        )]
    elif "/l/" in path:
        list_id = path.split("/l/")[-1]
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        ) as session:
            resp = await session.get(
                f"https://pixeldrain.com/api/list/{list_id}"
            )
            data = await resp.json()
            files: list[ResolvedFile] = []
            for item in data.get("files", []):
                fid = item.get("id", "")
                files.append(ResolvedFile(
                    url=f"https://pixeldrain.com/api/file/{fid}",
                    filename=item.get("name"),
                    size=item.get("size"),
                ))
            if not files:
                raise ResolveError("pixeldrain: no files found in this list")
            return files

    raise ResolveError(
        "pixeldrain: unrecognized URL format. "
        "Expected /u/<id> or /l/<id>."
    )


# ---------------------------------------------------------------------------
# krakenfiles.com
# ---------------------------------------------------------------------------

async def _resolve_krakenfiles(url: str) -> list[ResolvedFile]:
    """Resolve KrakenFiles share links by scraping for the download token."""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:
        resp = await session.get(url, allow_redirects=True)
        html = await resp.text()

        token_match = re.search(
            r'name="token"\s+value="([^"]+)"', html
        )
        if not token_match:
            raise ResolveError(
                "krakenfiles: could not find download token. "
                "The file may have been removed."
            )

        action_match = re.search(
            r'<form[^>]*id="dl-form"[^>]*action="([^"]+)"', html
        )
        if not action_match:
            raise ResolveError("krakenfiles: could not find download form")

        post_url = action_match.group(1)
        if not post_url.startswith("http"):
            post_url = f"https://krakenfiles.com{post_url}"

        resp = await session.post(
            post_url,
            data={"token": token_match.group(1)},
            headers={"Referer": url},
        )
        data = await resp.json()

        dl_url = data.get("url")
        if not dl_url:
            raise ResolveError(
                f"krakenfiles: download request failed: {data}"
            )

        return [ResolvedFile(url=dl_url)]


# ---------------------------------------------------------------------------
# send.cm
# ---------------------------------------------------------------------------

async def _resolve_sendcm(url: str) -> list[ResolvedFile]:
    """Resolve send.cm share links by scraping the download page."""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    ) as session:
        resp = await session.get(url, allow_redirects=True)
        html = await resp.text()

        match = re.search(
            r'href="(https?://[^"]*send\.cm/dl/[^"]+)"', html
        )
        if not match:
            match = re.search(
                r'<a[^>]*class="[^"]*download[^"]*"[^>]*href="([^"]+)"',
                html,
                re.IGNORECASE,
            )
        if not match:
            raise ResolveError(
                "send.cm: could not find download link on the page"
            )

        return [ResolvedFile(url=match.group(1))]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_RESOLVERS: list[tuple[tuple[str, ...], object]] = [
    (("gofile.io",), _resolve_gofile),
    (("mediafire.com",), _resolve_mediafire),
    (("pixeldrain.com",), _resolve_pixeldrain),
    (("krakenfiles.com",), _resolve_krakenfiles),
    (("send.cm",), _resolve_sendcm),
]

SUPPORTED_HOSTS: list[str] = []
for _patterns, _ in _RESOLVERS:
    SUPPORTED_HOSTS.extend(_patterns)


async def resolve_url(
    url: str, *, password: Optional[str] = None
) -> list[ResolvedFile]:
    """Resolve a URL to direct download link(s).

    If the URL belongs to a known file hosting service, the resolver
    extracts direct download URLs. Otherwise returns the URL as-is
    (assumed to be a direct download link).

    Args:
        url: The URL to resolve.
        password: Optional password for password-protected links (gofile).

    Returns:
        List of ResolvedFile with direct download URLs.
    """
    for patterns, resolver in _RESOLVERS:
        if _host_matches(url, *patterns):
            log.info("resolving %s via %s", url, resolver.__name__)
            if resolver is _resolve_gofile:
                return await resolver(url, password=password)
            return await resolver(url)

    return [ResolvedFile(url=url)]


def is_hosted_link(url: str) -> bool:
    """Check if a URL belongs to a supported file hosting service."""
    return any(_host_matches(url, *patterns) for patterns, _ in _RESOLVERS)
