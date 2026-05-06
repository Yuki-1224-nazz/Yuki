"""Chunked HTTP download helpers.

The bot streams the user-provided URL in large chunks so multi-GB log
archives never need to fit in RAM. Supports both async (aiohttp) and
sync (requests) download paths. The async path is preferred for speed
as it avoids blocking the event loop and supports concurrent I/O.

Speed optimizations over the original implementation:
- Chunk size increased from 64 KB to 512 KB for better throughput
- Async aiohttp downloads with TCP connection reuse
- Automatic retry with exponential backoff for transient failures
- Optimized file I/O with larger write buffers
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Iterator, Optional

import aiohttp
import requests

log = logging.getLogger(__name__)

CHUNK_SIZE = 512 * 1024  # 512 KB — 8x larger for better throughput
DEFAULT_TIMEOUT = (15, 600)  # (connect, read) — faster connect timeout
DEFAULT_USER_AGENT = (
    "logs-to-cookie/2.1 (+https://github.com/Yuki-1224-nazz/Yuki)"
)
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.5  # seconds


class DownloadError(RuntimeError):
    """Raised when the download fails for any reason."""


ProgressCallback = Callable[[int, Optional[int]], None]
"""``progress(bytes_read, total_bytes_or_None)``."""


# ---------------------------------------------------------------------------
# Async download (preferred path — used by the bot)
# ---------------------------------------------------------------------------

async def async_download_to_file(
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
) -> int:
    """Async download using aiohttp — much faster than blocking requests.

    Uses 512 KB chunks, TCP keepalive, and automatic retries.
    Returns bytes written. Raises DownloadError on failure.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(
        total=None,
        connect=15,
        sock_read=120,
    )
    connector = aiohttp.TCPConnector(
        limit=4,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": "*/*",
                    "Accept-Encoding": "identity",
                    "Connection": "keep-alive",
                },
            ) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status >= 400:
                        raise DownloadError(
                            f"HTTP {resp.status} for {url}"
                        )

                    total: Optional[int] = None
                    cl = resp.headers.get("Content-Length")
                    if cl and cl.isdigit():
                        total = int(cl)
                        if max_bytes is not None and total > max_bytes:
                            raise DownloadError(
                                f"file is {total} bytes, larger than "
                                f"max ({max_bytes})"
                            )

                    written = 0
                    last_emit = 0.0

                    with open(dest, "wb", buffering=1024 * 1024) as f:
                        async for chunk in resp.content.iter_chunked(
                            CHUNK_SIZE
                        ):
                            if not chunk:
                                continue
                            f.write(chunk)
                            written += len(chunk)

                            if max_bytes is not None and written > max_bytes:
                                raise DownloadError(
                                    f"download exceeded max_bytes "
                                    f"({max_bytes})"
                                )

                            if on_progress is not None:
                                now = time.time()
                                if now - last_emit >= progress_interval:
                                    on_progress(written, total)
                                    last_emit = now

            if on_progress is not None:
                on_progress(written, total)
            return written

        except DownloadError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "download attempt %d/%d failed (%s), "
                    "retrying in %.1fs...",
                    attempt,
                    MAX_RETRIES,
                    exc,
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                raise DownloadError(
                    f"download failed after {MAX_RETRIES} attempts: "
                    f"{last_exc}"
                ) from last_exc

    raise DownloadError("download failed (unreachable)")


# ---------------------------------------------------------------------------
# Sync download (fallback — used by tests and non-async callers)
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_USER_AGENT, "Accept": "*/*"})
    return s


def _open_stream(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: tuple = DEFAULT_TIMEOUT,
) -> requests.Response:
    sess = session or _build_session()
    try:
        resp = sess.get(url, stream=True, timeout=timeout, allow_redirects=True)
    except requests.RequestException as exc:
        raise DownloadError(f"network error: {exc}") from exc

    if resp.status_code >= 400:
        resp.close()
        raise DownloadError(f"HTTP {resp.status_code} for {url}")
    return resp


def download_to_file(
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    session: Optional[requests.Session] = None,
) -> int:
    """Stream ``url`` to ``dest`` in 512 KB chunks.

    Returns the number of bytes written. Raises :class:`DownloadError`
    on transport failures or if the response exceeds ``max_bytes``.

    Includes retry logic for transient network errors.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _open_stream(url, session=session)

            total: Optional[int] = None
            cl = resp.headers.get("Content-Length")
            if cl and cl.isdigit():
                total = int(cl)
                if max_bytes is not None and total > max_bytes:
                    resp.close()
                    raise DownloadError(
                        f"file is {total} bytes, larger than max ({max_bytes})"
                    )

            written = 0
            last_emit = 0.0
            try:
                with open(dest, "wb", buffering=1024 * 1024) as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        written += len(chunk)
                        if max_bytes is not None and written > max_bytes:
                            raise DownloadError(
                                f"download exceeded max_bytes ({max_bytes})"
                            )
                        if on_progress is not None:
                            now = time.time()
                            if now - last_emit >= progress_interval:
                                on_progress(written, total)
                                last_emit = now
            finally:
                resp.close()

            if on_progress is not None:
                on_progress(written, total)
            return written

        except DownloadError:
            raise
        except (requests.RequestException, OSError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "download attempt %d/%d failed (%s), "
                    "retrying in %.1fs...",
                    attempt,
                    MAX_RETRIES,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                raise DownloadError(
                    f"download failed after {MAX_RETRIES} attempts: "
                    f"{last_exc}"
                ) from last_exc

    raise DownloadError("download failed (unreachable)")


def stream_lines(
    url: str,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    session: Optional[requests.Session] = None,
) -> Iterator[str]:
    """Yield lines from ``url`` without ever buffering the full body.

    Bytes are decoded as UTF-8 with errors replaced. The helper stitches
    partial lines across chunk boundaries so a cookie row split across
    two TCP packets is never lost.
    """
    resp = _open_stream(url, session=session)
    total: Optional[int] = None
    cl = resp.headers.get("Content-Length")
    if cl and cl.isdigit():
        total = int(cl)
        if max_bytes is not None and total > max_bytes:
            resp.close()
            raise DownloadError(
                f"file is {total} bytes, larger than max ({max_bytes})"
            )

    bytes_read = 0
    last_emit = 0.0
    pending = ""
    try:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            if not chunk:
                continue
            bytes_read += len(chunk)
            if max_bytes is not None and bytes_read > max_bytes:
                raise DownloadError(
                    f"download exceeded max_bytes ({max_bytes})"
                )
            if on_progress is not None:
                now = time.time()
                if now - last_emit >= progress_interval:
                    on_progress(bytes_read, total)
                    last_emit = now

            text = chunk.decode("utf-8", errors="replace")
            if pending:
                text = pending + text
                pending = ""

            if not text.endswith("\n"):
                idx = text.rfind("\n")
                if idx == -1:
                    pending = text
                    continue
                pending = text[idx + 1:]
                text = text[: idx + 1]

            for line in text.splitlines():
                yield line

        if pending:
            yield pending
    finally:
        resp.close()
        if on_progress is not None:
            on_progress(bytes_read, total)
