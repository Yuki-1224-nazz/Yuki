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
- **Multi-connection parallel downloading** via HTTP Range requests
  (splits file into segments downloaded concurrently — up to 10x faster)
"""

from __future__ import annotations

import asyncio
import logging
import os
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
DEFAULT_CONNECTIONS = int(os.getenv("DOWNLOAD_CONNECTIONS", "8"))
MIN_SEGMENT_SIZE = 2 * 1024 * 1024  # 2 MB — no point splitting smaller


class DownloadError(RuntimeError):
    """Raised when the download fails for any reason."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


ProgressCallback = Callable[[int, Optional[int]], None]
"""``progress(bytes_read, total_bytes_or_None)``."""


# ---------------------------------------------------------------------------
# Async download (preferred path — used by the bot)
# ---------------------------------------------------------------------------

def _make_connector(num_connections: int) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=max(num_connections + 2, 10),
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )


def _make_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=None, connect=15, sock_read=120)


_COMMON_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "*/*",
    "Accept-Encoding": "identity",
    "Connection": "keep-alive",
}


async def _probe_range_support(
    session: aiohttp.ClientSession, url: str
) -> tuple[bool, Optional[int]]:
    """HEAD + small Range probe. Returns (supports_range, content_length)."""
    try:
        async with session.head(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                return False, None
            cl_raw = resp.headers.get("Content-Length")
            content_length = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
            accept_ranges = resp.headers.get("Accept-Ranges", "").lower()
            if accept_ranges == "bytes" and content_length:
                return True, content_length
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    # Fallback: try an actual Range request for the first byte
    try:
        async with session.get(
            url, headers={"Range": "bytes=0-0"}, allow_redirects=True
        ) as resp:
            if resp.status == 206:
                cr = resp.headers.get("Content-Range", "")
                # Content-Range: bytes 0-0/<total>
                if "/" in cr:
                    total_str = cr.rsplit("/", 1)[-1]
                    if total_str.isdigit():
                        return True, int(total_str)
                return True, None
            cl_raw = resp.headers.get("Content-Length")
            content_length = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
            return False, content_length
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return False, None


async def _download_segment(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    start: int,
    end: int,
    segment_id: int,
    progress: list[int],
    progress_lock: asyncio.Lock,
) -> int:
    """Download a byte-range segment to a temp file with retry."""
    seg_path = dest.parent / f"{dest.name}.part{segment_id}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            written = 0
            async with session.get(
                url,
                headers={"Range": f"bytes={start}-{end}"},
                allow_redirects=True,
            ) as resp:
                if resp.status not in (200, 206):
                    raise DownloadError(
                        f"HTTP {resp.status} for segment {segment_id}"
                    )
                with open(seg_path, "wb", buffering=1024 * 1024) as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        written += len(chunk)
                        async with progress_lock:
                            progress[segment_id] = written
            return written
        except DownloadError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
            else:
                raise DownloadError(
                    f"segment {segment_id} failed after {MAX_RETRIES} "
                    f"attempts: {exc}"
                ) from exc
    raise DownloadError(f"segment {segment_id} failed (unreachable)")


def _assemble_segments_sync(dest: Path, num_segments: int) -> int:
    """Concatenate segment files into the final destination (sync, for executor)."""
    total = 0
    with open(dest, "wb", buffering=4 * 1024 * 1024) as out:
        for i in range(num_segments):
            seg = dest.parent / f"{dest.name}.part{i}"
            with open(seg, "rb", buffering=4 * 1024 * 1024) as inp:
                while True:
                    buf = inp.read(4 * 1024 * 1024)
                    if not buf:
                        break
                    out.write(buf)
                    total += len(buf)
            seg.unlink(missing_ok=True)
    return total


async def _multi_conn_download(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    total_size: int,
    *,
    num_connections: int,
    max_bytes: Optional[int],
    on_progress: Optional[ProgressCallback],
    progress_interval: float,
) -> int:
    """Download using multiple parallel Range-request connections."""
    if max_bytes is not None and total_size > max_bytes:
        raise DownloadError(
            f"file is {total_size} bytes, larger than max ({max_bytes})"
        )

    seg_size = total_size // num_connections
    if seg_size < MIN_SEGMENT_SIZE:
        num_connections = max(1, total_size // MIN_SEGMENT_SIZE)
        seg_size = total_size // num_connections if num_connections > 0 else total_size
    if num_connections <= 1:
        num_connections = 1

    segments: list[tuple[int, int]] = []
    for i in range(num_connections):
        start = i * seg_size
        end = (i + 1) * seg_size - 1 if i < num_connections - 1 else total_size - 1
        segments.append((start, end))

    log.info(
        "multi-connection download: %d segments, %d bytes each, total %d",
        num_connections, seg_size, total_size,
    )

    progress: list[int] = [0] * num_connections
    progress_lock = asyncio.Lock()

    # Progress reporter task
    progress_done = asyncio.Event()

    async def _report_progress() -> None:
        last_emit = 0.0
        while not progress_done.is_set():
            await asyncio.sleep(progress_interval)
            if on_progress is not None:
                now = time.time()
                if now - last_emit >= progress_interval:
                    async with progress_lock:
                        total_read = sum(progress)
                    on_progress(total_read, total_size)
                    last_emit = now

    reporter = asyncio.create_task(_report_progress()) if on_progress else None

    try:
        tasks = [
            _download_segment(
                session, url, dest, start, end, i, progress, progress_lock,
            )
            for i, (start, end) in enumerate(segments)
        ]
        await asyncio.gather(*tasks)
    finally:
        progress_done.set()
        if reporter is not None:
            await reporter

    # Assemble segments into final file
    written = await asyncio.get_running_loop().run_in_executor(
        None, _assemble_segments_sync, dest, num_connections,
    )

    if on_progress is not None:
        on_progress(written, total_size)

    return written


async def _single_conn_download(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int],
    on_progress: Optional[ProgressCallback],
    progress_interval: float,
) -> int:
    """Single-connection fallback when Range is not supported."""
    async with session.get(url, allow_redirects=True) as resp:
        if resp.status >= 400:
            raise DownloadError(f"HTTP {resp.status} for {url}")

        total: Optional[int] = None
        cl = resp.headers.get("Content-Length")
        if cl and cl.isdigit():
            total = int(cl)
            if max_bytes is not None and total > max_bytes:
                raise DownloadError(
                    f"file is {total} bytes, larger than max ({max_bytes})"
                )

        written = 0
        last_emit = 0.0

        with open(dest, "wb", buffering=1024 * 1024) as f:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
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

    if on_progress is not None:
        on_progress(written, total)
    return written


async def async_download_to_file(
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    num_connections: int = DEFAULT_CONNECTIONS,
) -> int:
    """Async download with automatic multi-connection acceleration.

    When the server supports HTTP Range requests and the file is large
    enough, the download is split across *num_connections* parallel
    connections (default controlled by ``DOWNLOAD_CONNECTIONS`` env var,
    default 8). Falls back to a single connection otherwise.

    Returns bytes written. Raises DownloadError on failure.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    connector = _make_connector(num_connections)

    last_exc: Optional[Exception] = None
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with aiohttp.ClientSession(
                    connector=connector,
                    connector_owner=False,
                    timeout=_make_timeout(),
                    headers=_COMMON_HEADERS,
                ) as session:
                    supports_range, content_length = await _probe_range_support(
                        session, url
                    )

                    if (
                        supports_range
                        and content_length is not None
                        and content_length >= MIN_SEGMENT_SIZE
                        and num_connections > 1
                    ):
                        log.info(
                            "server supports Range — using %d connections "
                            "for %d bytes",
                            num_connections,
                            content_length,
                        )
                        return await _multi_conn_download(
                            session,
                            url,
                            dest,
                            content_length,
                            num_connections=num_connections,
                            max_bytes=max_bytes,
                            on_progress=on_progress,
                            progress_interval=progress_interval,
                        )

                    log.info(
                        "Range not supported or file too small — single "
                        "connection download"
                    )
                    return await _single_conn_download(
                        session,
                        url,
                        dest,
                        max_bytes=max_bytes,
                        on_progress=on_progress,
                        progress_interval=progress_interval,
                    )

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
    finally:
        await connector.close()

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
        raise DownloadError(f"network error: {exc}", retryable=True) from exc

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

        except DownloadError as exc:
            if not exc.retryable:
                raise
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
