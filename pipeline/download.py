"""Chunked HTTP download helpers.

The bot streams the user-provided URL in large chunks so multi-GB log
archives never need to fit in RAM. Supports both async (aiohttp) and
sync (requests) download paths. The async path is preferred for speed
as it avoids blocking the event loop and supports concurrent I/O.

Speed & reliability optimizations:
- Multi-connection parallel download (splits file into segments)
- Chunk size of 1 MB for maximum throughput
- Async aiohttp downloads with TCP connection reuse
- Automatic retry with exponential backoff and resume support
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

CHUNK_SIZE = 1024 * 1024  # 1 MB for maximum throughput
DEFAULT_TIMEOUT = (15, 600)  # (connect, read)
DEFAULT_USER_AGENT = (
    "logs-to-cookie/2.2 (+https://github.com/Yuki-1224-nazz/Yuki)"
)
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2.0  # seconds
PARALLEL_CONNECTIONS = 8
PARALLEL_MIN_FILE_SIZE = 10 * 1024 * 1024  # 10 MB — below this, single connection is fine


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

def _base_headers(extra_headers: Optional[dict[str, str]] = None) -> dict[str, str]:
    headers: dict[str, str] = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _make_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=None, connect=15, sock_read=300)


def _make_connector(limit: int = 16) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=limit,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )


async def _probe_file(
    url: str,
    headers: dict[str, str],
    timeout: aiohttp.ClientTimeout,
    connector: aiohttp.TCPConnector,
) -> tuple[Optional[int], bool]:
    """HEAD probe to get file size and check Range support."""
    async with aiohttp.ClientSession(
        connector=connector,
        connector_owner=False,
        timeout=timeout,
        headers=headers,
    ) as session:
        async with session.head(url, allow_redirects=True) as resp:
            if resp.status >= 400:
                raise DownloadError(f"HTTP {resp.status} for {url}")
            cl = resp.headers.get("Content-Length")
            file_size = int(cl) if cl and cl.isdigit() else None
            accept_ranges = resp.headers.get("Accept-Ranges", "").lower()
            supports_range = accept_ranges == "bytes"
            return file_size, supports_range


async def _download_segment(
    url: str,
    dest: Path,
    start: int,
    end: int,
    segment_idx: int,
    headers: dict[str, str],
    timeout: aiohttp.ClientTimeout,
    connector: aiohttp.TCPConnector,
    progress: dict[int, int],
    progress_lock: asyncio.Lock,
) -> int:
    """Download a byte range [start, end] to a segment file with retries."""
    seg_file = dest.parent / f"{dest.name}.part{segment_idx}"
    written = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req_headers = dict(headers)
            req_headers["Range"] = f"bytes={start + written}-{end}"

            async with aiohttp.ClientSession(
                connector=connector,
                connector_owner=False,
                timeout=timeout,
                headers=req_headers,
            ) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status == 416:
                        break
                    if resp.status not in (200, 206):
                        raise DownloadError(
                            f"segment {segment_idx}: HTTP {resp.status}"
                        )

                    mode = "ab" if written > 0 else "wb"
                    with open(seg_file, mode, buffering=2 * 1024 * 1024) as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            written += len(chunk)
                            async with progress_lock:
                                progress[segment_idx] = written

            return written

        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "segment %d attempt %d/%d failed at %d bytes (%s), "
                    "retrying in %.1fs...",
                    segment_idx, attempt, MAX_RETRIES, written, exc, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise DownloadError(
                    f"segment {segment_idx} failed after {MAX_RETRIES} "
                    f"attempts ({written} bytes): {exc}"
                ) from exc

    return written


async def _parallel_download(
    url: str,
    dest: Path,
    file_size: int,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    extra_headers: Optional[dict[str, str]] = None,
) -> int:
    """Download file using multiple parallel connections."""
    if max_bytes is not None and file_size > max_bytes:
        raise DownloadError(
            f"file is {file_size} bytes, larger than max ({max_bytes})"
        )

    n_conn = min(PARALLEL_CONNECTIONS, max(1, file_size // (5 * 1024 * 1024)))
    seg_size = file_size // n_conn

    segments: list[tuple[int, int]] = []
    for i in range(n_conn):
        seg_start = i * seg_size
        seg_end = file_size - 1 if i == n_conn - 1 else (i + 1) * seg_size - 1
        segments.append((seg_start, seg_end))

    log.info(
        "parallel download: %d segments, %d bytes each, %d total",
        n_conn, seg_size, file_size,
    )

    headers = _base_headers(extra_headers)
    timeout = _make_timeout()
    connector = _make_connector(limit=n_conn + 2)

    progress_map: dict[int, int] = {i: 0 for i in range(n_conn)}
    progress_lock = asyncio.Lock()

    async def _progress_reporter() -> None:
        if on_progress is None:
            return
        while True:
            await asyncio.sleep(progress_interval)
            async with progress_lock:
                total_written = sum(progress_map.values())
            on_progress(total_written, file_size)

    reporter_task: Optional[asyncio.Task[None]] = None
    try:
        if on_progress is not None:
            reporter_task = asyncio.create_task(_progress_reporter())

        tasks = [
            _download_segment(
                url, dest, seg_start, seg_end, idx,
                headers, timeout, connector, progress_map, progress_lock,
            )
            for idx, (seg_start, seg_end) in enumerate(segments)
        ]
        results = await asyncio.gather(*tasks)

        total_written = sum(results)

        with open(dest, "wb", buffering=2 * 1024 * 1024) as out:
            for idx in range(n_conn):
                seg_file = dest.parent / f"{dest.name}.part{idx}"
                if seg_file.exists():
                    with open(seg_file, "rb") as sf:
                        while True:
                            chunk = sf.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            out.write(chunk)
                    seg_file.unlink()

        if on_progress is not None:
            on_progress(total_written, file_size)
        return total_written

    finally:
        if reporter_task is not None:
            reporter_task.cancel()
            try:
                await reporter_task
            except asyncio.CancelledError:
                pass
        await connector.close()
        for idx in range(n_conn):
            seg_file = dest.parent / f"{dest.name}.part{idx}"
            if seg_file.exists():
                seg_file.unlink(missing_ok=True)


async def _single_download(
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    extra_headers: Optional[dict[str, str]] = None,
) -> int:
    """Single-connection download with resume support."""
    timeout = _make_timeout()
    connector = _make_connector(limit=8)

    last_exc: Optional[Exception] = None
    written = 0
    total: Optional[int] = None

    try:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                req_headers = _base_headers(extra_headers)

                if written > 0:
                    req_headers["Range"] = f"bytes={written}-"

                async with aiohttp.ClientSession(
                    connector=connector,
                    connector_owner=False,
                    timeout=timeout,
                    headers=req_headers,
                ) as session:
                    async with session.get(url, allow_redirects=True) as resp:
                        if resp.status == 416:
                            log.info("range not satisfiable — file already complete")
                            if on_progress is not None:
                                on_progress(written, total)
                            return written

                        if written > 0 and resp.status != 206:
                            log.warning(
                                "server does not support Range (got %d), "
                                "restarting from scratch",
                                resp.status,
                            )
                            written = 0

                        if resp.status >= 400:
                            raise DownloadError(
                                f"HTTP {resp.status} for {url}"
                            )

                        cl = resp.headers.get("Content-Length")
                        if cl and cl.isdigit():
                            chunk_total = int(cl)
                            if resp.status == 206:
                                total = written + chunk_total
                            else:
                                total = chunk_total
                            if max_bytes is not None and total > max_bytes:
                                raise DownloadError(
                                    f"file is {total} bytes, larger than "
                                    f"max ({max_bytes})"
                                )

                        last_emit = 0.0
                        mode = "ab" if written > 0 else "wb"

                        with open(dest, mode, buffering=2 * 1024 * 1024) as f:
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
                        "download attempt %d/%d failed at %d bytes (%s), "
                        "retrying in %.1fs...",
                        attempt,
                        MAX_RETRIES,
                        written,
                        exc,
                        wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise DownloadError(
                        f"download failed after {MAX_RETRIES} attempts "
                        f"({written} bytes received): {last_exc}"
                    ) from last_exc
    finally:
        await connector.close()

    raise DownloadError("download failed (unreachable)")


async def async_download_to_file(
    url: str,
    dest: Path,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 0.5,
    extra_headers: Optional[dict[str, str]] = None,
) -> int:
    """Async download using aiohttp for maximum speed.

    Automatically uses multi-connection parallel download when the
    server supports Range requests and the file is large enough.
    Falls back to single-connection download with resume support.

    Returns bytes written. Raises DownloadError on failure.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    timeout = _make_timeout()
    connector = _make_connector(limit=4)
    try:
        try:
            file_size, supports_range = await _probe_file(
                url, _base_headers(extra_headers), timeout, connector,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            file_size, supports_range = None, False
    finally:
        await connector.close()

    if (
        supports_range
        and file_size is not None
        and file_size >= PARALLEL_MIN_FILE_SIZE
    ):
        log.info(
            "using parallel download (%d connections) for %d byte file",
            min(PARALLEL_CONNECTIONS, max(1, file_size // (5 * 1024 * 1024))),
            file_size,
        )
        return await _parallel_download(
            url, dest,
            file_size,
            max_bytes=max_bytes,
            on_progress=on_progress,
            progress_interval=progress_interval,
            extra_headers=extra_headers,
        )

    log.info("using single-connection download")
    return await _single_download(
        url, dest,
        max_bytes=max_bytes,
        on_progress=on_progress,
        progress_interval=progress_interval,
        extra_headers=extra_headers,
    )


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
    """Stream ``url`` to ``dest`` in 1 MB chunks.

    Returns the number of bytes written. Raises :class:`DownloadError`
    on transport failures or if the response exceeds ``max_bytes``.

    Only retries connection-phase errors; once data starts streaming,
    failures are raised immediately to avoid re-downloading.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        written = 0
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

            last_emit = 0.0
            try:
                with open(dest, "wb", buffering=2 * 1024 * 1024) as f:
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
                    "download attempt %d/%d failed at %d bytes (%s), "
                    "retrying in %.1fs...",
                    attempt,
                    MAX_RETRIES,
                    written,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                raise DownloadError(
                    f"download failed after {MAX_RETRIES} attempts "
                    f"({written} bytes received): {last_exc}"
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
