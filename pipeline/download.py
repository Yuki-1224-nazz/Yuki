"""Chunked HTTP download helpers.

The bot streams the user-provided URL in 64 KB chunks so multi-GB log
archives never need to fit in RAM. The same primitives are used both
for plain-text Netscape cookie URLs (``stream_lines``) and for archive
URLs that have to be saved to disk before extraction (``download_to_file``).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable, Iterator, Optional

import requests

log = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024  # 64 KB
DEFAULT_TIMEOUT = (30, 600)  # (connect, read)
DEFAULT_USER_AGENT = "logs-to-cookie/2.0 (+https://github.com/zyblahblah/logs-to-cookie)"


class DownloadError(RuntimeError):
    """Raised when the download fails for any reason."""


ProgressCallback = Callable[[int, Optional[int]], None]
"""``progress(bytes_read, total_bytes_or_None)``."""


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
    progress_interval: float = 1.0,
    session: Optional[requests.Session] = None,
) -> int:
    """Stream ``url`` to ``dest`` in 64 KB chunks.

    Returns the number of bytes written. Raises :class:`DownloadError`
    on transport failures or if the response exceeds ``max_bytes``.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
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
        with open(dest, "wb") as f:
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


def stream_lines(
    url: str,
    *,
    max_bytes: Optional[int] = None,
    on_progress: Optional[ProgressCallback] = None,
    progress_interval: float = 1.0,
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

            # Hold on to a trailing partial line until the next chunk.
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
