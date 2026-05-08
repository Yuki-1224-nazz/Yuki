"""End-to-end pipeline that ties download + extract + cookie parsing.

The flow mirrors the diagram in the README:

    /start
       └─► download link  (chunked stream, never buffered to disk)
             └─► password? (only used for encrypted archives)
                  └─► keywords?  (optional case-insensitive filter)
                       └─► parse + extract cookies
                            └─► one Netscape file per "cookie set"
                                 └─► zip → uploaded by the bot

Performance improvements:
- Async download path via aiohttp for non-blocking I/O
- Concurrent cookie file processing for archives with many files
- Faster zip compression (level 1 for speed)
"""

from __future__ import annotations

import asyncio
import logging
import re
import time as _time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from .archive import (
    ARCHIVE_SUFFIXES,
    detect_archive_kind,
    extract_archive,
)
from .cookies import (
    CookieRow,
    iter_cookies_from_lines,
    parse_cookie_line,
    write_netscape_file,
)
from .download import (
    ProgressCallback,
    async_download_to_file,
    download_to_file,
)
from .gofile import GofileError, is_gofile_url, resolve_gofile_url

log = logging.getLogger(__name__)

COOKIE_FILENAME_HINTS: tuple[str, ...] = (
    "cookie",
    "cookies",
    "cookie.txt",
    "cookies.txt",
    "passwords-cookies",
)

StatusCallback = Callable[[str], None]
"""``status(message)`` — called by the pipeline to report progress."""


@dataclass
class PipelineResult:
    zip_path: Path
    output_dir: Path
    cookie_files: List[Path] = field(default_factory=list)
    cookie_count: int = 0
    bytes_read: int = 0


def _safe_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "cookies"


def _find_cookie_files(root: Path) -> List[Path]:
    out: List[Path] = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        lname = p.name.lower()
        if any(h in lname for h in COOKIE_FILENAME_HINTS):
            out.append(p)
            continue
        if lname.endswith(".txt"):
            try:
                with open(p, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        if not line.strip() or line.lstrip().startswith("#"):
                            continue
                        if parse_cookie_line(line) is not None:
                            out.append(p)
                        break
            except OSError:
                continue
    return out


def _extract_set(
    src: Path,
    out_path: Path,
    keywords: Optional[Sequence[str]],
) -> int:
    """Read one source cookies file and write one Netscape file out."""
    rows: List[CookieRow] = []
    with open(src, "r", encoding="utf-8", errors="replace") as f:
        for row in iter_cookies_from_lines(f, keywords=keywords):
            rows.append(row)
    if not rows:
        return 0
    return write_netscape_file(out_path, rows)


def _zip_results(zip_path: Path, files: Sequence[Path], root: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=1,
    ) as z:
        for p in files:
            z.write(p, arcname=p.relative_to(root).as_posix())


def _label_for(src: Path, archive_root: Path) -> str:
    """Build a human-readable filename from the source path inside the archive."""
    try:
        rel = src.relative_to(archive_root)
    except ValueError:
        rel = Path(src.name)
    parts = [_safe_name(p) for p in rel.parts if p not in (".", "")]
    if not parts:
        return _safe_name(src.stem)
    label = "__".join(parts)
    if label.lower().endswith(".txt"):
        label = label[:-4]
    return label or _safe_name(src.stem)


def _process_cookie_source(
    i: int,
    src: Path,
    cookies_dir: Path,
    extracted: Path,
    keywords: Optional[Sequence[str]],
) -> tuple[Optional[Path], int]:
    """Process a single cookie source file. Returns (path, count) or (None, 0)."""
    label = _label_for(src, extracted)
    out_path = cookies_dir / f"{i:04d}_{label}.txt"
    n = _extract_set(src, out_path, keywords)
    if n:
        return out_path, n
    try:
        out_path.unlink()
    except OSError:
        pass
    return None, 0


async def async_run_pipeline(
    url: str,
    workdir: Path,
    *,
    password: Optional[str] = None,
    keywords: Optional[Sequence[str]] = None,
    max_bytes: Optional[int] = None,
    on_status: Optional[StatusCallback] = None,
    on_progress: Optional[ProgressCallback] = None,
    num_connections: int = 8,
) -> PipelineResult:
    """Async version of run_pipeline — uses aiohttp for faster downloads.

    This is the preferred entry point when called from an async context
    (e.g. the Telegram bot handler).
    """
    workdir.mkdir(parents=True, exist_ok=True)
    output_dir = workdir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    cookies_dir = output_dir / "cookies"
    cookies_dir.mkdir(parents=True, exist_ok=True)

    def status(msg: str) -> None:
        log.info("status: %s", msg)
        if on_status is not None:
            try:
                on_status(msg)
            except Exception:
                log.exception("status callback raised")

    result = PipelineResult(
        zip_path=output_dir / "cookies_result.zip",
        output_dir=output_dir,
    )

    # 1. Resolve URL (gofile.io needs API calls to get direct links)
    download_url = url
    extra_headers: Optional[dict[str, str]] = None

    if is_gofile_url(url):
        status("🔗 Resolving gofile.io link...")
        try:
            gf_files = await resolve_gofile_url(url, password=password)
        except GofileError as exc:
            raise RuntimeError(f"gofile resolution failed: {exc}") from exc
        # Use the first file (or the largest one for multi-file shares)
        gf = max(gf_files, key=lambda f: f.size)
        download_url = gf.url
        extra_headers = {"Cookie": f"accountToken={gf.token}"}
        log.info("gofile resolved: %s → %s (%d bytes)", url, gf.name, gf.size)
        status(f"⏳ Downloading {gf.name}...")

    # 2. Download (async, chunked)
    if extra_headers is None:
        status("⏳ Downloading...")
    suffix = next(
        (s for s in ARCHIVE_SUFFIXES if download_url.lower().endswith(s)),
        "",
    )
    if not suffix and is_gofile_url(url):
        # Gofile URLs don't have file extensions in the path
        gf_name = gf.name if is_gofile_url(url) else ""
        suffix = next(
            (s for s in ARCHIVE_SUFFIXES if gf_name.lower().endswith(s)),
            "",
        )
    download_path = workdir / f"input{suffix or '.bin'}"
    bytes_read = await async_download_to_file(
        download_url,
        download_path,
        max_bytes=max_bytes,
        on_progress=on_progress,
        num_connections=num_connections,
        extra_headers=extra_headers,
    )
    result.bytes_read = bytes_read

    # 3. Detect archive type
    kind = detect_archive_kind(download_path)

    if kind is not None:
        dl_size_mb = download_path.stat().st_size / (1024 * 1024)
        status(
            f"⚙ Extracting {kind} archive ({dl_size_mb:.0f} MB)... "
            "this may take a few minutes for large files"
        )
        extracted = workdir / "extracted"

        # Scale timeout with archive size (min 600s, +60s per 100 MB)
        extraction_timeout = max(600, int(600 + (dl_size_mb / 100) * 60))

        loop = asyncio.get_running_loop()
        extract_start = _time.time()
        extraction_future = loop.run_in_executor(
            None, lambda: extract_archive(
                download_path, extracted,
                password=password, timeout=extraction_timeout,
            )
        )

        # Show live progress updates while extraction runs
        while not extraction_future.done():
            await asyncio.sleep(8)
            if extraction_future.done():
                break
            elapsed = int(_time.time() - extract_start)
            file_count = 0
            if extracted.exists():
                try:
                    file_count = sum(
                        1 for p in extracted.rglob("*") if p.is_file()
                    )
                except OSError:
                    pass
            status(
                f"⚙ Extracting {kind} archive ({dl_size_mb:.0f} MB)...\n"
                f"📂 {file_count:,} files extracted\n"
                f"⏱️ {elapsed}s elapsed"
            )

        # Await the result (re-raises any exception)
        await extraction_future

        status("⚙ Processing... (scanning extracted files)")
        sources = await loop.run_in_executor(None, lambda: _find_cookie_files(extracted))

        if not sources:
            status("⚙ Processing... (no cookie files found)")
        else:
            status(f"🔄 Converting... ({len(sources)} cookie set(s))")

            # Process cookie files concurrently for large archives
            if len(sources) > 10:
                workers = min(8, max(4, len(sources) // 100))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = []
                    for i, src in enumerate(sources, start=1):
                        futures.append(
                            loop.run_in_executor(
                                pool,
                                _process_cookie_source,
                                i, src, cookies_dir, extracted, keywords,
                            )
                        )
                    results = await asyncio.gather(*futures)
                    for path, count in results:
                        if path is not None:
                            result.cookie_files.append(path)
                            result.cookie_count += count
            else:
                for i, src in enumerate(sources, start=1):
                    path, count = _process_cookie_source(
                        i, src, cookies_dir, extracted, keywords
                    )
                    if path is not None:
                        result.cookie_files.append(path)
                        result.cookie_count += count
    else:
        status("⚙ Processing... (parsing as plain cookie file)")
        rows: List[CookieRow] = []
        with open(download_path, "r", encoding="utf-8", errors="replace") as f:
            for row in iter_cookies_from_lines(f, keywords=keywords):
                rows.append(row)

        if rows:
            status(f"🔄 Converting... (1 cookie set, {len(rows)} cookies)")
            out_path = cookies_dir / "0001_cookies.txt"
            write_netscape_file(out_path, rows)
            result.cookie_files.append(out_path)
            result.cookie_count = len(rows)

    # 4. Zip results
    if result.cookie_files:
        status(f"⚙ Processing... (packaging {len(result.cookie_files)} file(s))")
        _zip_results(result.zip_path, result.cookie_files, output_dir)
    else:
        _zip_results(result.zip_path, [], output_dir)

    return result


def run_pipeline(
    url: str,
    workdir: Path,
    *,
    password: Optional[str] = None,
    keywords: Optional[Sequence[str]] = None,
    max_bytes: Optional[int] = None,
    on_status: Optional[StatusCallback] = None,
    on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
) -> PipelineResult:
    """Run the full download → extract → convert pipeline (sync version).

    Always materialises ``cookies_result.zip`` inside ``workdir/output/``.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    output_dir = workdir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    cookies_dir = output_dir / "cookies"
    cookies_dir.mkdir(parents=True, exist_ok=True)

    def status(msg: str) -> None:
        log.info("status: %s", msg)
        if on_status is not None:
            try:
                on_status(msg)
            except Exception:
                log.exception("status callback raised")

    result = PipelineResult(
        zip_path=output_dir / "cookies_result.zip",
        output_dir=output_dir,
    )

    # 1. Download (chunked, to disk)
    status("⏳ Downloading...")
    suffix = next(
        (s for s in ARCHIVE_SUFFIXES if url.lower().endswith(s)),
        "",
    )
    download_path = workdir / f"input{suffix or '.bin'}"
    bytes_read = download_to_file(
        url,
        download_path,
        max_bytes=max_bytes,
        on_progress=on_progress,
    )
    result.bytes_read = bytes_read

    # 2. Decide what we actually got: archive vs plain Netscape file.
    kind = detect_archive_kind(download_path)

    if kind is not None:
        status(f"⚙ Processing... (extracting {kind} archive)")
        extracted = workdir / "extracted"
        extract_archive(download_path, extracted, password=password)

        status("⚙ Processing... (scanning extracted files)")
        sources = _find_cookie_files(extracted)
        if not sources:
            status("⚙ Processing... (no cookie files found)")
        else:
            status(f"🔄 Converting... ({len(sources)} cookie set(s))")

        for i, src in enumerate(sources, start=1):
            path, count = _process_cookie_source(
                i, src, cookies_dir, extracted, keywords
            )
            if path is not None:
                result.cookie_files.append(path)
                result.cookie_count += count
    else:
        status("⚙ Processing... (parsing as plain cookie file)")
        rows: List[CookieRow] = []
        with open(download_path, "r", encoding="utf-8", errors="replace") as f:
            for row in iter_cookies_from_lines(f, keywords=keywords):
                rows.append(row)

        if rows:
            status(f"🔄 Converting... (1 cookie set, {len(rows)} cookies)")
            out_path = cookies_dir / "0001_cookies.txt"
            write_netscape_file(out_path, rows)
            result.cookie_files.append(out_path)
            result.cookie_count = len(rows)

    # 4. Zip everything up
    if result.cookie_files:
        status(f"⚙ Processing... (packaging {len(result.cookie_files)} file(s))")
        _zip_results(result.zip_path, result.cookie_files, output_dir)
    else:
        _zip_results(result.zip_path, [], output_dir)

    return result
