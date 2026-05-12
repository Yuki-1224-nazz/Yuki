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
import os
import re
import shutil
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

# ULP (User Login Password) filename hints
ULP_FILENAME_HINTS: tuple[str, ...] = (
    "password",
    "passwords",
    "passwords.txt",
    "login",
    "logins",
    "credentials",
    "all passwords",
)

# Path-level hints: if ANY component of the full file path matches
# one of these, the file is a candidate for ULP scanning.
_ULP_PATH_HINTS: tuple[str, ...] = (
    "password",
    "passwords",
    "login",
    "logins",
    "credential",
    "credentials",
    "autologin",
    "all passwords",
)

# Regex patterns for extracting credentials (from v4.py)
# Ordered from most specific → least specific so the first match wins.
_ULP_PATTERNS = [
    re.compile(
        r"URL:\s*(https?://\S+)\s+USER:\s*(\S+)\s+PASS:\s*(\S+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"SOFT:\s*(?:.*?)\s*URL:\s*(\S+)\s*USER:\s*(\S+)\s*PASS:\s*(\S+)",
        re.IGNORECASE,
    ),
    re.compile(r"USER:\s*(\S+)\s*PASS:\s*(\S+)", re.IGNORECASE),
]

# Quick byte-level check — if a file doesn't contain these markers
# it can't match any of our patterns, so skip the regex entirely.
_ULP_QUICK_MARKERS = (b"URL:", b"url:", b"USER:", b"user:", b"PASS:", b"pass:")


def extract_credentials_from_text(
    text: str,
    kw_lowers: Optional[List[str]] = None,
    *,
    include_url: bool = False,
) -> List[str]:
    """Extract credentials from a log file block.

    When *include_url* is ``False`` (default), returns ``"user:pass"``
    strings.  When ``True``, returns ``"url:user:pass"`` strings (the
    URL is included when the pattern captures one; otherwise the URL
    part is omitted and the result is still ``"user:pass"``).

    When *kw_lowers* is given, only credentials whose **full match
    context** (including URL) contains at least one keyword are kept.
    """
    results: list[str] = []
    seen: set[str] = set()
    for pat in _ULP_PATTERNS:
        for m in pat.finditer(text):
            groups = m.groups()
            if include_url and len(groups) == 3:
                cred = f"{groups[0]}:{groups[1]}:{groups[2]}"
            else:
                cred = f"{groups[-2]}:{groups[-1]}"
            if cred in seen:
                continue
            if kw_lowers:
                context_low = m.group(0).lower()
                if not any(kw in context_low for kw in kw_lowers):
                    continue
            seen.add(cred)
            results.append(cred)
    return results

StatusCallback = Callable[[str], None]
"""``status(message)`` — called by the pipeline to report progress."""


@dataclass
class PipelineResult:
    zip_path: Path
    output_dir: Path
    cookie_files: List[Path] = field(default_factory=list)
    cookie_count: int = 0
    bytes_read: int = 0
    # ULP mode results
    ulp_credentials: set = field(default_factory=set)
    ulp_count: int = 0
    # Per-keyword ULP results (keyword -> set of credentials)
    ulp_per_keyword: dict = field(default_factory=dict)


def _safe_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "cookies"


def _fast_listdir(root: str) -> List[str]:
    """Recursively list file paths using os.scandir (much faster than rglob)."""
    result: List[str] = []
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        result.append(entry.path)
                    elif entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
        except OSError:
            pass
    return result


def _fast_listdir_ulp(root: str) -> tuple[List[str], int]:
    """List only ULP-candidate files, skipping irrelevant directories.

    Returns ``(ulp_files, total_file_count)``.
    Skips entire directory subtrees that cannot contain credentials
    (e.g. Cookies/, Autofill/, Screenshots/) for massive speedup.
    """
    ulp_files: List[str] = []
    total = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        total += 1
                        if _is_ulp_candidate(entry.path):
                            ulp_files.append(entry.path)
                    elif entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
        except OSError:
            pass
    return ulp_files, total


def _fast_file_count(root: str) -> int:
    """Count files using os.scandir (faster than rglob for progress)."""
    count = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if entry.is_file(follow_symlinks=False):
                        count += 1
                    elif entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
        except OSError:
            pass
    return count


_MAX_COOKIE_FILE_BYTES = 512 * 1024  # 512 KB — real cookies are small


def _scan_and_convert_one(
    args: tuple,
) -> Optional[tuple[Path, int, List[CookieRow]]]:
    """Combined scan + convert in ONE pass.

    Checks if a file is a cookie file and converts it in the same read.
    Returns (out_path, count, rows) or None.
    """
    idx, file_path_str, cookies_dir_str, extracted_str, kw_lowers = args
    name = os.path.basename(file_path_str)
    lname = name.lower()

    # Quick reject: not a .txt and no cookie hint in name
    is_hint = any(h in lname for h in COOKIE_FILENAME_HINTS)
    if not is_hint and not lname.endswith(".txt"):
        return None

    # Quick reject: skip large files (real cookie files are small)
    try:
        size = os.path.getsize(file_path_str)
        if size == 0 or size > _MAX_COOKIE_FILE_BYTES:
            return None
    except OSError:
        return None

    # Read the file once — used for both detection AND conversion
    try:
        fd = os.open(file_path_str, os.O_RDONLY)
        try:
            raw = os.read(fd, _MAX_COOKIE_FILE_BYTES)
        finally:
            os.close(fd)
    except OSError:
        return None

    text = raw.decode("utf-8", errors="replace")
    rows: List[CookieRow] = []

    for line in text.splitlines():
        if kw_lowers:
            low = line.lower()
            if not any(kw_lo in low for kw_lo in kw_lowers):
                continue
        row = parse_cookie_line(line)
        if row is not None:
            rows.append(row)

    if not rows:
        return None

    # Build label from path
    try:
        rel = os.path.relpath(file_path_str, extracted_str)
    except ValueError:
        rel = name
    parts = [
        re.sub(r"[^A-Za-z0-9._-]+", "_", p).strip("._") or "cookies"
        for p in rel.replace("\\", "/").split("/")
        if p not in (".", "")
    ]
    label = "__".join(parts) if parts else "cookies"
    if label.lower().endswith(".txt"):
        label = label[:-4]
    if len(label) > _MAX_LABEL_LEN:
        label = label[:_MAX_LABEL_LEN]

    out_name = f"{idx:04d}_{label}.txt"
    out_path = os.path.join(cookies_dir_str, out_name)

    # Write output using fast os.write
    header = (
        "# Netscape HTTP Cookie File\n"
        "# https://curl.se/docs/http-cookies.html\n"
        "# This is a generated file. Do not edit.\n\n"
    )
    parts_out = [header]
    for r in rows:
        parts_out.append(r.to_line())
        parts_out.append("\n")
    data = "".join(parts_out).encode("utf-8")
    fd = os.open(out_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)

    return Path(out_path), len(rows), rows


_MAX_ULP_FILE_BYTES = 1024 * 1024  # 1 MB — password files can be slightly larger


def _is_ulp_candidate(file_path_str: str) -> bool:
    """Fast check: is this file likely to contain credentials?

    Checks path components (directories + filename) against known hints.
    Much faster than opening + regex-scanning every .txt file.
    """
    low = file_path_str.lower()
    return any(h in low for h in _ULP_PATH_HINTS)


def _scan_and_extract_ulp(
    args: tuple,
) -> Optional[tuple[str, int, Optional[dict]]]:
    """Scan a file for ULP credentials.

    *args* is ``(idx, file_path_str, kw_lowers, include_url)``.
    When *include_url* is ``True`` the output format is
    ``url:user:pass``; otherwise ``user:pass``.

    Returns ``(credentials_text, count, per_kw_dict)`` or ``None``.
    *per_kw_dict* maps keyword→set[cred] when multiple keywords are given.
    """
    idx, file_path_str, kw_lowers, include_url = args

    try:
        size = os.path.getsize(file_path_str)
        if size == 0 or size > _MAX_ULP_FILE_BYTES:
            return None
    except OSError:
        return None

    try:
        fd = os.open(file_path_str, os.O_RDONLY)
        try:
            raw = os.read(fd, _MAX_ULP_FILE_BYTES)
        finally:
            os.close(fd)
    except OSError:
        return None

    # Quick byte-level pre-check: skip files that can't possibly
    # contain structured credentials (no USER:/PASS: markers).
    if not any(marker in raw for marker in _ULP_QUICK_MARKERS):
        return None

    text = raw.decode("utf-8", errors="replace")

    if not kw_lowers:
        creds = extract_credentials_from_text(text, include_url=include_url)
        if not creds:
            return None
        return "\n".join(creds), len(creds), None

    # With keywords: match each keyword against full URL context
    # and track which credentials belong to which keyword
    per_kw: dict[str, set[str]] = {}
    all_creds: set[str] = set()
    for kw in kw_lowers:
        matched = extract_credentials_from_text(
            text, kw_lowers=[kw], include_url=include_url,
        )
        if matched:
            per_kw[kw] = set(matched)
            all_creds.update(matched)

    if not all_creds:
        return None

    return "\n".join(all_creds), len(all_creds), per_kw


def _zip_results(zip_path: Path, files: Sequence[Path], root: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_STORED,
    ) as z:
        for p in files:
            z.write(p, arcname=p.relative_to(root).as_posix())


_MAX_LABEL_LEN = 80


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
    skip_zip: bool = False,
    mode: str = "cookie",
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

    # Precompute keyword lowercase list (used by both archive and plain paths)
    kw_lowers: List[str] = []
    if keywords:
        kw_lowers = [
            k.strip().lower() for k in keywords
            if k and k.strip()
        ]

    if kind is not None:
        dl_size_mb = download_path.stat().st_size / (1024 * 1024)
        dl_size_gb = dl_size_mb / 1024
        if dl_size_gb >= 1:
            status(
                f"⚙ Extracting {kind} archive ({dl_size_gb:.1f} GB)... "
                "this may take a while for large files"
            )
        else:
            status(
                f"⚙ Extracting {kind} archive ({dl_size_mb:.0f} MB)... "
                "this may take a few minutes for large files"
            )
        extracted = workdir / "extracted"

        # Scale timeout with archive size (min 600s, +300s per GB, no cap)
        extraction_timeout = max(600, int(600 + dl_size_gb * 300))

        # Check disk space: extraction can expand 2-5x the archive size
        try:
            import os as _os
            st = _os.statvfs(str(workdir))
            free_bytes = st.f_bavail * st.f_frsize
            needed = int(download_path.stat().st_size * 3)
            if free_bytes < needed:
                status(
                    f"⚠️ Low disk space: {free_bytes / (1024**3):.1f} GB free, "
                    f"may need ~{needed / (1024**3):.1f} GB for extraction"
                )
        except OSError:
            pass

        loop = asyncio.get_running_loop()
        extract_start = _time.time()
        extraction_future = loop.run_in_executor(
            None, lambda: extract_archive(
                download_path, extracted,
                password=password, timeout=extraction_timeout,
            )
        )

        # Show live progress updates while extraction runs
        _last_file_count = 0
        while not extraction_future.done():
            await asyncio.sleep(3)
            if extraction_future.done():
                break
            elapsed = int(_time.time() - extract_start)
            if extracted.exists():
                try:
                    _last_file_count = _fast_file_count(str(extracted))
                except OSError:
                    pass
            status(
                f"⚙ Extracting {kind} archive ({dl_size_mb:.0f} MB)...\n"
                f"📂 {_last_file_count:,} files extracted\n"
                f"⏱️ {elapsed}s elapsed"
            )

        # Await the result (re-raises any exception)
        await extraction_future

        # Free disk: delete the downloaded archive now that it's extracted
        try:
            download_path.unlink()
        except OSError:
            pass

        # Combined scan + convert in ONE pass (no separate scan step)
        convert_start = _time.time()

        if mode in ("ulp", "ulp_url"):
            include_url = mode == "ulp_url"
            # ULP mode: use specialized lister that filters during
            # traversal — never builds a 103K item list in memory.
            ulp_files, total_files = await loop.run_in_executor(
                None, lambda: _fast_listdir_ulp(str(extracted))
            )
            ulp_count = len(ulp_files)
            status(
                f"🔑 Found {ulp_count:,} credential files "
                f"(out of {total_files:,} total)... scanning"
            )

            ulp_workers = min(500, max(8, ulp_count // 10))
            work_items_ulp = [
                (i, fp, kw_lowers, include_url)
                for i, fp in enumerate(ulp_files, start=1)
            ]
            del ulp_files

            def _process_ulp_result(r: Optional[tuple]) -> None:
                if r is None:
                    return
                cred_text, count, per_kw = r
                for line in cred_text.splitlines():
                    stripped = line.strip()
                    if stripped:
                        result.ulp_credentials.add(stripped)
                if per_kw:
                    for kw, creds_set in per_kw.items():
                        if kw not in result.ulp_per_keyword:
                            result.ulp_per_keyword[kw] = set()
                        result.ulp_per_keyword[kw].update(creds_set)
                result.ulp_count = len(result.ulp_credentials)

            if len(work_items_ulp) <= 50:
                for item in work_items_ulp:
                    _process_ulp_result(_scan_and_extract_ulp(item))
            else:
                processed = 0
                last_status = _time.time()
                with ThreadPoolExecutor(max_workers=ulp_workers) as pool:
                    futs = [
                        loop.run_in_executor(
                            pool, _scan_and_extract_ulp, item,
                        )
                        for item in work_items_ulp
                    ]
                    for coro in asyncio.as_completed(futs):
                        _process_ulp_result(await coro)
                        processed += 1
                        now = _time.time()
                        if now - last_status >= 2:
                            last_status = now
                            elapsed_conv = now - convert_start
                            status(
                                f"🔑 Scanning... "
                                f"{processed:,}/{ulp_count:,} files "
                                f"({result.ulp_count:,} credentials, "
                                f"{elapsed_conv:.0f}s)"
                            )
        else:
            # --- Cookie mode: scan + convert cookies ---
            all_files = await loop.run_in_executor(
                None, lambda: _fast_listdir(str(extracted))
            )
            total_files = len(all_files)
            workers = min(500, max(8, total_files // 10))
            status(f"🔄 Processing {total_files:,} files...")
            cookies_dir_str = str(cookies_dir)
            extracted_str = str(extracted)

            work_items = [
                (i, fp, cookies_dir_str, extracted_str, kw_lowers)
                for i, fp in enumerate(all_files, start=1)
            ]
            del all_files

            if len(work_items) <= 50:
                for item in work_items:
                    r = _scan_and_convert_one(item)
                    if r is not None:
                        path, count, _rows = r
                        result.cookie_files.append(path)
                        result.cookie_count += count
            else:
                processed = 0
                last_status = _time.time()
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = [
                        loop.run_in_executor(
                            pool, _scan_and_convert_one, item,
                        )
                        for item in work_items
                    ]
                    for coro in asyncio.as_completed(futs):
                        r = await coro
                        if r is not None:
                            path, count, _rows = r
                            result.cookie_files.append(path)
                            result.cookie_count += count
                        processed += 1
                        now = _time.time()
                        if now - last_status >= 2:
                            last_status = now
                            elapsed_conv = now - convert_start
                            status(
                                f"🔄 Converting... "
                                f"{processed:,}/{total_files:,} files "
                                f"({result.cookie_count:,} cookies, "
                                f"{elapsed_conv:.0f}s)"
                            )

        # Clean up extracted files to free disk
        try:
            shutil.rmtree(extracted, ignore_errors=True)
        except OSError:
            pass
    else:
        if mode in ("ulp", "ulp_url"):
            include_url = mode == "ulp_url"
            status("⚙ Processing... (parsing as plain text for credentials)")
            try:
                with open(download_path, "r", encoding="utf-8", errors="replace") as f:
                    text = f.read()
                creds = extract_credentials_from_text(
                    text, kw_lowers=kw_lowers or None,
                    include_url=include_url,
                )
                for c in creds:
                    result.ulp_credentials.add(c)
                result.ulp_count = len(result.ulp_credentials)
            except Exception:
                pass
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

    # 4. Zip results (skip if caller handles zipping)
    if not skip_zip:
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

        status("⚙ Processing... (scanning & converting)")
        all_files = _fast_listdir(str(extracted))
        kw_lowers: List[str] = []
        if keywords:
            kw_lowers = [
                k.strip().lower() for k in keywords if k and k.strip()
            ]
        cookies_dir_str = str(cookies_dir)
        extracted_str = str(extracted)
        for i, fp in enumerate(all_files, start=1):
            r = _scan_and_convert_one(
                (i, fp, cookies_dir_str, extracted_str, kw_lowers)
            )
            if r is not None:
                path, count, _rows = r
                result.cookie_files.append(path)
                result.cookie_count += count
        if not result.cookie_files:
            status("⚙ Processing... (no cookie files found)")
        else:
            status(f"🔄 Converting... ({len(result.cookie_files)} cookie set(s))")
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
