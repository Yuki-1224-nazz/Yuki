"""End-to-end pipeline that ties download + extract + cookie parsing.

The flow mirrors the diagram in the README:

    /start
       └─► download link  (chunked stream, never buffered to disk)
             └─► password? (only used for encrypted archives)
                  └─► keywords?  (optional case-insensitive filter)
                       └─► parse + extract cookies
                            └─► one Netscape file per "cookie set"
                                 └─► zip → uploaded by the bot
"""

from __future__ import annotations

import logging
import re
import zipfile
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
from .download import download_to_file

log = logging.getLogger(__name__)

# A "cookie set" is a single source of cookies — one log victim, one
# domain, one stored cookies.txt. For archive inputs we honour the
# directory layout the stealer left behind; for plain-text URLs we
# treat the whole stream as a single set.
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
            # Heuristic: peek at the first non-comment line and accept
            # files that look like Netscape cookie tables.
            try:
                with open(p, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        if not line.strip() or line.lstrip().startswith("#"):
                            continue
                        if parse_cookie_line(line) is not None:
                            out.append(p)
                        break
            except OSError:  # pragma: no cover
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
    """Run the full download → extract → convert pipeline.

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
            except Exception:  # noqa: BLE001
                log.exception("status callback raised")

    result = PipelineResult(
        zip_path=output_dir / "cookies_result.zip",
        output_dir=output_dir,
    )

    # ------------------------------------------------------------------
    # 1. Download (chunked, to disk)
    # ------------------------------------------------------------------
    # We always download to a file rather than streaming, because users
    # routinely send tokenised CDN URLs whose path doesn't carry a
    # ``.zip``/``.7z``/``.rar`` suffix — we can only tell whether the
    # body is an archive after inspecting its first few bytes on disk.
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

    # ------------------------------------------------------------------
    # 2. Decide what we actually got: archive vs plain Netscape file.
    # ------------------------------------------------------------------
    kind = detect_archive_kind(download_path)

    if kind is not None:
        # ------------------------------------------------------
        # 2a. Extract archive
        # ------------------------------------------------------
        status(f"⚙ Processing... (extracting {kind} archive)")
        extracted = workdir / "extracted"
        extract_archive(download_path, extracted, password=password)

        # ------------------------------------------------------
        # 3. Find cookie sources
        # ------------------------------------------------------
        status("⚙ Processing... (scanning extracted files)")
        sources = _find_cookie_files(extracted)
        if not sources:
            status("⚙ Processing... (no cookie files found)")
        else:
            status(
                f"🔄 Converting... ({len(sources)} cookie set(s))"
            )

        for i, src in enumerate(sources, start=1):
            label = _label_for(src, extracted)
            out_path = cookies_dir / f"{i:04d}_{label}.txt"
            n = _extract_set(src, out_path, keywords)
            if n:
                result.cookie_files.append(out_path)
                result.cookie_count += n
            else:
                # No keepers after filtering — drop the empty file.
                try:
                    out_path.unlink()
                except OSError:  # pragma: no cover
                    pass
    else:
        # Defensive fallback: not an archive by magic bytes. Treat the
        # downloaded body as a plain Netscape cookie file.
        status("⚙ Processing... (parsing as plain cookie file)")
        rows: List[CookieRow] = []
        with open(download_path, "r", encoding="utf-8", errors="replace") as f:
            for row in iter_cookies_from_lines(f, keywords=keywords):
                rows.append(row)

        if rows:
            status(
                f"🔄 Converting... (1 cookie set, {len(rows)} cookies)"
            )
            out_path = cookies_dir / "0001_cookies.txt"
            write_netscape_file(out_path, rows)
            result.cookie_files.append(out_path)
            result.cookie_count = len(rows)

    # ------------------------------------------------------------------
    # 4. Zip everything up
    # ------------------------------------------------------------------
    if result.cookie_files:
        status(
            f"⚙ Processing... (packaging {len(result.cookie_files)} file(s))"
        )
        _zip_results(result.zip_path, result.cookie_files, output_dir)
    else:
        # Always materialise a zip so the bot has something to upload /
        # link to (even if it's just an empty result archive).
        _zip_results(result.zip_path, [], output_dir)

    return result
