"""Archive extraction helpers (zip / 7z / rar).

All three archive types route through ``7z`` (from ``p7zip-full``) —
recent versions of p7zip support both RAR4 and RAR5 archives in
addition to zip and 7z, including encryption. The proprietary ``unrar``
binary is checked as a fallback for RAR archives, but is no longer
required — it's not available on Railway's Railpack runtime image.

The bot accepts CDN URLs that don't carry an ``.zip``/``.7z``/``.rar``
suffix in their path (e.g. tokenised LinkForge / file-host URLs), so
:func:`detect_archive_kind` sniffs the file's magic bytes before
falling back to the suffix.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

log = logging.getLogger(__name__)

ARCHIVE_SUFFIXES: tuple[str, ...] = (".zip", ".7z", ".rar")

# Magic byte signatures, in (kind, prefix) tuples. ZIP has three valid
# leading records (local file header, end-of-central-directory record,
# data descriptor) so we list all three. RAR4 and RAR5 share the
# leading ``Rar!\x1a\x07`` so a single 7-byte prefix covers both.
MAGIC_SIGNATURES: tuple[tuple[str, bytes], ...] = (
    ("zip", b"PK\x03\x04"),
    ("zip", b"PK\x05\x06"),
    ("zip", b"PK\x07\x08"),
    ("7z", b"7z\xbc\xaf\x27\x1c"),
    ("rar", b"Rar!\x1a\x07"),
)

# All 7z-family binaries the bot will accept. ``7z`` is the canonical
# one on Linux nixpkgs; macOS Homebrew installs ``7zz``.
SEVENZIP_BINARIES: tuple[str, ...] = ("7z", "7za", "7zz")
UNRAR_BINARIES: tuple[str, ...] = ("unrar",)


class ArchiveError(RuntimeError):
    """Raised when archive extraction fails."""


def is_archive_url(url: str) -> bool:
    """Return ``True`` when the URL path ends in a known archive ext."""
    name = Path(urlparse(url).path).name.lower()
    return any(name.endswith(suf) for suf in ARCHIVE_SUFFIXES)


def archive_kind(path: Path) -> Optional[str]:
    """Return ``"zip"`` / ``"7z"`` / ``"rar"`` based on extension."""
    name = path.name.lower()
    for suf in ARCHIVE_SUFFIXES:
        if name.endswith(suf):
            return suf.lstrip(".")
    return None


def _read_magic_header(path: Path, n: int = 8) -> bytes:
    try:
        with open(path, "rb") as f:
            return f.read(n)
    except OSError:
        return b""


def detect_archive_kind(path: Path) -> Optional[str]:
    """Return the archive kind for ``path`` (``"zip"``/``"7z"``/``"rar"``).

    Magic bytes are checked first so the bot accepts archives served
    behind opaque CDN URLs (no ``.zip``/``.7z``/``.rar`` suffix in the
    URL path). The on-disk filename suffix is consulted as a fallback
    only when the file is empty / unreadable / not-yet-downloaded.
    """
    header = _read_magic_header(path)
    for kind, prefix in MAGIC_SIGNATURES:
        if header.startswith(prefix):
            return kind
    return archive_kind(path)


def _which_first(candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        path = shutil.which(c)
        if path:
            return path
    return None


def extract_archive(
    archive_path: Path,
    dest_dir: Path,
    *,
    password: Optional[str] = None,
    timeout: int = 1800,
) -> Path:
    """Extract ``archive_path`` into ``dest_dir`` (created if needed).

    Returns ``dest_dir``. Raises :class:`ArchiveError` on any failure.
    The archive type is detected from magic bytes first and the file
    extension second — the bot routinely sees archives behind tokenised
    CDN URLs that have no extension at all.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    kind = detect_archive_kind(archive_path)
    if kind is None:
        raise ArchiveError(
            f"unsupported archive type: {archive_path.name} "
            "(magic bytes don't match zip/7z/rar)"
        )

    if kind in ("zip", "7z", "rar"):
        bin_path = _which_first(SEVENZIP_BINARIES)
        if bin_path is None:
            # Last-resort fallback: ``unrar`` only handles ``.rar``.
            unrar_path = (
                _which_first(UNRAR_BINARIES) if kind == "rar" else None
            )
            if unrar_path is None:
                raise ArchiveError(
                    "7z binary not found — install p7zip-full on "
                    "your host (it handles zip, 7z, and rar)."
                )
            cmd = [unrar_path, "x", "-y"]
            if password is not None and password != "":
                cmd.append(f"-p{password}")
            else:
                cmd.append("-p-")
            cmd += [str(archive_path), str(dest_dir) + "/"]
        else:
            cmd = [
                bin_path,
                "x",
                "-y",
                f"-o{dest_dir}",
                str(archive_path),
            ]
            if password is not None and password != "":
                cmd.insert(2, f"-p{password}")
            else:
                # Use a non-empty placeholder so 7z fails fast on
                # encrypted archives instead of hanging on the
                # interactive prompt.
                cmd.insert(2, "-p-")
    else:  # pragma: no cover — guarded by detect_archive_kind() above
        raise ArchiveError(f"unsupported archive type: {kind}")

    log.info("extracting %s -> %s (kind=%s)", archive_path, dest_dir, kind)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ArchiveError(f"extraction timed out after {timeout}s") from exc
    except FileNotFoundError as exc:
        raise ArchiveError(f"extractor binary not found: {exc}") from exc

    if proc.returncode != 0:
        # 7z returns 2 for "fatal error" which is what we get on a bad
        # password; surface a friendly message either way.
        stderr = (proc.stderr or proc.stdout or "").strip().splitlines()
        tail = stderr[-1] if stderr else f"rc={proc.returncode}"
        raise ArchiveError(f"extraction failed: {tail}")

    return dest_dir
