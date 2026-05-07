"""Archive extraction helpers (zip / 7z / rar).

All three archive types route through ``7z`` (from ``p7zip-full``) —
recent versions of p7zip support both RAR4 and RAR5 archives in
addition to zip and 7z, including encryption. The proprietary ``unrar``
binary is checked as a fallback for RAR archives, but is no longer
required — it's not available on Railway's Railpack runtime image.

When ``7z`` fails with "Unsupported Method" (common with ZIP archives
that use ZSTD or other modern compression), the bot falls back to
Python's built-in :mod:`zipfile` and then to the ``unzip`` command.

The bot accepts CDN URLs that don't carry an ``.zip``/``.7z``/``.rar``
suffix in their path (e.g. tokenised LinkForge / file-host URLs), so
:func:`detect_archive_kind` sniffs the file's magic bytes before
falling back to the suffix.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import zipfile
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
UNZIP_BINARIES: tuple[str, ...] = ("unzip",)


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


def _is_wrong_password(output: str) -> bool:
    """Detect wrong-password errors across 7z/unrar/unzip output."""
    low = output.lower()
    return (
        "wrong password" in low
        or "crc failed" in low
        or "encrypted" in low
        or "incorrect password" in low
    )


def _extract_with_7z(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Try extraction with 7z/7za/7zz."""
    bin_path = _which_first(SEVENZIP_BINARIES)
    if bin_path is None:
        raise ArchiveError(
            "7z binary not found — install p7zip-full on "
            "your host (it handles zip, 7z, and rar)."
        )
    cmd = [bin_path, "x", "-y", f"-o{dest_dir}", str(archive_path)]
    if password is not None and password != "":
        cmd.insert(2, f"-p{password}")
    else:
        cmd.insert(2, "-p-")

    log.info("7z extract: %s", " ".join(cmd))
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False,
    )


def _extract_with_python_zipfile(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
) -> None:
    """Fallback: extract ZIP archives using Python's zipfile module."""
    pwd_bytes = password.encode("utf-8") if password else None
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(path=dest_dir, pwd=pwd_bytes)
    except RuntimeError as exc:
        msg = str(exc).lower()
        if "password" in msg or "encrypted" in msg:
            raise ArchiveError(
                "wrong password — the archive is encrypted and the "
                "password you provided didn't work. Please check "
                "and try again with /start."
            ) from exc
        raise ArchiveError(f"Python zipfile extraction failed: {exc}") from exc
    except zipfile.BadZipFile as exc:
        raise ArchiveError(f"corrupt or invalid zip: {exc}") from exc


def _extract_with_unzip(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Fallback: extract ZIP archives using the ``unzip`` command."""
    bin_path = _which_first(UNZIP_BINARIES)
    if bin_path is None:
        raise ArchiveError("unzip binary not found")
    cmd = [bin_path, "-o", str(archive_path), "-d", str(dest_dir)]
    if password is not None and password != "":
        cmd.extend(["-P", password])

    log.info("unzip extract: %s", " ".join(cmd))
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False,
    )


def _extract_with_unrar(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Fallback: extract RAR archives using the ``unrar`` command."""
    bin_path = _which_first(UNRAR_BINARIES)
    if bin_path is None:
        raise ArchiveError("unrar binary not found")
    cmd = [bin_path, "x", "-y"]
    if password is not None and password != "":
        cmd.append(f"-p{password}")
    else:
        cmd.append("-p-")
    cmd += [str(archive_path), str(dest_dir) + "/"]

    log.info("unrar extract: %s", " ".join(cmd))
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False,
    )


def extract_archive(
    archive_path: Path,
    dest_dir: Path,
    *,
    password: Optional[str] = None,
    timeout: int = 1800,
) -> Path:
    """Extract ``archive_path`` into ``dest_dir`` (created if needed).

    Returns ``dest_dir``. Raises :class:`ArchiveError` on any failure.

    Extraction strategy (tried in order until one succeeds):
    1. **7z** — handles zip, 7z, rar (including encrypted).
    2. **Python zipfile** — fallback for ZIP with modern compression
       methods that older p7zip builds don't support (e.g. ZSTD).
    3. **unzip** — another fallback for ZIP archives.
    4. **unrar** — last resort for RAR-only environments.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    kind = detect_archive_kind(archive_path)
    if kind is None:
        raise ArchiveError(
            f"unsupported archive type: {archive_path.name} "
            "(magic bytes don't match zip/7z/rar)"
        )

    log.info("extracting %s -> %s (kind=%s)", archive_path, dest_dir, kind)

    # --- Attempt 1: 7z ---
    sevenzip_error = ""
    try:
        proc = _extract_with_7z(archive_path, dest_dir, password, timeout)
        if proc.returncode == 0:
            return dest_dir
        sevenzip_error = (proc.stderr or proc.stdout or "").strip()
        log.warning("7z failed (rc=%d): %s", proc.returncode, sevenzip_error)

        if _is_wrong_password(sevenzip_error):
            raise ArchiveError(
                "wrong password — the archive is encrypted and the "
                "password you provided didn't work. Please check "
                "and try again with /start."
            )
    except subprocess.TimeoutExpired as exc:
        raise ArchiveError(f"extraction timed out after {timeout}s") from exc
    except FileNotFoundError:
        log.warning("7z binary not found, trying fallbacks")
        sevenzip_error = "7z not found"

    # --- Attempt 2 (ZIP only): Python zipfile ---
    if kind == "zip":
        try:
            log.info("falling back to Python zipfile for %s", archive_path.name)
            _extract_with_python_zipfile(archive_path, dest_dir, password)
            return dest_dir
        except ArchiveError:
            raise
        except Exception as exc:
            log.warning("Python zipfile failed: %s", exc)

    # --- Attempt 3 (ZIP only): unzip command ---
    if kind == "zip":
        try:
            log.info("falling back to unzip command for %s", archive_path.name)
            proc = _extract_with_unzip(archive_path, dest_dir, password, timeout)
            if proc.returncode == 0:
                return dest_dir
            unzip_error = (proc.stderr or proc.stdout or "").strip()
            log.warning("unzip failed (rc=%d): %s", proc.returncode, unzip_error)
            if _is_wrong_password(unzip_error):
                raise ArchiveError(
                    "wrong password — the archive is encrypted and the "
                    "password you provided didn't work. Please check "
                    "and try again with /start."
                )
        except ArchiveError:
            raise
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(f"extraction timed out after {timeout}s") from exc
        except FileNotFoundError:
            log.warning("unzip not found")

    # --- Attempt 4 (RAR only): unrar ---
    if kind == "rar":
        try:
            log.info("falling back to unrar for %s", archive_path.name)
            proc = _extract_with_unrar(archive_path, dest_dir, password, timeout)
            if proc.returncode == 0:
                return dest_dir
            unrar_error = (proc.stderr or proc.stdout or "").strip()
            log.warning("unrar failed (rc=%d): %s", proc.returncode, unrar_error)
            if _is_wrong_password(unrar_error):
                raise ArchiveError(
                    "wrong password — the archive is encrypted and the "
                    "password you provided didn't work. Please check "
                    "and try again with /start."
                )
        except ArchiveError:
            raise
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(f"extraction timed out after {timeout}s") from exc
        except FileNotFoundError:
            log.warning("unrar not found")

    # All attempts failed — report the original 7z error
    lines = sevenzip_error.splitlines()
    tail = lines[-1] if lines else "all extraction methods failed"
    raise ArchiveError(f"extraction failed: {tail}")
