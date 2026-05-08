"""Archive extraction helpers (zip / 7z / rar).

Extraction strategy is tailored per archive type:

**RAR** — ``unrar`` is the primary extractor (only tool that fully
supports RAR5 encryption + compression).  ``7z``/``7zz`` are tried
as fallbacks but often produce 0-byte files on RAR5 archives.

**ZIP** — ``7zz`` (modern 7-Zip, supports ZSTD etc.) is preferred,
with ``7z`` (legacy p7zip), Python's :mod:`zipfile`, and ``unzip``
as fallbacks.

**7z** — ``7zz`` is preferred over legacy ``7z``.

The bot accepts CDN URLs that don't carry an ``.zip``/``.7z``/``.rar``
suffix in their path (e.g. tokenised LinkForge / file-host URLs), so
:func:`detect_archive_kind` sniffs the file's magic bytes before
falling back to the suffix.
"""

from __future__ import annotations

import logging
import os
import platform
import shutil
import stat
import subprocess
import zipfile
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

log = logging.getLogger(__name__)

ARCHIVE_SUFFIXES: tuple[str, ...] = (
    ".zip", ".7z", ".rar", ".tar", ".tar.gz", ".tgz",
    ".tar.bz2", ".tbz2", ".tar.xz", ".txz",
)

MAGIC_SIGNATURES: tuple[tuple[str, bytes], ...] = (
    ("zip", b"PK\x03\x04"),
    ("zip", b"PK\x05\x06"),
    ("zip", b"PK\x07\x08"),
    ("7z", b"7z\xbc\xaf\x27\x1c"),
    ("rar", b"Rar!\x1a\x07"),
    ("gz", b"\x1f\x8b"),
    ("bz2", b"BZ"),
    ("xz", b"\xfd7zXZ\x00"),
)

# Prefer ``7zz`` (modern 7-Zip for Linux, supports ZSTD and all
# modern compression methods) over ``7z`` (legacy p7zip).
SEVENZIP_BINARIES: tuple[str, ...] = ("7zz", "7z", "7za")
UNRAR_BINARIES: tuple[str, ...] = ("unrar",)
UNZIP_BINARIES: tuple[str, ...] = ("unzip",)

# Archives larger than this skip the Python zipfile fallback.
_PYTHON_ZIP_MAX_BYTES = 500 * 1024 * 1024  # 500 MB

# Official RAR/UnRAR download URL (Linux x64 static binary).
_UNRAR_URL = "https://www.rarlab.com/rar/rarlinux-x64-722.tar.gz"
_UNRAR_LOCAL_DIR = Path.home() / ".local" / "bin"


class ArchiveError(RuntimeError):
    """Raised when archive extraction fails."""


def _ensure_unrar() -> Optional[str]:
    """Return the path to ``unrar``, downloading it if needed.

    If ``unrar`` is already on PATH, return it immediately.  Otherwise
    download the official Linux x64 static binary from rarlab.com and
    cache it locally.  Returns ``None`` if download fails or platform
    is not Linux x64.
    """
    existing = shutil.which("unrar")
    if existing:
        return existing

    # Check common cached locations
    for candidate_dir in (_UNRAR_LOCAL_DIR, Path("/tmp/.unrar_bin")):
        candidate = candidate_dir / "unrar"
        if candidate.is_file():
            return str(candidate)

    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "AMD64"):
        log.warning("auto-download of unrar only supported on Linux x64")
        return None

    log.info("unrar not found — downloading from rarlab.com ...")

    # Try multiple target dirs in case some are read-only
    target_dirs = [_UNRAR_LOCAL_DIR, Path("/tmp/.unrar_bin")]
    for target_dir in target_dirs:
        try:
            import tarfile
            import tempfile
            import urllib.request

            target_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
                tmp_path = tmp.name
                urllib.request.urlretrieve(_UNRAR_URL, tmp_path)

            target_bin = target_dir / "unrar"
            with tarfile.open(tmp_path, "r:gz") as tf:
                for member in tf.getmembers():
                    if member.name.endswith("/unrar") or member.name == "unrar":
                        member.name = "unrar"
                        tf.extract(member, path=str(target_dir))
                        break

            os.unlink(tmp_path)
            target_bin.chmod(target_bin.stat().st_mode | stat.S_IEXEC)

            if target_bin.is_file():
                log.info("unrar installed to %s", target_bin)
                return str(target_bin)
        except Exception:
            log.warning("failed to install unrar to %s", target_dir, exc_info=True)
            continue

    log.error("could not install unrar to any location")
    return None


def is_archive_url(url: str) -> bool:
    """Return ``True`` when the URL path ends in a known archive ext."""
    name = Path(urlparse(url).path).name.lower()
    return any(name.endswith(suf) for suf in ARCHIVE_SUFFIXES)


def archive_kind(path: Path) -> Optional[str]:
    """Return archive type based on extension."""
    name = path.name.lower()
    # Check tar variants first (multi-part extensions)
    tar_map = {
        ".tar.gz": "gz", ".tgz": "gz",
        ".tar.bz2": "bz2", ".tbz2": "bz2",
        ".tar.xz": "xz", ".txz": "xz",
        ".tar": "tar",
    }
    for suf, kind in tar_map.items():
        if name.endswith(suf):
            return kind
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
    """Return the archive kind for ``path``.

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


def _is_tar_archive(path: Path) -> bool:
    """Check if a file is a tar archive (including compressed tar)."""
    name = path.name.lower()
    tar_suffixes = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")
    return any(name.endswith(s) for s in tar_suffixes)


def _which_first(candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        path = shutil.which(c)
        if path:
            return path
    return None


def _which_all(candidates: Sequence[str]) -> list[str]:
    """Return paths for all found binaries (preserving order)."""
    out: list[str] = []
    for c in candidates:
        path = shutil.which(c)
        if path:
            out.append(path)
    return out


def _is_wrong_password(output: str) -> bool:
    """Detect wrong-password errors from extractor output.

    Only returns True for UNAMBIGUOUS wrong-password signals.
    7z often outputs "Wrong password?" as a GUESS when it fails for
    other reasons (e.g. can't decode RAR5), so we require the message
    to NOT end with "?" (which indicates a guess, not a definitive error).
    """
    low = output.lower()
    # unrar: "Incorrect password for ..."
    if "incorrect password" in low:
        return True
    # unrar: "The specified password is incorrect"
    if "password is incorrect" in low:
        return True
    # 7z definitive: "Wrong password" (without trailing ?)
    # But ignore 7z's guess: "Wrong password?"
    if "wrong password" in low:
        # Check if it's 7z's guess format (ends with ?)
        if "wrong password?" not in low:
            return True
    return False


def _dest_has_real_files(dest_dir: Path) -> bool:
    """Return True if dest_dir has at least one non-empty file."""
    try:
        for p in dest_dir.rglob("*"):
            if p.is_file() and p.stat().st_size > 0:
                return True
    except OSError:
        pass
    return False


def _run_extractor(
    cmd: list[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Run an extractor command, decoding output as UTF-8 with replacement.

    Archives frequently contain filenames with non-UTF-8 bytes (e.g.
    Windows-1251 Cyrillic, Shift-JIS, etc.).  Using ``text=True`` would
    crash with ``UnicodeDecodeError``, so we capture raw bytes and
    decode manually with ``errors="replace"``.
    """
    proc = subprocess.run(
        cmd, capture_output=True, timeout=timeout, check=False,
        stdin=subprocess.DEVNULL,
    )
    return subprocess.CompletedProcess(
        args=proc.args,
        returncode=proc.returncode,
        stdout=proc.stdout.decode("utf-8", errors="replace") if proc.stdout else "",
        stderr=proc.stderr.decode("utf-8", errors="replace") if proc.stderr else "",
    )


# ── Individual extractors ──────────────────────────────────────────


def _try_7z_binary(
    bin_path: str,
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Run a single 7z-family binary."""
    cmd = [bin_path, "x", "-y", f"-o{dest_dir}", str(archive_path)]
    if password is not None and password != "":
        cmd.insert(2, f"-p{password}")
    else:
        cmd.insert(2, "-p-")
    log.info("7z extract: %s", " ".join(cmd))
    return _run_extractor(cmd, timeout)


def _extract_with_7z_all(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Try all available 7z-family binaries in preference order.

    Returns the CompletedProcess from the first binary that succeeds
    (rc 0 or 1 with real files), or the last CompletedProcess on
    total failure.  Returns ``None`` when no 7z binary is found.
    """
    binaries = _which_all(SEVENZIP_BINARIES)
    if not binaries:
        log.warning("no 7z binary found on PATH")
        return None

    last_proc: Optional[subprocess.CompletedProcess[str]] = None
    for bin_path in binaries:
        try:
            proc = _try_7z_binary(
                bin_path, archive_path, dest_dir, password, timeout,
            )
            last_proc = proc
            output = (proc.stderr or proc.stdout or "").strip()

            if _is_wrong_password(output):
                return proc  # caller will raise

            if proc.returncode == 0:
                return proc

            # Accept any non-zero rc when real files were produced
            if _dest_has_real_files(dest_dir):
                log.info(
                    "%s exited rc=%d but produced real files — "
                    "accepting", bin_path, proc.returncode,
                )
                return proc

            log.warning(
                "%s failed (rc=%d): %s", bin_path, proc.returncode, output,
            )
            # Wipe 0-byte files so the next binary starts clean.
            shutil.rmtree(dest_dir, ignore_errors=True)
            dest_dir.mkdir(parents=True, exist_ok=True)
        except subprocess.TimeoutExpired:
            raise
        except FileNotFoundError:
            continue

    return last_proc


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
) -> Optional[subprocess.CompletedProcess[str]]:
    """Fallback: extract ZIP archives using the ``unzip`` command."""
    bin_path = _which_first(UNZIP_BINARIES)
    if bin_path is None:
        log.warning("unzip binary not found, skipping fallback")
        return None
    cmd = [bin_path, "-o", str(archive_path), "-d", str(dest_dir)]
    if password is not None and password != "":
        cmd.extend(["-P", password])
    log.info("unzip extract: %s", " ".join(cmd))
    return _run_extractor(cmd, timeout)


def _extract_with_unrar(
    archive_path: Path,
    dest_dir: Path,
    password: Optional[str],
    timeout: int,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Extract RAR archives using the ``unrar`` command."""
    bin_path = _ensure_unrar()
    if bin_path is None:
        log.warning("unrar binary not available, skipping")
        return None
    cmd = [bin_path, "x", "-y", "-o+"]
    if password is not None and password != "":
        cmd.append(f"-p{password}")
    else:
        cmd.append("-p-")
    cmd += [str(archive_path), str(dest_dir) + "/"]
    log.info("unrar extract: %s", " ".join(cmd))
    return _run_extractor(cmd, timeout)


def _extract_with_tar(
    archive_path: Path,
    dest_dir: Path,
    timeout: int,
) -> Optional[subprocess.CompletedProcess[str]]:
    """Extract tar/tar.gz/tar.bz2/tar.xz archives using Python tarfile."""
    import tarfile as _tarfile
    try:
        with _tarfile.open(str(archive_path)) as tf:
            tf.extractall(path=str(dest_dir))
        return subprocess.CompletedProcess(
            args=["tarfile"], returncode=0, stdout="", stderr="",
        )
    except Exception as exc:
        log.warning("tarfile extraction failed: %s", exc)
        return subprocess.CompletedProcess(
            args=["tarfile"], returncode=1, stdout="", stderr=str(exc),
        )


# ── Dispatcher helpers ─────────────────────────────────────────────


def _check_proc_result(
    proc: Optional[subprocess.CompletedProcess[str]],
    dest_dir: Path,
    label: str,
) -> Optional[str]:
    """Return ``None`` on success, or an error string on failure.

    Success means real (non-zero-byte) files were extracted, regardless
    of exit code.  Wrong-password is only raised when NO files were
    produced AND the output clearly indicates a password problem.
    """
    if proc is None:
        return f"{label} not available"

    # FIRST: check if real files were extracted — if yes, password was
    # correct regardless of what the output says (extractors often
    # print misleading messages for partial success).
    if _dest_has_real_files(dest_dir):
        if proc.returncode != 0:
            log.info(
                "%s exited with rc=%d but produced real files — "
                "accepting as success",
                label, proc.returncode,
            )
        return None

    # No files extracted — check if it's a password issue
    output = (proc.stderr or proc.stdout or "").strip()
    if _is_wrong_password(output):
        raise ArchiveError(
            "wrong password — the archive is encrypted and the "
            "password you provided didn't work. Please check "
            "and try again with /start."
        )
    return output or f"{label} failed (rc={proc.returncode})"


# ── Main entry point ───────────────────────────────────────────────


def extract_archive(
    archive_path: Path,
    dest_dir: Path,
    *,
    password: Optional[str] = None,
    timeout: int = 1800,
) -> Path:
    """Extract ``archive_path`` into ``dest_dir`` (created if needed).

    Returns ``dest_dir``. Raises :class:`ArchiveError` on any failure.

    Supports all major archive formats:

    **RAR**: unrar → 7zz/7z (all binaries)
    **ZIP**: 7zz/7z (all binaries) → Python zipfile → unzip
    **7z** : 7zz/7z (all binaries)
    **tar/gz/bz2/xz**: Python tarfile → 7zz/7z
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    kind = detect_archive_kind(archive_path)

    # For tar-like formats detected by magic or suffix
    if kind in ("gz", "bz2", "xz") or _is_tar_archive(archive_path):
        kind = "tar"

    if kind is None:
        raise ArchiveError(
            f"unsupported archive type: {archive_path.name} "
            "(magic bytes don't match zip/7z/rar/tar)"
        )

    archive_size = archive_path.stat().st_size
    log.info(
        "extracting %s -> %s (kind=%s, size=%d)",
        archive_path, dest_dir, kind, archive_size,
    )

    errors: list[str] = []

    if kind == "rar":
        # ── RAR: prefer unrar (handles RAR5), then 7z as fallback ──
        try:
            proc = _extract_with_unrar(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "unrar")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

        # Clean dest before trying 7z
        shutil.rmtree(dest_dir, ignore_errors=True)
        dest_dir.mkdir(parents=True, exist_ok=True)

        try:
            proc = _extract_with_7z_all(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "7z")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

    elif kind == "zip":
        # ── ZIP: prefer 7zz/7z, then Python zipfile, then unzip ──
        try:
            proc = _extract_with_7z_all(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "7z")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

        # Python zipfile — skip for large or encrypted archives
        if archive_size <= _PYTHON_ZIP_MAX_BYTES:
            try:
                log.info("falling back to Python zipfile")
                shutil.rmtree(dest_dir, ignore_errors=True)
                dest_dir.mkdir(parents=True, exist_ok=True)
                _extract_with_python_zipfile(
                    archive_path, dest_dir, password,
                )
                if _dest_has_real_files(dest_dir):
                    return dest_dir
                errors.append("Python zipfile produced no files")
            except ArchiveError:
                raise
            except Exception as exc:
                errors.append(f"Python zipfile: {exc}")

        # unzip command
        try:
            shutil.rmtree(dest_dir, ignore_errors=True)
            dest_dir.mkdir(parents=True, exist_ok=True)
            proc = _extract_with_unzip(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "unzip")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

    elif kind == "tar":
        # ── TAR (gz/bz2/xz): Python tarfile → 7z ──
        try:
            proc = _extract_with_tar(archive_path, dest_dir, timeout)
            err = _check_proc_result(proc, dest_dir, "tarfile")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

        # Fallback to 7z for tar archives
        shutil.rmtree(dest_dir, ignore_errors=True)
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            proc = _extract_with_7z_all(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "7z")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

    else:
        # ── 7z / unknown: try 7z binaries, then unrar ──
        try:
            proc = _extract_with_7z_all(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "7z")
            if err is None:
                return dest_dir
            errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

        # Try unrar as last resort for unknown formats
        shutil.rmtree(dest_dir, ignore_errors=True)
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            proc = _extract_with_unrar(
                archive_path, dest_dir, password, timeout,
            )
            err = _check_proc_result(proc, dest_dir, "unrar")
            if err is None:
                return dest_dir
            if err:
                errors.append(err)
        except subprocess.TimeoutExpired as exc:
            raise ArchiveError(
                f"extraction timed out after {timeout}s"
            ) from exc

    # All attempts failed
    last_error = errors[-1] if errors else "all extraction methods failed"
    lines = last_error.splitlines()
    # Find most informative error line (skip empty/generic lines)
    tail = ""
    for line in reversed(lines):
        stripped = line.strip()
        if stripped and "---" not in stripped:
            tail = stripped
            break
    if not tail:
        tail = lines[-1] if lines else last_error
    # Truncate very long error messages for Telegram display
    if len(tail) > 200:
        tail = tail[:200] + "..."
    raise ArchiveError(f"extraction failed: {tail}")
