"""Tests for archive helpers (URL detection + extraction)."""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import pytest

from pipeline.archive import (
    ARCHIVE_SUFFIXES,
    ArchiveError,
    archive_kind,
    detect_archive_kind,
    extract_archive,
    is_archive_url,
)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://example.com/foo.zip", True),
        ("https://example.com/foo.7z", True),
        ("https://example.com/foo.rar", True),
        ("https://example.com/foo.txt", False),
        ("https://example.com/foo.zip?token=x", True),
        ("https://example.com/", False),
        ("not-a-url", False),
    ],
)
def test_is_archive_url(url: str, expected: bool) -> None:
    assert is_archive_url(url) is expected


def test_archive_kind_from_path(tmp_path: Path) -> None:
    assert archive_kind(tmp_path / "x.zip") == "zip"
    assert archive_kind(tmp_path / "x.7z") == "7z"
    assert archive_kind(tmp_path / "x.rar") == "rar"
    assert archive_kind(tmp_path / "x.txt") is None


def test_archive_suffixes_are_tuple() -> None:
    assert isinstance(ARCHIVE_SUFFIXES, tuple)
    assert ".zip" in ARCHIVE_SUFFIXES
    assert ".7z" in ARCHIVE_SUFFIXES
    assert ".rar" in ARCHIVE_SUFFIXES


@pytest.mark.skipif(
    shutil.which("7z") is None
    and shutil.which("7za") is None
    and shutil.which("7zz") is None,
    reason="7z binary not available on this host",
)
def test_extract_zip_roundtrip(tmp_path: Path) -> None:
    archive = tmp_path / "input.zip"
    inner_dir = tmp_path / "src"
    inner_dir.mkdir()
    cookies_file = inner_dir / "cookies.txt"
    cookies_file.write_text("hello cookies\n", encoding="utf-8")

    with zipfile.ZipFile(archive, "w") as z:
        z.write(cookies_file, arcname="src/cookies.txt")

    out = tmp_path / "out"
    extract_archive(archive, out, password=None)
    assert (out / "src" / "cookies.txt").read_text(encoding="utf-8") == (
        "hello cookies\n"
    )


def test_extract_unsupported_extension_raises(tmp_path: Path) -> None:
    bogus = tmp_path / "x.tar"
    bogus.write_bytes(b"junk")
    with pytest.raises(ArchiveError):
        extract_archive(bogus, tmp_path / "out")


class TestDetectArchiveKind:
    """Magic-byte sniffing must work even when the URL has no suffix."""

    def test_zip_local_file_header(self, tmp_path: Path) -> None:
        f = tmp_path / "opaque-cdn-path"
        f.write_bytes(b"PK\x03\x04" + b"\x00" * 16)
        assert detect_archive_kind(f) == "zip"

    def test_zip_eocd(self, tmp_path: Path) -> None:
        f = tmp_path / "opaque-cdn-path"
        f.write_bytes(b"PK\x05\x06" + b"\x00" * 16)
        assert detect_archive_kind(f) == "zip"

    def test_seven_zip_signature(self, tmp_path: Path) -> None:
        f = tmp_path / "opaque"
        f.write_bytes(b"7z\xbc\xaf\x27\x1c" + b"\x00" * 8)
        assert detect_archive_kind(f) == "7z"

    def test_rar_signature(self, tmp_path: Path) -> None:
        f = tmp_path / "opaque"
        f.write_bytes(b"Rar!\x1a\x07\x00" + b"\x00" * 8)
        assert detect_archive_kind(f) == "rar"

    def test_falls_back_to_extension_when_empty(self, tmp_path: Path) -> None:
        f = tmp_path / "x.zip"
        f.write_bytes(b"")
        assert detect_archive_kind(f) == "zip"

    def test_returns_none_for_plain_text(self, tmp_path: Path) -> None:
        f = tmp_path / "opaque-cdn-path"
        f.write_bytes(b"# Netscape cookie file\n.example.com\tTRUE\n")
        assert detect_archive_kind(f) is None

    def test_magic_bytes_beat_extension(self, tmp_path: Path) -> None:
        # A .zip-named file that is actually a 7z archive
        f = tmp_path / "mislabeled.zip"
        f.write_bytes(b"7z\xbc\xaf\x27\x1c" + b"\x00" * 8)
        assert detect_archive_kind(f) == "7z"


@pytest.mark.skipif(
    shutil.which("7z") is None
    and shutil.which("7za") is None
    and shutil.which("7zz") is None,
    reason="7z binary not available on this host",
)
def test_extract_zip_with_no_extension_via_magic_bytes(
    tmp_path: Path,
) -> None:
    """Reproduces the LinkForge-style URL path: archive served as e.g.
    ``/download/AgAD0w22104`` with no ``.zip`` suffix.

    The bot now writes the body to ``input.bin`` and detects the kind
    from the magic bytes alone.
    """
    src_archive = tmp_path / "src.zip"
    inner = tmp_path / "src"
    inner.mkdir()
    (inner / "cookies.txt").write_text("hello\n", encoding="utf-8")
    with zipfile.ZipFile(src_archive, "w") as z:
        z.write(inner / "cookies.txt", arcname="cookies.txt")

    # Simulate "saved with no suffix" — the bot's pipeline writes
    # ``input.bin`` for tokenised CDN URLs.
    no_ext = tmp_path / "input.bin"
    no_ext.write_bytes(src_archive.read_bytes())

    out = tmp_path / "out"
    extract_archive(no_ext, out, password=None)
    assert (out / "cookies.txt").read_text(encoding="utf-8") == "hello\n"


def test_extract_garbage_raises_with_clear_message(tmp_path: Path) -> None:
    bogus = tmp_path / "input.bin"
    bogus.write_bytes(b"this is just plain text, not an archive\n" * 10)
    with pytest.raises(ArchiveError, match="magic bytes"):
        extract_archive(bogus, tmp_path / "out")
