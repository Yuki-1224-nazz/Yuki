"""End-to-end pipeline tests using a local HTTP server.

These tests exercise the chunked-download → parse → convert flow
without ever touching Telegram or external networks.
"""

from __future__ import annotations

import shutil
import socket
import threading
import zipfile
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

import pytest

from pipeline import run_pipeline


GOOD_LINE_A = (
    ".netflix.com\tTRUE\t/\tTRUE\t1735689600\tNetflixId\txyz"
)
GOOD_LINE_B = (
    ".example.com\tTRUE\t/\tFALSE\t1735689600\tsession_id\tabc123"
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler(payload: bytes, content_type: str = "text/plain"):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args, **_kwargs) -> None:  # noqa: D401
            return None

    return Handler


@contextmanager
def serve(payload: bytes, content_type: str = "text/plain") -> Iterator[str]:
    port = _free_port()
    httpd = ThreadingHTTPServer(
        ("127.0.0.1", port), _make_handler(payload, content_type)
    )
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}/file.txt"
    finally:
        httpd.shutdown()
        thread.join(timeout=2)


@contextmanager
def serve_zip(
    zip_bytes: bytes, *, url_path: str = "/payload.zip"
) -> Iterator[str]:
    port = _free_port()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(zip_bytes)))
            self.end_headers()
            self.wfile.write(zip_bytes)

        def log_message(self, *_args, **_kwargs) -> None:  # noqa: D401
            return None

    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}{url_path}"
    finally:
        httpd.shutdown()
        thread.join(timeout=2)


def test_pipeline_plain_text_url(tmp_path: Path) -> None:
    body = (
        "# Netscape cookie file\n"
        f"{GOOD_LINE_A}\n"
        f"{GOOD_LINE_B}\n"
    ).encode("utf-8")

    with serve(body) as url:
        result = run_pipeline(url, tmp_path)

    assert result.cookie_count == 2
    assert len(result.cookie_files) == 1
    assert result.zip_path.exists()
    with zipfile.ZipFile(result.zip_path) as z:
        names = z.namelist()
    assert any(n.endswith(".txt") for n in names)


def test_pipeline_keyword_filter(tmp_path: Path) -> None:
    body = "\n".join([GOOD_LINE_A, GOOD_LINE_B]).encode("utf-8")
    with serve(body) as url:
        result = run_pipeline(url, tmp_path, keywords=["netflix"])
    assert result.cookie_count == 1
    assert len(result.cookie_files) == 1
    out = result.cookie_files[0].read_text(encoding="utf-8")
    assert "netflix" in out.lower()
    assert "example.com" not in out


def test_pipeline_no_matches_returns_empty_zip(tmp_path: Path) -> None:
    body = "\n".join([GOOD_LINE_A, GOOD_LINE_B]).encode("utf-8")
    with serve(body) as url:
        result = run_pipeline(url, tmp_path, keywords=["nope-no-match"])
    assert result.cookie_count == 0
    assert result.cookie_files == []
    assert result.zip_path.exists()


@pytest.mark.skipif(
    shutil.which("7z") is None
    and shutil.which("7za") is None
    and shutil.which("7zz") is None,
    reason="7z binary not available on this host",
)
def test_pipeline_archive_one_file_per_set(tmp_path: Path) -> None:
    """Archive with two stealer victims → two cookie set files."""
    src_root = tmp_path / "src"
    (src_root / "victim_1").mkdir(parents=True)
    (src_root / "victim_2").mkdir(parents=True)
    (src_root / "victim_1" / "cookies.txt").write_text(
        f"{GOOD_LINE_A}\n", encoding="utf-8"
    )
    (src_root / "victim_2" / "cookies.txt").write_text(
        f"{GOOD_LINE_B}\n", encoding="utf-8"
    )

    archive_path = tmp_path / "logs.zip"
    with zipfile.ZipFile(archive_path, "w") as z:
        for p in src_root.rglob("*"):
            if p.is_file():
                z.write(p, arcname=p.relative_to(src_root).as_posix())
    zip_bytes = archive_path.read_bytes()

    work = tmp_path / "work"
    with serve_zip(zip_bytes) as url:
        result = run_pipeline(url, work)

    assert len(result.cookie_files) == 2
    assert result.cookie_count == 2


@pytest.mark.skipif(
    shutil.which("7z") is None
    and shutil.which("7za") is None
    and shutil.which("7zz") is None,
    reason="7z binary not available on this host",
)
def test_pipeline_archive_url_without_extension(tmp_path: Path) -> None:
    """Regression: tokenised CDN URLs (LinkForge-style) carry no
    ``.zip``/``.7z``/``.rar`` suffix in their path. The pipeline must
    still detect the archive kind from the response body's magic bytes
    and extract it correctly.
    """
    src_root = tmp_path / "src"
    (src_root / "victim_alpha").mkdir(parents=True)
    (src_root / "victim_alpha" / "cookies.txt").write_text(
        f"{GOOD_LINE_A}\n", encoding="utf-8"
    )
    archive_path = tmp_path / "logs.zip"
    with zipfile.ZipFile(archive_path, "w") as z:
        for p in src_root.rglob("*"):
            if p.is_file():
                z.write(p, arcname=p.relative_to(src_root).as_posix())
    zip_bytes = archive_path.read_bytes()

    work = tmp_path / "work"
    # Simulate a CDN URL: tokenised path, no archive extension.
    with serve_zip(zip_bytes, url_path="/download/AgAD0w22104") as url:
        result = run_pipeline(url, work)

    assert len(result.cookie_files) == 1
    assert result.cookie_count == 1
    with zipfile.ZipFile(result.zip_path) as z:
        names = sorted(z.namelist())
    assert all(n.endswith(".txt") for n in names if not n.endswith("/"))
