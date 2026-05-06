"""Logs-to-cookie processing pipeline."""

from .archive import (
    ARCHIVE_SUFFIXES,
    ArchiveError,
    detect_archive_kind,
    extract_archive,
    is_archive_url,
)
from .cookies import (
    NETSCAPE_HEADER,
    CookieRow,
    extract_cookies_from_text,
    parse_cookie_line,
    write_netscape_file,
)
from .download import DownloadError, download_to_file, stream_lines
from .pipeline import PipelineResult, run_pipeline

__all__ = [
    "ARCHIVE_SUFFIXES",
    "ArchiveError",
    "CookieRow",
    "DownloadError",
    "NETSCAPE_HEADER",
    "PipelineResult",
    "detect_archive_kind",
    "download_to_file",
    "extract_archive",
    "extract_cookies_from_text",
    "is_archive_url",
    "parse_cookie_line",
    "run_pipeline",
    "stream_lines",
    "write_netscape_file",
]
