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
from .download import (
    DownloadError,
    async_download_to_file,
    download_to_file,
    stream_lines,
)
from .gofile import GofileError, is_gofile_url, resolve_gofile_url
from .pipeline import PipelineResult, async_run_pipeline, run_pipeline

__all__ = [
    "ARCHIVE_SUFFIXES",
    "ArchiveError",
    "CookieRow",
    "DownloadError",
    "GofileError",
    "NETSCAPE_HEADER",
    "PipelineResult",
    "async_download_to_file",
    "async_run_pipeline",
    "detect_archive_kind",
    "download_to_file",
    "extract_archive",
    "extract_cookies_from_text",
    "is_archive_url",
    "is_gofile_url",
    "parse_cookie_line",
    "resolve_gofile_url",
    "run_pipeline",
    "stream_lines",
    "write_netscape_file",
]
