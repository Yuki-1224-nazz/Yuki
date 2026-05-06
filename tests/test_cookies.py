"""Tests for the Netscape cookie parser/writer."""

from pathlib import Path

import pytest

from pipeline.cookies import (
    NETSCAPE_HEADER,
    extract_cookies_from_text,
    parse_cookie_line,
    write_netscape_file,
)


GOOD_LINE = (
    ".example.com\tTRUE\t/\tFALSE\t1735689600\tsession_id\tabc123"
)
HTTPONLY_LINE = (
    "#HttpOnly_.netflix.com\tTRUE\t/\tTRUE\t1735689600\t"
    "NetflixId\txyz"
)
COMMENT_LINE = "# This is a Netscape cookie file"
TOO_FEW_COLS_LINE = ".bad.com\tTRUE\t/\tFALSE"


def test_parse_good_line() -> None:
    row = parse_cookie_line(GOOD_LINE)
    assert row is not None
    assert row.domain == ".example.com"
    assert row.include_subdomains == "TRUE"
    assert row.path == "/"
    assert row.secure == "FALSE"
    assert row.expires == "1735689600"
    assert row.name == "session_id"
    assert row.value == "abc123"
    assert row.to_line() == GOOD_LINE


def test_parse_httponly_prefix_kept() -> None:
    row = parse_cookie_line(HTTPONLY_LINE)
    assert row is not None
    assert row.domain == "#HttpOnly_.netflix.com"
    assert row.name == "NetflixId"
    assert row.secure == "TRUE"


@pytest.mark.parametrize(
    "line",
    ["", "  \n", "\t\t\t\t\t\t", COMMENT_LINE, TOO_FEW_COLS_LINE],
)
def test_parse_skips_invalid(line: str) -> None:
    assert parse_cookie_line(line) is None


def test_extract_cookies_from_text_returns_only_valid_rows() -> None:
    text = "\n".join(
        [
            COMMENT_LINE,
            "",
            GOOD_LINE,
            TOO_FEW_COLS_LINE,
            HTTPONLY_LINE,
        ]
    )
    rows = extract_cookies_from_text(text)
    assert len(rows) == 2
    assert {r.name for r in rows} == {"session_id", "NetflixId"}


def test_extract_cookies_keyword_filter_is_case_insensitive() -> None:
    text = "\n".join([GOOD_LINE, HTTPONLY_LINE])
    rows = extract_cookies_from_text(text, keywords=["NETFLIX"])
    assert len(rows) == 1
    assert rows[0].name == "NetflixId"


def test_extract_cookies_multiple_keywords_any_match() -> None:
    text = "\n".join([GOOD_LINE, HTTPONLY_LINE])
    rows = extract_cookies_from_text(text, keywords=["example", "netflix"])
    assert len(rows) == 2


def test_write_netscape_file_emits_header_and_rows(tmp_path: Path) -> None:
    rows = extract_cookies_from_text("\n".join([GOOD_LINE, HTTPONLY_LINE]))
    out = tmp_path / "out.txt"
    n = write_netscape_file(out, rows)
    assert n == 2
    content = out.read_text(encoding="utf-8")
    assert content.startswith(NETSCAPE_HEADER)
    body = content[len(NETSCAPE_HEADER):]
    assert GOOD_LINE in body
    assert HTTPONLY_LINE in body
