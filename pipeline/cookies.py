"""Netscape cookie parsing and writing helpers.

The Netscape cookies file format is a tab-separated file with seven
columns per row:

    domain  flag  path  secure  expires  name  value

A leading ``#HttpOnly_`` prefix on the domain column is preserved by
modern browsers and is treated as part of the domain field here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

NETSCAPE_HEADER = (
    "# Netscape HTTP Cookie File\n"
    "# https://curl.se/docs/http-cookies.html\n"
    "# This is a generated file. Do not edit.\n\n"
)


@dataclass(frozen=True)
class CookieRow:
    """One parsed Netscape cookie row."""

    domain: str
    include_subdomains: str  # "TRUE" / "FALSE"
    path: str
    secure: str  # "TRUE" / "FALSE"
    expires: str  # integer-as-string (epoch)
    name: str
    value: str

    def to_line(self) -> str:
        return "\t".join(
            (
                self.domain,
                self.include_subdomains,
                self.path,
                self.secure,
                self.expires,
                self.name,
                self.value,
            )
        )


def parse_cookie_line(line: str) -> Optional[CookieRow]:
    """Parse a single Netscape cookie line.

    Returns ``None`` for blank lines, comments, and malformed lines so
    callers can iterate over arbitrary log content without crashing.
    """
    if not line:
        return None
    raw = line.rstrip("\r\n")
    if not raw.strip():
        return None
    # Allow ``#HttpOnly_`` rows (those are real cookie rows whose
    # domain column starts with ``#HttpOnly_``). Skip every other line
    # that starts with ``#`` — they're comments.
    if raw.lstrip().startswith("#") and not raw.lstrip().startswith("#HttpOnly_"):
        return None

    parts = raw.split("\t")
    if len(parts) != 7:
        return None

    domain, include_sub, path, secure, expires, name, value = parts
    if not domain or not name:
        return None

    return CookieRow(
        domain=domain,
        include_subdomains=include_sub.strip().upper() or "FALSE",
        path=path or "/",
        secure=secure.strip().upper() or "FALSE",
        expires=expires.strip() or "0",
        name=name,
        value=value,
    )


def _matches_keywords(line: str, keywords: Iterable[str]) -> bool:
    haystack = line.lower()
    for kw in keywords:
        kw = kw.strip().lower()
        if kw and kw in haystack:
            return True
    return False


def extract_cookies_from_text(
    text: str,
    keywords: Optional[Iterable[str]] = None,
) -> List[CookieRow]:
    """Extract every valid cookie row from a chunk of text.

    If ``keywords`` is provided and non-empty, a row is only kept when
    its raw line contains at least one of the keywords (case-insensitive).
    """
    kw_list = [k for k in (keywords or []) if k and k.strip()]
    out: List[CookieRow] = []
    for line in text.splitlines():
        if kw_list and not _matches_keywords(line, kw_list):
            continue
        row = parse_cookie_line(line)
        if row is not None:
            out.append(row)
    return out


def iter_cookies_from_lines(
    lines: Iterable[str],
    keywords: Optional[Iterable[str]] = None,
) -> Iterator[CookieRow]:
    """Stream-friendly variant of :func:`extract_cookies_from_text`."""
    kw_list = [k for k in (keywords or []) if k and k.strip()]
    for line in lines:
        if kw_list and not _matches_keywords(line, kw_list):
            continue
        row = parse_cookie_line(line)
        if row is not None:
            yield row


def write_netscape_file(path: Path, cookies: Iterable[CookieRow]) -> int:
    """Write ``cookies`` to ``path`` in Netscape format.

    Returns the number of rows written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(NETSCAPE_HEADER)
        for row in cookies:
            f.write(row.to_line())
            f.write("\n")
            n += 1
    return n
