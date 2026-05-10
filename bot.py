"""Telegram bot — Logs to Netscape Cookie Converter.

Implements the interactive flow shown in
``telegram_bot_cookie_converter_flow.svg``::

    /start
       └─► Bot asks for the direct download URL of the logs
            └─► Bot asks for the archive password (or /skip)
                 └─► Bot asks for keywords to filter on (or /skip)
                      └─► Pipeline runs (async download → parse →
                          convert → 1 file per cookie set → zip)
                           └─► Bot returns the zip directly. If the
                               zip is bigger than Telegram's bot upload
                               limit (50 MB by default), the bot stops
                               with a clear error.

Run as ``worker: python bot.py``. ``BOT_TOKEN`` and ``ADMIN_IDS`` are
read from the environment (or a local ``.env`` file).

Performance improvements in v2.1:
- Async aiohttp downloads (8x chunk size, non-blocking I/O)
- Retry with exponential backoff for transient failures
- Faster progress updates (every 0.5s instead of 1.0s)
- Download speed indicator in progress bar
- Concurrent cookie file processing for large archives
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import platform
import shutil
import tempfile
import time
import zipfile
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv
from telegram import BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from pipeline import async_run_pipeline
from pipeline.archive import SEVENZIP_BINARIES, _ensure_unrar
from pipeline.cookies import parse_cookie_line

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("logs-to-cookie.bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8737930830:AAGXlk6NJlH11N0TLOsd7ATuT2Pqo0jl5X8").strip()
DOC_UPLOAD_LIMIT = int(os.getenv("DOC_UPLOAD_LIMIT", str(50 * 1024 * 1024)))
MAX_DOWNLOAD_BYTES = int(
    os.getenv("MAX_DOWNLOAD_BYTES", str(5 * 1024 * 1024 * 1024))
)
DOWNLOAD_CONNECTIONS = int(os.getenv("DOWNLOAD_CONNECTIONS", "32"))

BOT_VERSION = "2.3.0"
_BOOT_TIME = time.time()


# ---------------------------------------------------------------------------
# Job history tracking
# ---------------------------------------------------------------------------
@dataclass
class JobRecord:
    """Lightweight record of a completed or failed job."""
    user_id: int
    username: str
    urls: list[str]
    cookie_count: int
    bytes_read: int
    elapsed: int
    status: str  # "success" | "partial" | "failed" | "no_cookies"
    timestamp: float = field(default_factory=time.time)


_job_history: deque[JobRecord] = deque(maxlen=50)


# ---------------------------------------------------------------------------
# Per-user active job tracking
# ---------------------------------------------------------------------------
_active_jobs: dict[int, str] = {}  # user_id -> status description


# ---------------------------------------------------------------------------
# Conversation states
# ---------------------------------------------------------------------------
ASK_URL = 1
ASK_PASSWORD = 2
ASK_KEYWORDS = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_admins(raw: str) -> set[int]:
    out: set[int] = set()
    for part in (raw or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            log.warning("ignoring non-integer ADMIN_IDS entry: %r", part)
    return out


ADMIN_IDS: set[int] = _parse_admins(os.getenv("ADMIN_IDS", "5028065177,5376199311"))


def _is_admin(update: Update) -> bool:
    if not ADMIN_IDS:
        return True
    user = update.effective_user
    return bool(user and user.id in ADMIN_IDS)


async def _deny_access(update: Update) -> None:
    """Send an access-denied reply to unauthorized users."""
    if update.message:
        await update.message.reply_text(
            "\u26d4 Access denied. You are not authorized to use this bot."
        )


def _looks_like_url(s: str) -> bool:
    try:
        u = urlparse(s)
    except ValueError:
        return False
    return u.scheme in ("http", "https") and bool(u.netloc)


def _extract_urls(text: str) -> list[str]:
    """Extract all URLs from text (comma, space, or newline separated)."""
    urls: list[str] = []
    for part in text.replace(",", " ").split():
        part = part.strip()
        if _looks_like_url(part):
            urls.append(part)
    return urls


def _human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == units[-1]:
            return f"{f:.2f} {u}"
        f /= 1024
    return f"{n} B"


def _human_speed(bytes_per_sec: float) -> str:
    """Format download speed in human-readable form."""
    if bytes_per_sec >= 1024 * 1024:
        return f"{bytes_per_sec / (1024 * 1024):.1f} MB/s"
    if bytes_per_sec >= 1024:
        return f"{bytes_per_sec / 1024:.1f} KB/s"
    return f"{bytes_per_sec:.0f} B/s"


def _progress_bar(
    read: int,
    total: Optional[int],
    started: float,
    width: int = 12,
) -> str:
    elapsed = time.time() - started
    speed = read / elapsed if elapsed > 0.1 else 0
    speed_str = f"⚡ {_human_speed(speed)}" if speed > 0 else ""

    if total and total > 0:
        ratio = min(1.0, read / total)
        filled = int(ratio * width)
        bar = "▓" * filled + "░" * (width - filled)
        pct = f"{ratio * 100:.1f}%"
        eta = ""
        if speed > 0 and ratio < 1.0:
            remaining = (total - read) / speed
            if remaining < 60:
                eta = f"  ~{remaining:.0f}s left"
            else:
                eta = f"  ~{remaining / 60:.1f}min left"
        return (
            f"{bar} {pct}  ({_human_bytes(read)} / {_human_bytes(total)})\n"
            f"{speed_str}{eta}"
        )
    return f"░░░░░░░░░░░░ — ({_human_bytes(read)} so far)\n{speed_str}"


def _split_keywords(text: str) -> list[str]:
    parts: list[str] = []
    for chunk in text.replace(";", ",").split(","):
        for sub in chunk.split():
            sub = sub.strip()
            if sub:
                parts.append(sub)
    return parts


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Phase 1 — START. Greet and ask for the download URL."""
    if not _is_admin(update):
        await _deny_access(update)
        return ConversationHandler.END

    context.user_data.clear()
    text = (
        "👋 *logs-to-cookie* — Netscape cookie converter\n\n"
        "Send me one or more *direct download URLs* to your logs "
        "(comma or space separated). I accept any `http(s)` link "
        "— zip, 7z, rar, tokenised CDN paths, or `gofile.io` "
        "links. I'll download them all in parallel, extract every "
        "Netscape cookie, and send the results back as a single "
        "zip.\n\n"
        "At any time you can send /cancel to abort."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    return ASK_URL


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Show all available commands."""
    if not _is_admin(update):
        await _deny_access(update)
        return ConversationHandler.END

    text = (
        "📋 *Available Commands*\n\n"
        "🔹 /start — Start cookie extraction flow\n"
        "🔹 /help — Show this command list\n"
        "🔹 /skip — Skip password or keywords prompt\n"
        "🔹 /cancel — Cancel current job\n"
        "🔹 /status — Check if a job is running\n"
        "🔹 /settings — View bot configuration\n"
        "🔹 /history — Last 10 completed jobs\n"
        "🔹 /connections — View/set parallel downloads (1–64)\n"
        "🔹 /info — Bot version, uptime, stats\n"
        "\n"
        "💡 *Tips:*\n"
        "• Send multiple links (comma/space separated)\n"
        "• One password works for all links with same password\n"
        "• Multiple keywords → separate zip per keyword\n"
        "• Supports ZIP, 7z, RAR (including RAR5)\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    if update.message:
        await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def on_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Phase 2a — INPUT. User just sent the download URL(s)."""
    text = (update.message.text or "").strip()
    if text.startswith("/"):
        return await cmd_cancel(update, context)

    urls = _extract_urls(text)
    if not urls:
        await update.message.reply_text(
            "No valid `http(s)` URL found. "
            "Send one or more direct download URLs (comma or space "
            "separated), or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ASK_URL

    context.user_data["urls"] = urls
    count = len(urls)
    label = f"🔗 Got {count} link{'s' if count > 1 else ''}"
    if count > 1:
        await update.message.reply_text(
            f"{label}. If the archives are encrypted, send the "
            "*password(s)* now (comma-separated, one per link — "
            "e.g. `pass1, pass2`). If all archives share the same "
            "password, send it once. Otherwise send /skip.",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text(
            f"{label}. If the archive is encrypted, send the "
            "*password* now. Otherwise send /skip.",
            parse_mode=ParseMode.MARKDOWN,
        )
    return ASK_PASSWORD


def _parse_passwords(text: str, num_urls: int) -> list[Optional[str]]:
    """Parse comma-separated passwords and align them with URLs.

    Rules:
    - Single URL → the *entire* text is the password (no splitting).
    - Multiple URLs with comma-separated passwords → map 1-to-1.
    - One password for multiple URLs → reuse for all.
    - Fewer passwords than URLs → remaining get ``None``.
    - Only genuinely empty tokens are treated as no password.
    """
    # Single URL: never split — the whole text is the password.
    if num_urls <= 1:
        return [text] if text else [None]

    raw_parts = [p.strip() for p in text.split(",")]
    passwords: list[Optional[str]] = []
    for p in raw_parts:
        passwords.append(p if p else None)

    if len(passwords) == 1:
        return passwords * num_urls

    # Pad with None if fewer passwords than URLs
    while len(passwords) < num_urls:
        passwords.append(None)

    return passwords[:num_urls]


async def on_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Phase 2b — INPUT. User just answered the password prompt."""
    text = (update.message.text or "").strip()
    urls = context.user_data.get("urls") or []
    num_urls = len(urls) or 1
    if text.startswith("/"):
        if text.lower().startswith("/skip"):
            context.user_data["passwords"] = [None] * num_urls
        elif text.lower().startswith("/cancel"):
            return await cmd_cancel(update, context)
        else:
            return await cmd_cancel(update, context)
    elif not text:
        context.user_data["passwords"] = [None] * num_urls
    else:
        context.user_data["passwords"] = _parse_passwords(text, num_urls)

    await update.message.reply_text(
        "🔎 Send the *keywords* you want to filter cookies by "
        "(comma-separated), or send /skip to keep every cookie.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ASK_KEYWORDS


async def on_keywords(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Phase 2c — INPUT. User just answered the keyword prompt."""
    text = (update.message.text or "").strip()
    if text.startswith("/"):
        if text.lower().startswith("/skip"):
            keywords: list[str] = []
        elif text.lower().startswith("/cancel"):
            return await cmd_cancel(update, context)
        else:
            return await cmd_cancel(update, context)
    elif text.lower() in ("none", "-", "skip", ""):
        keywords = []
    else:
        keywords = _split_keywords(text)

    context.user_data["keywords"] = keywords
    await _run_job(update, context)
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Phase 3-5 — PROCESS, OUTPUT, FEEDBACK
# ---------------------------------------------------------------------------
async def _run_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    urls: list[str] = context.user_data.get("urls") or []
    if not urls:
        url_single = context.user_data.get("url", "")
        if url_single:
            urls = [url_single]
    passwords: list[Optional[str]] = context.user_data.get("passwords") or []
    # Backward compat: old-style single "password" key
    if not passwords:
        legacy = context.user_data.get("password")
        passwords = [legacy] * len(urls)
    # Ensure passwords list matches urls length
    while len(passwords) < len(urls):
        passwords.append(None)
    passwords = passwords[: len(urls)]
    keywords: Sequence[str] = context.user_data.get("keywords") or []
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    started = time.time()
    num_urls = len(urls)

    _active_jobs[user_id] = f"Downloading {num_urls} link{'s' if num_urls > 1 else ''}..."

    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"⏳ Downloading {num_urls} link{'s' if num_urls > 1 else ''}... (initializing)",
    )

    last_text = ""
    _edit_lock = asyncio.Lock()

    async def _edit(text: str) -> None:
        nonlocal last_text
        async with _edit_lock:
            if text == last_text:
                return
            last_text = text
            try:
                await status_msg.edit_text(text)
            except Exception:
                pass

    # Per-URL progress tracking for concurrent downloads
    _url_progress: dict[int, tuple[int, Optional[int]]] = {}
    _progress_lock = asyncio.Lock()

    def _make_progress_cb(idx: int):
        def _cb(read: int, total: Optional[int]) -> None:
            _url_progress[idx] = (read, total)
            total_read = sum(r for r, _ in _url_progress.values())
            total_size_known = all(t is not None for _, t in _url_progress.values())
            total_size = sum(t for _, t in _url_progress.values() if t is not None) if total_size_known else None
            label = f"⏳ Downloading... ({len(_url_progress)}/{num_urls} active)"
            body = (
                f"{label}\n"
                f"{_progress_bar(total_read, total_size, started)}\n"
                f"⏱️ Elapsed: {int(time.time() - started)}s"
            )
            loop = asyncio.get_running_loop()
            asyncio.run_coroutine_threadsafe(_edit(body), loop)
        return _cb

    def _make_status_cb(idx: int):
        def _cb(line: str) -> None:
            prefix = f"[{idx + 1}/{num_urls}] " if num_urls > 1 else ""
            elapsed = int(time.time() - started)
            body = f"{prefix}{line}\n⏱️ Elapsed: {elapsed}s"
            loop = asyncio.get_running_loop()
            asyncio.run_coroutine_threadsafe(_edit(body), loop)
        return _cb

    workdir = Path(tempfile.mkdtemp(prefix="logs2cookie-"))
    try:
        # Run all URLs concurrently
        # When multiple keywords: download without filtering, then split per keyword
        pipeline_keywords = keywords if len(keywords) <= 1 else []

        async def _run_one(idx: int, url: str, pwd: Optional[str]) -> Optional[object]:
            sub_workdir = workdir / f"job_{idx}"
            try:
                return await async_run_pipeline(
                    url,
                    sub_workdir,
                    password=pwd,
                    keywords=pipeline_keywords,
                    max_bytes=MAX_DOWNLOAD_BYTES,
                    on_status=_make_status_cb(idx),
                    on_progress=_make_progress_cb(idx),
                    num_connections=DOWNLOAD_CONNECTIONS,
                    skip_zip=True,
                )
            except Exception as exc:
                log.exception("pipeline failed for %s", url)
                return exc

        tasks = [_run_one(i, u, passwords[i]) for i, u in enumerate(urls)]
        raw_results = await asyncio.gather(*tasks)

        _active_jobs[user_id] = "Processing results..."

        # Merge results
        from pipeline.pipeline import PipelineResult
        total_bytes_read = 0
        all_cookie_files: list[Path] = []
        total_cookie_count = 0
        errors: list[str] = []
        output_dir = workdir / "merged_output"
        cookies_dir = output_dir / "cookies"
        cookies_dir.mkdir(parents=True, exist_ok=True)

        for idx, res in enumerate(raw_results):
            if isinstance(res, Exception):
                errors.append(f"Link {idx + 1}: {res}")
                continue
            if not isinstance(res, PipelineResult):
                continue
            total_bytes_read += res.bytes_read
            total_cookie_count += res.cookie_count
            for src_file in res.cookie_files:
                dst = cookies_dir / f"link{idx + 1}_{src_file.name}"
                shutil.copy2(src_file, dst)
                all_cookie_files.append(dst)

        elapsed = int(time.time() - started)
        speed = total_bytes_read / (time.time() - started) if (time.time() - started) > 0 else 0

        if errors and not all_cookie_files:
            error_text = "\n".join(errors)
            await _edit(f"❌ All downloads failed:\n{error_text}")
            _job_history.append(JobRecord(
                user_id=user_id, username=username, urls=urls,
                cookie_count=0, bytes_read=0,
                elapsed=int(time.time() - started), status="failed",
            ))
            return

        if errors:
            error_text = "\n".join(errors)
            loop = asyncio.get_running_loop()
            asyncio.run_coroutine_threadsafe(
                _edit(f"⚠️ Some links failed:\n{error_text}\nProcessing successful ones..."),
                loop,
            )

        if total_cookie_count == 0:
            await _edit(
                "ℹ️ Done — no matching cookies found.\n"
                f"📡 Read: {_human_bytes(total_bytes_read)} "
                f"({_human_speed(speed)})\n"
                f"⏱️ Elapsed: {elapsed}s"
            )
            _job_history.append(JobRecord(
                user_id=user_id, username=username, urls=urls,
                cookie_count=0, bytes_read=total_bytes_read,
                elapsed=elapsed, status="no_cookies",
            ))
            return

        # --- Per-keyword zip splitting (multiple keywords) ---
        if len(keywords) > 1:
            from concurrent.futures import ThreadPoolExecutor

            _COOKIE_HEADER = (
                "# Netscape HTTP Cookie File\n"
                "# https://curl.se/docs/http-cookies.html\n"
                "# This is a generated file. Do not edit.\n\n"
            )

            _active_jobs[user_id] = "Splitting by keywords..."
            await _edit(
                f"🔄 Classifying {len(all_cookie_files):,} files "
                f"by {len(keywords)} keywords..."
            )

            # Precompute lowercase keywords once
            kw_lower = [(kw, kw.strip().lower()) for kw in keywords]

            def _classify_one(
                item: tuple[int, Path],
            ) -> tuple[int, str, dict[str, str]]:
                """Read one cookie file, return per-keyword content
                as ready-to-zip strings (no intermediate files)."""
                idx, src_path = item
                sp = str(src_path)
                try:
                    sz = os.path.getsize(sp)
                    if sz == 0 or sz > 512 * 1024:
                        return idx, src_path.name, {}
                    fd = os.open(sp, os.O_RDONLY)
                    try:
                        raw = os.read(fd, 512 * 1024)
                    finally:
                        os.close(fd)
                except OSError:
                    return idx, src_path.name, {}
                text = raw.decode("utf-8", errors="replace")
                per_kw: dict[str, list] = {}
                for line in text.splitlines():
                    row = parse_cookie_line(line)
                    if row is None:
                        continue
                    low = line.lower()
                    for kw, kw_lo in kw_lower:
                        if kw_lo in low:
                            per_kw.setdefault(kw, []).append(row)
                # Convert rows to content strings immediately
                per_kw_content: dict[str, str] = {}
                for kw, rows in per_kw.items():
                    lines = [_COOKIE_HEADER]
                    for r in rows:
                        lines.append(r.to_line())
                        lines.append("\n")
                    per_kw_content[kw] = "".join(lines)
                return idx, src_path.name, per_kw_content

            loop = asyncio.get_running_loop()
            workers = min(32, max(4, len(all_cookie_files) // 50))
            classify_start = time.time()
            n_files = len(all_cookie_files)

            items = [
                (i + 1, all_cookie_files[i])
                for i in range(n_files)
            ]
            all_classified: list[tuple[int, str, dict[str, str]]] = []
            batch_sz = min(500, max(100, n_files // 20))
            done = 0
            last_edit = time.time()
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for bs in range(0, len(items), batch_sz):
                    batch = items[bs : bs + batch_sz]
                    futs = [
                        loop.run_in_executor(pool, _classify_one, it)
                        for it in batch
                    ]
                    results = await asyncio.gather(*futs)
                    all_classified.extend(results)
                    done += len(batch)
                    now = time.time()
                    if now - last_edit >= 2:
                        last_edit = now
                        await _edit(
                            f"🔄 Classifying... "
                            f"{done:,}/{n_files:,} files "
                            f"({now - classify_start:.0f}s)"
                        )

            # Build per-keyword zips DIRECTLY in memory (no disk I/O)
            sent_count = 0
            grand_cookie_count = 0
            for kw_idx, kw in enumerate(keywords, start=1):
                # Collect matching entries for this keyword
                kw_entries: list[tuple[str, str]] = []  # (arcname, content)
                kw_count = 0
                for idx, name, per_kw_content in all_classified:
                    content = per_kw_content.get(kw)
                    if not content:
                        continue
                    arcname = f"cookies/{idx:04d}_{name}"
                    kw_entries.append((arcname, content))
                    kw_count += content.count("\t") // 6  # fast row count
                if not kw_entries:
                    continue
                grand_cookie_count += kw_count
                await _edit(
                    f"📤 Sending {kw}_Cookies.zip "
                    f"({kw_idx}/{len(keywords)}) — "
                    f"{len(kw_entries)} sets, {kw_count:,} cookies..."
                )

                # Build zip in memory — no disk writes
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(
                    zip_buf, "w", compression=zipfile.ZIP_STORED,
                ) as z:
                    for arcname, content in kw_entries:
                        z.writestr(arcname, content)
                zip_size = zip_buf.tell()

                if zip_size <= DOC_UPLOAD_LIMIT:
                    zip_buf.seek(0)
                    await context.bot.send_document(
                        chat_id=chat_id,
                        document=zip_buf,
                        filename=f"{kw}_Cookies.zip",
                        caption=(
                            f"🔑 {kw} — {len(kw_entries)} set(s), "
                            f"{kw_count:,} cookies"
                        ),
                    )
                    sent_count += 1
                else:
                    # Split into parts in memory
                    tgt = int(DOC_UPLOAD_LIMIT * 0.8)
                    entries_per_part = max(
                        1,
                        int(len(kw_entries) * tgt / zip_size),
                    )
                    for ps in range(0, len(kw_entries), entries_per_part):
                        part = kw_entries[ps : ps + entries_per_part]
                        pi = ps // entries_per_part
                        pn = (
                            f"{kw}_Cookies.zip" if pi == 0
                            else f"{kw}_Cookies_{pi}.zip"
                        )
                        pbuf = io.BytesIO()
                        with zipfile.ZipFile(
                            pbuf, "w",
                            compression=zipfile.ZIP_STORED,
                        ) as z:
                            for arcname, content in part:
                                z.writestr(arcname, content)
                        if pbuf.tell() > DOC_UPLOAD_LIMIT:
                            continue
                        pbuf.seek(0)
                        await context.bot.send_document(
                            chat_id=chat_id,
                            document=pbuf,
                            filename=pn,
                            caption=(
                                f"🔑 {kw} part {pi + 1} — "
                                f"{len(part)} set(s)"
                            ),
                        )
                        sent_count += 1
            elapsed = int(time.time() - started)
            await _edit(
                f"✅ Done! Sent {sent_count} keyword zip(s) "
                f"({grand_cookie_count:,} cookies total).\n"
                f"⚡ Avg speed: {_human_speed(speed)}"
            )
            job_status = "success" if not errors else "partial"
            _job_history.append(JobRecord(
                user_id=user_id, username=username, urls=urls,
                cookie_count=grand_cookie_count, bytes_read=total_bytes_read,
                elapsed=elapsed, status=job_status,
            ))
        else:
            # --- Single or split zip (0 or 1 keyword) ---
            # Build one zip first; if too large, split into parts
            zip_path = output_dir / "cookies_result.zip"
            with zipfile.ZipFile(
                zip_path, "w", compression=zipfile.ZIP_STORED,
            ) as z:
                for p in all_cookie_files:
                    z.write(p, arcname=p.relative_to(output_dir).as_posix())

            zip_size = zip_path.stat().st_size

            if zip_size <= DOC_UPLOAD_LIMIT:
                # Single zip fits — send it
                await _edit(
                    "📤 Uploading result...\n"
                    f"📦 {len(all_cookie_files)} cookie set(s) — "
                    f"{total_cookie_count} cookies\n"
                    f"📡 zip: {_human_bytes(zip_size)}\n"
                    f"⏱️ Elapsed: {elapsed}s"
                )
                with open(zip_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=chat_id,
                        document=f,
                        filename=zip_path.name,
                        caption=(
                            f"✅ {len(all_cookie_files)} cookie set(s) — "
                            f"{total_cookie_count} cookies\n"
                            f"📡 read: {_human_bytes(total_bytes_read)} "
                            f"({_human_speed(speed)})\n"
                            f"⏱️ {elapsed}s"
                        ),
                    )
                await _edit(
                    f"✅ Done! Sent {_human_bytes(zip_size)} "
                    f"({len(all_cookie_files)} sets, "
                    f"{total_cookie_count} cookies).\n"
                    f"⚡ Avg speed: {_human_speed(speed)}"
                )
            else:
                # Zip too large — split into multiple parts
                await _edit(
                    f"📦 Result too large for one file "
                    f"({_human_bytes(zip_size)}). "
                    "Splitting into parts..."
                )
                # Target ~40 MB per part to stay under 50 MB limit
                target_part_size = int(DOC_UPLOAD_LIMIT * 0.8)
                files_per_part = max(
                    1,
                    int(len(all_cookie_files) * target_part_size / zip_size),
                )
                parts_sent = 0
                for part_start in range(0, len(all_cookie_files), files_per_part):
                    part_files = all_cookie_files[
                        part_start:part_start + files_per_part
                    ]
                    part_idx = part_start // files_per_part
                    if part_idx == 0:
                        part_name = "cookies_result.zip"
                    else:
                        part_name = f"cookies_result_{part_idx}.zip"
                    part_path = output_dir / part_name
                    with zipfile.ZipFile(
                        part_path, "w",
                        compression=zipfile.ZIP_STORED,
                    ) as z:
                        for p in part_files:
                            z.write(
                                p,
                                arcname=p.relative_to(output_dir).as_posix(),
                            )
                    part_size = part_path.stat().st_size
                    if part_size > DOC_UPLOAD_LIMIT:
                        # Still too large — skip with message
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⚠️ Part `{part_name}` is still too large "
                                f"({_human_bytes(part_size)}), skipping."
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        continue
                    with open(part_path, "rb") as f:
                        await context.bot.send_document(
                            chat_id=chat_id,
                            document=f,
                            filename=part_name,
                            caption=(
                                f"📦 Part {part_idx + 1} — "
                                f"{len(part_files)} set(s), "
                                f"{_human_bytes(part_size)}"
                            ),
                        )
                    parts_sent += 1

                await _edit(
                    f"✅ Done! Sent {parts_sent} zip part(s) "
                    f"({len(all_cookie_files)} sets, "
                    f"{total_cookie_count} cookies).\n"
                    f"⚡ Avg speed: {_human_speed(speed)}"
                )

            job_status = "success" if not errors else "partial"
            _job_history.append(JobRecord(
                user_id=user_id, username=username, urls=urls,
                cookie_count=total_cookie_count, bytes_read=total_bytes_read,
                elapsed=elapsed, status=job_status,
            ))
    finally:
        _active_jobs.pop(user_id, None)
        context.user_data.clear()
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Utility commands
# ---------------------------------------------------------------------------
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current job status for the requesting user."""
    if not _is_admin(update):
        await _deny_access(update)
        return
    user_id = update.effective_user.id
    job_status = _active_jobs.get(user_id)
    if job_status:
        await update.message.reply_text(
            f"\U0001f504 *Active job:* {job_status}",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text("\u2705 No active job running.")


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current bot configuration."""
    if not _is_admin(update):
        await _deny_access(update)
        return
    sevenzip = (
        shutil.which("7zz") or shutil.which("7z") or shutil.which("7za")
        or "not found"
    )
    unrar = _ensure_unrar() or "not found"
    text = (
        "\u2699\ufe0f *Bot Settings*\n\n"
        f"\u2022 Max download size: `{_human_bytes(MAX_DOWNLOAD_BYTES)}`\n"
        f"\u2022 Upload limit: `{_human_bytes(DOC_UPLOAD_LIMIT)}`\n"
        f"\u2022 Download connections: `{DOWNLOAD_CONNECTIONS}`\n"
        f"\u2022 Admin IDs: `{', '.join(str(i) for i in ADMIN_IDS) or 'everyone'}`\n"
        f"\u2022 7z binary: `{sevenzip}`\n"
        f"\u2022 unrar binary: `{unrar}`\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show recent job history."""
    if not _is_admin(update):
        await _deny_access(update)
        return
    if not _job_history:
        await update.message.reply_text("\U0001f4cb No jobs recorded yet.")
        return

    lines: list[str] = ["\U0001f4cb *Recent Jobs*\n"]
    for i, job in enumerate(reversed(list(_job_history)), start=1):
        ts = time.strftime("%Y-%m-%d %H:%M", time.gmtime(job.timestamp))
        icon = {
            "success": "\u2705",
            "partial": "\u26a0\ufe0f",
            "failed": "\u274c",
            "no_cookies": "\u2139\ufe0f",
        }.get(job.status, "\u2022")
        links_label = f"{len(job.urls)} link{'s' if len(job.urls) > 1 else ''}"
        lines.append(
            f"{i}\\. {icon} `{ts}` \u2014 {links_label}, "
            f"{job.cookie_count} cookies, {_human_bytes(job.bytes_read)}, "
            f"{job.elapsed}s ({job.status})"
        )
        if i >= 10:
            remaining = len(_job_history) - 10
            if remaining > 0:
                lines.append(f"\n_\\.\\.\\.\\. and {remaining} older job(s)_")
            break
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_connections(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """View or set the number of parallel download connections."""
    global DOWNLOAD_CONNECTIONS
    if not _is_admin(update):
        await _deny_access(update)
        return
    args = context.args
    if args and args[0].isdigit():
        new_val = max(1, min(64, int(args[0])))
        DOWNLOAD_CONNECTIONS = new_val
        await update.message.reply_text(
            f"\u2705 Download connections set to `{new_val}`.",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text(
            f"\U0001f310 Current download connections: `{DOWNLOAD_CONNECTIONS}`\n"
            "Usage: `/connections <number>` (1\u201364)",
            parse_mode=ParseMode.MARKDOWN,
        )


async def cmd_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot version and uptime."""
    if not _is_admin(update):
        await _deny_access(update)
        return
    uptime_secs = int(time.time() - _BOOT_TIME)
    hours, remainder = divmod(uptime_secs, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        uptime_str = f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        uptime_str = f"{minutes}m {secs}s"
    else:
        uptime_str = f"{secs}s"

    jobs_total = len(_job_history)
    jobs_ok = sum(1 for j in _job_history if j.status == "success")

    text = (
        "\u2139\ufe0f *Bot Info*\n\n"
        f"\u2022 Version: `{BOT_VERSION}`\n"
        f"\u2022 Python: `{platform.python_version()}`\n"
        f"\u2022 Uptime: `{uptime_str}`\n"
        f"\u2022 Jobs this session: `{jobs_total}` ({jobs_ok} successful)\n"
        f"\u2022 OS: `{platform.system()} {platform.release()}`\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------
def build_app() -> Application:
    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN is not set. Add it to .env or your hosting "
            "provider's environment variables."
        )

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CommandHandler("help", cmd_help),
        ],
        states={
            ASK_URL: [
                CommandHandler("cancel", cmd_cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_url),
            ],
            ASK_PASSWORD: [
                CommandHandler("cancel", cmd_cancel),
                CommandHandler("skip", on_password),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_password),
            ],
            ASK_KEYWORDS: [
                CommandHandler("cancel", cmd_cancel),
                CommandHandler("skip", on_keywords),
                MessageHandler(filters.TEXT & ~filters.COMMAND, on_keywords),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        name="logs2cookie_conv",
        persistent=False,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("connections", cmd_connections))
    app.add_handler(CommandHandler("info", cmd_info))
    return app


def _check_extractor_binaries() -> None:
    """Ensure extraction tools are available at startup."""
    def _first_on_path(candidates: Sequence[str]) -> Optional[str]:
        for c in candidates:
            p = shutil.which(c)
            if p:
                return p
        return None

    sevenzip = _first_on_path(SEVENZIP_BINARIES)
    if sevenzip is None:
        log.warning(
            "7z binary not found on PATH (looked for %s). "
            "All archive extraction (zip / 7z / rar) will fail at "
            "runtime. Install 7zip (apt-get install 7zip) or "
            "p7zip-full on your host.",
            ", ".join(SEVENZIP_BINARIES),
        )
    else:
        log.info("7z binary OK: %s (handles zip, 7z, rar)", sevenzip)

    # Pre-download unrar so RAR5 extraction doesn't delay the first job
    unrar_path = _ensure_unrar()
    if unrar_path:
        log.info("unrar binary OK: %s (handles RAR5)", unrar_path)
    else:
        log.warning(
            "unrar binary not available — RAR5 archives may fail to "
            "extract. 7z will be tried as fallback."
        )


async def _post_init(application: Application) -> None:
    """Register the bot menu commands after the application starts."""
    await application.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("help", "Show all commands"),
        BotCommand("status", "Check current job status"),
        BotCommand("settings", "View bot configuration"),
        BotCommand("history", "Recent job history"),
        BotCommand("connections", "View/set download connections"),
        BotCommand("info", "Bot version & uptime"),
        BotCommand("cancel", "Cancel current job"),
    ])
    log.info("Bot menu commands registered.")


def main() -> None:
    app = build_app()
    log.info(
        "logs-to-cookie bot starting (admins=%s, doc_limit=%s, max_dl=%s, "
        "connections=%d)",
        ADMIN_IDS or "<everyone>",
        _human_bytes(DOC_UPLOAD_LIMIT),
        _human_bytes(MAX_DOWNLOAD_BYTES),
        DOWNLOAD_CONNECTIONS,
    )
    _check_extractor_binaries()
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
