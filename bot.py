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
import logging
import os
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv
from telegram import Update
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
from pipeline.archive import SEVENZIP_BINARIES

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("logs-to-cookie.bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "8661120242:AAEO3UUHVKrcQha_XCGsVA0tJJq3qy5_8YQ").strip()
DOC_UPLOAD_LIMIT = int(os.getenv("DOC_UPLOAD_LIMIT", str(50 * 1024 * 1024)))
MAX_DOWNLOAD_BYTES = int(
    os.getenv("MAX_DOWNLOAD_BYTES", str(5 * 1024 * 1024 * 1024))
)
DOWNLOAD_CONNECTIONS = int(os.getenv("DOWNLOAD_CONNECTIONS", "16"))


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


ADMIN_IDS: set[int] = _parse_admins(os.getenv("ADMIN_IDS", ""))


def _is_admin(update: Update) -> bool:
    if not ADMIN_IDS:
        return True
    user = update.effective_user
    return bool(user and user.id in ADMIN_IDS)


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
    return await cmd_start(update, context)


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
    - One password supplied → reuse it for every URL.
    - N passwords supplied (N == num_urls) → map 1-to-1.
    - Fewer or more passwords than URLs → map by index; extras are
      dropped, missing ones default to ``None``.
    - Empty / whitespace-only tokens are treated as *no password*.
    """
    raw_parts = [p.strip() for p in text.split(",")]
    passwords: list[Optional[str]] = []
    for p in raw_parts:
        if not p or p.lower() in ("none", "-", "skip"):
            passwords.append(None)
        else:
            passwords.append(p)

    if len(passwords) == 1:
        return passwords * num_urls

    # Pad with None if fewer passwords than URLs
    while len(passwords) < num_urls:
        passwords.append(None)

    return passwords[:num_urls]


async def on_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Phase 2b — INPUT. User just answered the password prompt."""
    text = (update.message.text or "").strip()
    num_urls = len(context.user_data.get("urls") or [1])
    if text.startswith("/"):
        if text.lower().startswith("/skip"):
            context.user_data["passwords"] = [None] * num_urls
        elif text.lower().startswith("/cancel"):
            return await cmd_cancel(update, context)
        else:
            return await cmd_cancel(update, context)
    elif text.lower() in ("none", "-", "skip", ""):
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
    started = time.time()
    num_urls = len(urls)

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
        async def _run_one(idx: int, url: str, pwd: Optional[str]) -> Optional[object]:
            sub_workdir = workdir / f"job_{idx}"
            try:
                return await async_run_pipeline(
                    url,
                    sub_workdir,
                    password=pwd,
                    keywords=keywords,
                    max_bytes=MAX_DOWNLOAD_BYTES,
                    on_status=_make_status_cb(idx),
                    on_progress=_make_progress_cb(idx),
                    num_connections=DOWNLOAD_CONNECTIONS,
                )
            except Exception as exc:
                log.exception("pipeline failed for %s", url)
                return exc

        tasks = [_run_one(i, u, passwords[i]) for i, u in enumerate(urls)]
        raw_results = await asyncio.gather(*tasks)

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
            return

        # Zip merged results
        zip_path = output_dir / "cookies_result.zip"
        with zipfile.ZipFile(
            zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1,
        ) as z:
            for p in all_cookie_files:
                z.write(p, arcname=p.relative_to(output_dir).as_posix())

        zip_size = zip_path.stat().st_size

        if zip_size > DOC_UPLOAD_LIMIT:
            await _edit(
                f"❌ Result zip is too large for Telegram "
                f"({_human_bytes(zip_size)} > "
                f"{_human_bytes(DOC_UPLOAD_LIMIT)}).\n"
                f"📦 {len(all_cookie_files)} cookie set(s) — "
                f"{total_cookie_count} cookies\n"
                "Tip: re-run with a stricter keyword filter to shrink "
                "the result."
            )
            return

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
    finally:
        context.user_data.clear()
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------
def build_app() -> Application:
    if not BOT_TOKEN:
        raise SystemExit(
            "BOT_TOKEN is not set. Add it to .env or your hosting "
            "provider's environment variables."
        )

    app = Application.builder().token(BOT_TOKEN).build()

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
    return app


def _check_extractor_binaries() -> None:
    """Warn loudly at startup if the archive extractor isn't on PATH."""
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
            "runtime. Install p7zip-full on your host (Railway: see "
            "railpack.json; Debian/Ubuntu: apt-get install p7zip-full).",
            ", ".join(SEVENZIP_BINARIES),
        )
    else:
        log.info("7z binary OK: %s (handles zip, 7z, rar)", sevenzip)


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
