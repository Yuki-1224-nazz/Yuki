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
from collections import defaultdict
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
from pipeline.resolvers import (
    SUPPORTED_HOSTS,
    ResolveError,
    is_hosted_link,
    resolve_url,
)

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

_active_jobs: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


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
    hosts = ", ".join(f"`{h}`" for h in SUPPORTED_HOSTS)
    text = (
        "👋 *logs-to-cookie* — Netscape cookie converter\n\n"
        "Send me a *download URL* to your logs. I accept:\n"
        f"• File hosting links: {hosts}\n"
        "• Any direct `http(s)` download link\n"
        "• zip, 7z, rar archives\n\n"
        "I'll download it, extract every Netscape cookie I can find, "
        "and send each cookie set back as a `.txt` file in a zip.\n\n"
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
    """Phase 2a — INPUT. User just sent the download URL."""
    text = (update.message.text or "").strip()
    if text.startswith("/"):
        return await cmd_cancel(update, context)
    if not _looks_like_url(text):
        await update.message.reply_text(
            "That doesn't look like an `http(s)` URL. "
            "Send the direct download URL again, or /cancel.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ASK_URL

    context.user_data["url"] = text
    await update.message.reply_text(
        "🔐 Got the link. If the archive is encrypted, send the "
        "*password* now. Otherwise send /skip.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ASK_PASSWORD


async def on_password(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Phase 2b — INPUT. User just answered the password prompt."""
    text = (update.message.text or "").strip()
    if text.startswith("/"):
        if text.lower().startswith("/skip"):
            context.user_data["password"] = None
        elif text.lower().startswith("/cancel"):
            return await cmd_cancel(update, context)
        else:
            return await cmd_cancel(update, context)
    elif text.lower() in ("none", "-", "skip", ""):
        context.user_data["password"] = None
    else:
        context.user_data["password"] = text

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
    url: str = context.user_data.get("url", "")
    password: Optional[str] = context.user_data.get("password")
    keywords: Sequence[str] = context.user_data.get("keywords") or []
    chat_id = update.effective_chat.id

    lock = _active_jobs[chat_id]
    if lock.locked():
        await context.bot.send_message(
            chat_id=chat_id,
            text="⏳ A download is already in progress. Wait for it to "
            "finish or send /cancel.",
        )
        return

    async with lock:
        await _do_download(update, context, url, password, keywords, chat_id)


async def _do_download(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    url: str,
    password: Optional[str],
    keywords: Sequence[str],
    chat_id: int,
) -> None:
    started = time.time()

    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text="⏳ Resolving link...",
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

    # ---- Resolve hosted link to direct URL(s) ----
    try:
        resolved = await resolve_url(url, password=password)
    except ResolveError as exc:
        await _edit(f"❌ {exc}")
        return
    except Exception as exc:
        log.exception("resolve failed for %s", url)
        await _edit(f"❌ Failed to resolve link: {exc}")
        return

    total_files = len(resolved)
    if total_files > 1:
        names = "\n".join(
            f"  • {r.filename or r.url.split('/')[-1]}" for r in resolved
        )
        await _edit(f"📂 Found {total_files} files:\n{names}\n\n⏳ Downloading...")
    else:
        await _edit("⏳ Downloading... (initializing)")

    # ---- Process each resolved file ----
    total_bytes_all = 0
    total_cookies_all = 0
    total_sets_all = 0
    results_to_send: list[tuple[Path, int, int, int, str]] = []
    workdirs: list[Path] = []

    try:
        for file_idx, rf in enumerate(resolved):
            file_label = rf.filename or f"file {file_idx + 1}"
            file_started = time.time()

            def _make_progress(label: str, fidx: int, fstart: float):
                def _post_progress(read: int, total: Optional[int]) -> None:
                    prefix = f"[{fidx + 1}/{total_files}] {label}\n" if total_files > 1 else ""
                    body = (
                        f"{prefix}⏳ Downloading...\n"
                        f"{_progress_bar(read, total, fstart)}\n"
                        f"⏱️ Elapsed: {int(time.time() - started)}s"
                    )
                    loop = asyncio.get_running_loop()
                    asyncio.run_coroutine_threadsafe(_edit(body), loop)
                return _post_progress

            def _make_status(label: str, fidx: int):
                def _post_status(line: str) -> None:
                    prefix = f"[{fidx + 1}/{total_files}] {label}\n" if total_files > 1 else ""
                    elapsed = int(time.time() - started)
                    body = f"{prefix}{line}\n⏱️ Elapsed: {elapsed}s"
                    loop = asyncio.get_running_loop()
                    asyncio.run_coroutine_threadsafe(_edit(body), loop)
                return _post_status

            workdir = Path(tempfile.mkdtemp(prefix="logs2cookie-"))
            workdirs.append(workdir)

            try:
                result = await async_run_pipeline(
                    rf.url,
                    workdir,
                    password=password,
                    keywords=keywords,
                    max_bytes=MAX_DOWNLOAD_BYTES,
                    on_status=_make_status(file_label, file_idx),
                    on_progress=_make_progress(file_label, file_idx, file_started),
                    extra_headers=rf.headers,
                )
            except Exception as exc:
                log.exception("pipeline failed for %s (%s)", rf.url, file_label)
                await _edit(f"❌ Error processing {file_label}: {exc}")
                continue

            total_bytes_all += result.bytes_read

            if result.cookie_count == 0:
                if total_files == 1:
                    elapsed = int(time.time() - started)
                    speed = result.bytes_read / (time.time() - started) if (time.time() - started) > 0 else 0
                    await _edit(
                        "ℹ️ Done — no matching cookies found.\n"
                        f"📡 Read: {_human_bytes(result.bytes_read)} "
                        f"({_human_speed(speed)})\n"
                        f"⏱️ Elapsed: {elapsed}s"
                    )
                continue

            total_cookies_all += result.cookie_count
            total_sets_all += len(result.cookie_files)
            results_to_send.append((
                result.zip_path,
                len(result.cookie_files),
                result.cookie_count,
                result.bytes_read,
                file_label,
            ))

        # ---- Send results ----
        elapsed = int(time.time() - started)
        speed = total_bytes_all / (time.time() - started) if (time.time() - started) > 0 else 0

        if not results_to_send:
            if total_cookies_all == 0 and total_files > 1 and total_bytes_all > 0:
                await _edit(
                    f"ℹ️ Done — processed {total_files} files, "
                    "no matching cookies found.\n"
                    f"📡 Read: {_human_bytes(total_bytes_all)} "
                    f"({_human_speed(speed)})\n"
                    f"⏱️ Elapsed: {elapsed}s"
                )
            return

        sent_count = 0
        for i, (zip_path, sets, cookies, bread, label) in enumerate(results_to_send):
            zip_size = zip_path.stat().st_size

            if zip_size > DOC_UPLOAD_LIMIT:
                await _edit(
                    f"❌ Result zip for {label} is too large for Telegram "
                    f"({_human_bytes(zip_size)} > "
                    f"{_human_bytes(DOC_UPLOAD_LIMIT)}).\n"
                    f"📦 {sets} cookie set(s) — {cookies} cookies\n"
                    "Tip: re-run with a stricter keyword filter."
                )
                continue

            prefix = f"[{i + 1}/{len(results_to_send)}] " if len(results_to_send) > 1 else ""
            await _edit(
                f"{prefix}📤 Uploading result...\n"
                f"📦 {sets} cookie set(s) — {cookies} cookies\n"
                f"📡 zip: {_human_bytes(zip_size)}\n"
                f"⏱️ Elapsed: {elapsed}s"
            )
            with open(zip_path, "rb") as f:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=f,
                    filename=zip_path.name,
                    caption=(
                        f"{prefix}✅ {sets} cookie set(s) — "
                        f"{cookies} cookies\n"
                        f"📡 read: {_human_bytes(bread)} "
                        f"({_human_speed(speed)})\n"
                        f"⏱️ {elapsed}s"
                    ),
                )
            sent_count += 1

        if sent_count == 0:
            await _edit(
                f"❌ Found {total_cookies_all} cookies in "
                f"{total_sets_all} set(s), but all result zips exceeded "
                f"Telegram's {_human_bytes(DOC_UPLOAD_LIMIT)} upload limit.\n"
                "Tip: re-run with a stricter keyword filter."
            )
        else:
            await _edit(
                f"✅ Done! {total_sets_all} set(s), "
                f"{total_cookies_all} cookies from "
                f"{total_files} file(s).\n"
                f"📡 Total: {_human_bytes(total_bytes_all)}\n"
                f"⚡ Avg speed: {_human_speed(speed)}"
            )
    finally:
        context.user_data.clear()
        for wd in workdirs:
            shutil.rmtree(wd, ignore_errors=True)


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
        conversation_timeout=600,
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
        "logs-to-cookie bot starting (admins=%s, doc_limit=%s, max_dl=%s)",
        ADMIN_IDS or "<everyone>",
        _human_bytes(DOC_UPLOAD_LIMIT),
        _human_bytes(MAX_DOWNLOAD_BYTES),
    )
    _check_extractor_binaries()
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
