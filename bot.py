"""Telegram bot — Logs to Netscape Cookie Converter.

Implements the interactive flow shown in
``telegram_bot_cookie_converter_flow.svg``::

    /start
       └─► Bot asks for the direct download URL of the logs
            └─► Bot asks for the archive password (or /skip)
                 └─► Bot asks for keywords to filter on (or /skip)
                      └─► Pipeline runs (chunked download → parse →
                          convert → 1 file per cookie set → zip)
                           └─► Bot returns the zip directly. If the
                               zip is bigger than Telegram's bot upload
                               limit (50 MB by default), the bot stops
                               with a clear error.

Run as ``worker: python bot.py``. ``BOT_TOKEN`` and ``ADMIN_IDS`` are
read from the environment (or a local ``.env`` file).
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
import time
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

from pipeline import run_pipeline
from pipeline.archive import SEVENZIP_BINARIES

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("logs-to-cookie.bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
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


def _progress_bar(read: int, total: Optional[int], width: int = 12) -> str:
    if total and total > 0:
        ratio = min(1.0, read / total)
        filled = int(ratio * width)
        bar = "▓" * filled + "░" * (width - filled)
        pct = f"{ratio * 100:.1f}%"
        return f"{bar} {pct}  ({_human_bytes(read)} / {_human_bytes(total)})"
    return f"░░░░░░░░░░░░ — ({_human_bytes(read)} so far)"


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
        "Send me a *direct download URL* to your logs. I accept any "
        "`http(s)` link — zip, 7z, rar, or even tokenised CDN paths "
        "that don't end in `.zip`/`.7z`/`.rar`. I'll stream it, "
        "extract every Netscape cookie I can find, and send each "
        "cookie set back as its own `.txt` file inside a single zip.\n\n"
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
    """Phase 2a — INPUT. User just sent the download URL.

    Tokenised CDN URLs (LinkForge, Telegram-CDN, file-host paths…)
    rarely carry a ``.zip``/``.7z``/``.rar`` suffix, so we can't tell
    from the URL alone whether the body is encrypted. Always ask for
    the password — the user can /skip if the archive isn't protected.
    """
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
    started = time.time()

    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text="⏳ Downloading...",
    )

    loop = asyncio.get_event_loop()
    last_text = ""

    async def _edit(text: str) -> None:
        nonlocal last_text
        if text == last_text:
            return
        last_text = text
        try:
            await status_msg.edit_text(text)
        except Exception:  # noqa: BLE001
            pass

    def _post_status(line: str) -> None:
        elapsed = int(time.time() - started)
        body = f"{line}\n⏱️ Elapsed: {elapsed}s"
        asyncio.run_coroutine_threadsafe(_edit(body), loop)

    def _post_progress(read: int, total: Optional[int]) -> None:
        elapsed = int(time.time() - started)
        body = (
            "⏳ Downloading...\n"
            f"{_progress_bar(read, total)}\n"
            f"⏱️ Elapsed: {elapsed}s"
        )
        asyncio.run_coroutine_threadsafe(_edit(body), loop)

    workdir = Path(tempfile.mkdtemp(prefix="logs2cookie-"))
    try:
        try:
            result = await loop.run_in_executor(
                None,
                lambda: run_pipeline(
                    url,
                    workdir,
                    password=password,
                    keywords=keywords,
                    max_bytes=MAX_DOWNLOAD_BYTES,
                    on_status=_post_status,
                    on_progress=_post_progress,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("pipeline failed for %s", url)
            await _edit(f"❌ Error: {exc}")
            return

        elapsed = int(time.time() - started)
        if result.cookie_count == 0:
            await _edit(
                "ℹ️ Done — no matching cookies found.\n"
                f"📡 Read: {_human_bytes(result.bytes_read)}\n"
                f"⏱️ Elapsed: {elapsed}s"
            )
            return

        zip_size = result.zip_path.stat().st_size

        if zip_size > DOC_UPLOAD_LIMIT:
            await _edit(
                f"❌ Result zip is too large for Telegram "
                f"({_human_bytes(zip_size)} > "
                f"{_human_bytes(DOC_UPLOAD_LIMIT)}).\n"
                f"📦 {len(result.cookie_files)} cookie set(s) — "
                f"{result.cookie_count} cookies\n"
                "Tip: re-run with a stricter keyword filter to shrink "
                "the result."
            )
            return

        await _edit(
            "📤 Uploading result...\n"
            f"📦 {len(result.cookie_files)} cookie set(s) — "
            f"{result.cookie_count} cookies\n"
            f"📡 zip: {_human_bytes(zip_size)}\n"
            f"⏱️ Elapsed: {elapsed}s"
        )
        with open(result.zip_path, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=result.zip_path.name,
                caption=(
                    f"✅ {len(result.cookie_files)} cookie set(s) — "
                    f"{result.cookie_count} cookies\n"
                    f"📡 read: {_human_bytes(result.bytes_read)}\n"
                    f"⏱️ {elapsed}s"
                ),
            )
        await _edit(
            f"✅ Done! Sent {_human_bytes(zip_size)} "
            f"({len(result.cookie_files)} sets, "
            f"{result.cookie_count} cookies)."
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
    """Warn loudly at startup if the archive extractor isn't on PATH.

    The previous failure mode was: bot deploys cleanly, accepts the
    URL, downloads multi-GB of data, and only then fails with
    ``❌ Error: 7z binary not found``. That's a terrible UX. Surface
    it in the deploy log instead.
    """
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
