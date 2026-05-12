"""
telegram_bot.py -- Telegram bot that auto-sends proxy files to configured chat IDs.

Reads config.json for bot_token and chat_ids.
Sends proxy list files (HTTP, SOCKS4, SOCKS5, alive, etc.) as documents.
"""

import os
import json
import asyncio
import logging

import aiohttp

logger = logging.getLogger(__name__)

CONFIG_FILE = "config.json"


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(
            f"{CONFIG_FILE} not found. Create it with your bot token and chat IDs."
        )
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


async def send_file_to_chat(
    bot_token: str,
    chat_id: str,
    file_path: str,
    caption: str = "",
) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field(
                "document",
                open(file_path, "rb"),
                filename=os.path.basename(file_path),
            )
            if caption:
                data.add_field("caption", caption[:1024])

            async with session.post(url, data=data) as resp:
                result = await resp.json()
                if result.get("ok"):
                    logger.info(
                        f"Sent {os.path.basename(file_path)} to chat {chat_id}"
                    )
                    return True
                else:
                    logger.error(
                        f"Failed to send to {chat_id}: "
                        f"{result.get('description', 'Unknown error')}"
                    )
                    return False
    except Exception as e:
        logger.error(f"Error sending file to {chat_id}: {e}")
        return False


async def send_message_to_chat(
    bot_token: str,
    chat_id: str,
    text: str,
) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "chat_id": str(chat_id),
                "text": text[:4096],
                "parse_mode": "HTML",
            }
            async with session.post(url, json=payload) as resp:
                result = await resp.json()
                return result.get("ok", False)
    except Exception as e:
        logger.error(f"Error sending message to {chat_id}: {e}")
        return False


async def broadcast_proxy_files(
    proxy_files: list[str],
    alive_count: int = 0,
    dead_count: int = 0,
    no_google_count: int = 0,
):
    """Send all proxy files to all configured chat IDs."""
    config = load_config()
    bot_token = config.get("telegram_bot_token", "")
    chat_ids = config.get("chat_ids", [])

    if not bot_token or bot_token == "YOUR_BOT_TOKEN_HERE":
        logger.warning(
            "Bot token not configured in config.json. Skipping Telegram send."
        )
        return

    if not chat_ids or chat_ids == ["CHAT_ID_1", "CHAT_ID_2"]:
        logger.warning(
            "Chat IDs not configured in config.json. Skipping Telegram send."
        )
        return

    summary = (
        f"<b>Proxy Update</b>\n"
        f"Alive: {alive_count}\n"
        f"Dead: {dead_count}\n"
        f"No Google: {no_google_count}\n"
        f"Total files: {len(proxy_files)}"
    )

    for chat_id in chat_ids:
        await send_message_to_chat(bot_token, chat_id, summary)

        for file_path in proxy_files:
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                caption = f"Proxy list: {os.path.basename(file_path)}"
                await send_file_to_chat(bot_token, chat_id, file_path, caption)
                await asyncio.sleep(0.5)

    logger.info(
        f"Broadcast complete: {len(proxy_files)} files to {len(chat_ids)} chats"
    )
