# Proxy Scraper + Checker + Telegram Bot

Auto-scrape HTTP, HTTPS, SOCKS4, SOCKS5 proxies from 20+ free sources, check them, and auto-send the results via Telegram bot.

## Features

- **Multi-source scraper** -- grabs proxies from ProxyScrape, Proxifly, GeoNode, iplocate, pubproxy, litport, redscrape, free-proxy-list.net
- **Async checker** -- validates proxies against judge servers + Google reachability test
- **Per-type output** -- saves separate files for HTTP, SOCKS4, SOCKS5 proxies
- **Telegram bot** -- auto-sends proxy files to configured chat IDs
- **Auto-loop** -- refreshes and re-checks on a configurable interval
- **config.json** -- put your bot token, chat IDs, and scraper settings in one file

## Project Structure

```
proxy-scraper-telegram-bot/
|-- config.json          # Bot token, chat IDs, scraper settings
|-- main.py              # Entry point / orchestrator
|-- proxy_scraper.py     # Scrapes proxies from online sources
|-- proxy_checker.py     # Checks proxies (judge + Google test)
|-- proxy_sources.py     # All proxy source URLs
|-- helpers.py           # Shared utilities (validation, connectors)
|-- telegram_bot.py      # Telegram bot (auto-send files)
|-- requirements.txt     # Python dependencies
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure `config.json`

Edit `config.json` with your Telegram bot token and chat IDs:

```json
{
    "telegram_bot_token": "123456:ABC-DEF...",
    "chat_ids": [
        "123456789",
        "-1001234567890"
    ],
    "scraper": {
        "timeout": 5,
        "max_concurrent": 300,
        "check_google": true,
        "google_timeout": 8,
        "proxy_types": ["http", "socks4", "socks5"],
        "output_format": null,
        "refresh_interval_minutes": 30,
        "auto_loop": true
    }
}
```

**How to get your bot token:**
1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow the prompts
3. Copy the token into `config.json`

**How to get chat IDs:**
1. Message [@userinfobot](https://t.me/userinfobot) on Telegram to get your personal chat ID
2. For groups: add the bot to the group, then use `https://api.telegram.org/bot<TOKEN>/getUpdates` to find the group chat ID

### 3. Run

```bash
python main.py
```

The bot will:
1. Scrape proxies from all sources
2. Check each proxy (judge + Google test)
3. Save results to `proxies/` and `cleaned/` directories
4. Auto-send proxy files to all configured Telegram chat IDs
5. Wait and repeat (if auto_loop is enabled)

## Output Files

- `proxies/alive_proxies_N.txt` -- all alive proxies (Google OK)
- `proxies/http_proxies.txt` -- HTTP proxies only
- `proxies/socks4_proxies.txt` -- SOCKS4 proxies only
- `proxies/socks5_proxies.txt` -- SOCKS5 proxies only
- `cleaned/dead_proxies.txt` -- dead proxies
- `cleaned/no_google_proxies.txt` -- alive but Google-blocked

## Config Options

| Key | Default | Description |
|-----|---------|-------------|
| `telegram_bot_token` | `""` | Telegram bot API token |
| `chat_ids` | `[]` | List of Telegram chat IDs to send files to |
| `timeout` | `5` | Proxy check timeout in seconds |
| `max_concurrent` | `300` | Max concurrent proxy checks |
| `check_google` | `true` | Test Google reachability |
| `google_timeout` | `8` | Google check timeout |
| `proxy_types` | `["http","socks4","socks5"]` | Types to check |
| `output_format` | `null` | Output format (`null`=ip:port, `"http"`, `"socks5"`, etc.) |
| `refresh_interval_minutes` | `30` | Minutes between auto-refresh cycles |
| `auto_loop` | `true` | Enable continuous scraping loop |
