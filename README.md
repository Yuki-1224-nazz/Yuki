# logs-to-cookie

Telegram bot that converts log archives into [Netscape cookie files](https://curl.se/docs/http-cookies.html)
through a guided, conversational `/start` flow. Designed to handle
multi-GB stealer-log dumps without ever buffering the full file to
disk.

```
START      User sends /start
INPUT       └─► Bot asks for direct download URL
                 └─► Bot asks for archive password (or /skip)
                      └─► Bot asks for keywords filter (or /skip)
PROCESS              └─► Chunked download (64 KB at a time)
                          └─► Parse & extract cookies
                               └─► Convert to Netscape format
                                    └─► One file per cookie set
OUTPUT                                  └─► Bot uploads cookies_result.zip
                                              (or stops with an error
                                               if it exceeds the
                                               Telegram bot upload limit)
FEEDBACK   ⏳ Downloading... ▓▓░░  ⚙ Processing...  🔄 Converting...
            ✅ Done!  /  ❌ Error
```

## The flow

| Phase | What happens |
|---|---|
| **START** | User sends `/start`. The bot greets them and asks for a *direct download URL* to the logs. |
| **INPUT** | The bot collects three things in sequence: the URL, an archive password (skippable), and an optional keyword filter (skippable). |
| **PROCESS** | The bot streams the URL in 64 KB chunks to a temp file, then sniffs the first few bytes to detect zip/7z/rar (so tokenised CDN URLs without `.zip`/`.7z`/`.rar` in the path also work). Archives are extracted with the supplied password; every cookie file inside is parsed. Each detected *cookie set* (one per source file) is emitted as its own Netscape `.txt` file. |
| **OUTPUT** | Every output `.txt` is bundled into `cookies_result.zip` and uploaded as a Telegram document. If the zip exceeds Telegram's 50 MB bot upload limit, the bot stops with a clear error and asks the user to re-run with a stricter keyword filter. |
| **FEEDBACK** | The bot edits a single status message throughout: `⏳ Downloading...` with a progress bar, `⚙ Processing...`, `🔄 Converting...`, and finally `✅ Done!` or `❌ Error <reason>`. |

You can send `/cancel` at any prompt to abort the current job.

## Commands

| Command | What it does |
|---|---|
| `/start`, `/help` | Welcome card + start the conversation. |
| `/skip` | Skip the current prompt (password or keywords). |
| `/cancel` | Abort the current job and reset the conversation. |

## Run locally

```bash
git clone https://github.com/zyblahblah/logs-to-cookie.git
cd logs-to-cookie
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then edit .env with your real BOT_TOKEN
python bot.py
```

You'll need `7z` (from `p7zip-full`) on your `$PATH` for archive
extraction. Recent p7zip handles `.zip`, `.7z`, and `.rar` (RAR4 and
RAR5, including encrypted) so you don't need a separate `unrar`.
On Debian/Ubuntu:

```bash
sudo apt-get install -y p7zip-full
```

## Deploy to Railway

1. Fork or link this repo to a Railway project.
2. Open *Variables* and set:
   - `BOT_TOKEN` — token from [@BotFather](https://t.me/BotFather)
   - `ADMIN_IDS` — comma-separated Telegram user IDs allowed to use the bot. Leave empty to allow everyone (not recommended).
3. Deploy. Railway runs `worker: python bot.py` (see `Procfile`). `railpack.json` installs `p7zip-full` (via apt in the deploy image) so encrypted archives — zip, 7z, and rar — work out of the box; `nixpacks.toml` is kept around for older Railway services / self-hosters that build with Nixpacks.

> **Heads up — Railway uses Railpack, not Nixpacks, by default since
> mid-2025.** That's why an earlier version of this README (which only
> shipped `nixpacks.toml`) deployed fine but failed at runtime with
> `❌ Error: 7z binary not found — install p7zip on your host.`.
> Railpack ignores `nixpacks.toml` entirely; it reads `railpack.json`.
> If you've forked an older copy, copy
> [`railpack.json`](./railpack.json) into the root of your repo and
> redeploy. Note that the Railpack runtime image doesn't ship with
> `multiverse` enabled, so the proprietary `unrar` apt package isn't
> available; we route `.rar` through `p7zip-full` instead, which
> handles RAR4 and RAR5 natively.

> **Rotate your token.** Anyone who has seen your bot token can
> control the bot. If you've ever pasted it in chat, run
> `/revoke` → pick the bot → `/token` in BotFather to issue a fresh
> one before deploying.

## Configuration

All knobs live in environment variables (or `.env` for local runs).
See [`.env.example`](.env.example) for the full list:

- `BOT_TOKEN` *(required)* — from BotFather.
- `ADMIN_IDS` — comma-separated allow-list of Telegram user IDs.
- `DOC_UPLOAD_LIMIT` *(bytes, default 52428800)* — result zips larger than this make the bot stop with a clear error message instead of uploading.
- `MAX_DOWNLOAD_BYTES` *(bytes, default 5368709120)* — refuses inputs larger than this.

## How it works

1. **Streaming.** `requests.get(stream=True)` pulls 64 KB at a time straight into a temp file on disk — the body is never buffered in RAM and the file is what we sniff for the archive type.
2. **Line buffer.** Chunks are decoded as UTF-8 (errors replaced) and split on `\n`. Trailing partial lines are stitched onto the next chunk so cookie rows split across chunk boundaries are never lost.
3. **Detection + extraction.** The first 8 bytes of the saved file are matched against ZIP (`PK\x03\x04` / `PK\x05\x06` / `PK\x07\x08`), 7Z (`7z\xbc\xaf\x27\x1c`) and RAR (`Rar!\x1a\x07`) signatures, so URLs like `https://cdn2.linkforge.xyz/download/AgAD0w22104` (no extension) work just fine. All three archive types are then unpacked with `7z x` — recent p7zip handles RAR4 and RAR5 natively, including encryption. The bot also tries `unrar x` as a last-resort fallback if 7z is unavailable. Either way it fails fast on a wrong password.
4. **Parse.** Every line that matches the 7-column Netscape cookie format (`domain TAB flag TAB path TAB secure TAB expires TAB name TAB value`) is kept. Comments and malformed lines are silently dropped. The `#HttpOnly_` prefix is preserved on the domain column.
5. **Filter.** If keywords were provided, a row is only kept when its raw line contains at least one of them (case-insensitive).
6. **Per-set output.** For archive inputs, every detected cookie file in the archive becomes its own `NNNN_<source-path>.txt` Netscape file. For plain-text URLs, the entire stream is one cookie set → one output file.
7. **Package.** All output files are bundled into `cookies_result.zip` with `ZIP_DEFLATED` (`compresslevel=1`).
8. **Deliver.** If `cookies_result.zip` ≤ `DOC_UPLOAD_LIMIT`, the bot uploads it via `bot.send_document()`. Otherwise the bot stops with a clear `❌ Result zip is too large for Telegram` error and asks the user to re-run with a stricter keyword filter.

## Tests

```bash
pip install -r requirements.txt pytest
pytest
```

The test suite spins up tiny local HTTP servers (no external network)
to exercise the full chunked-download → parse → convert → zip path.
