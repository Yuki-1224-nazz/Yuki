#!/usr/bin/env python3
"""
main.py -- Orchestrator: scrape, check, save, and auto-send proxies via Telegram.

Reads settings from config.json and runs in a loop.
"""

import asyncio
import json
import os
import sys
import time
import re
from datetime import datetime
from collections import Counter

import aiofiles
from colorama import Fore, Style, init
import pyfiglet

from proxy_scraper import scrape_all
from proxy_checker import ProxyChecker, CheckerConfig, get_next_filename
from telegram_bot import broadcast_proxy_files
from proxy_sources import PROXY_SOURCES
from helpers import read_proxies_from_file

init(autoreset=True)

CONFIG_FILE = "config.json"


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        print(f"{Fore.RED}  [!] {CONFIG_FILE} not found. Using defaults.")
        return {}
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def display_banner():
    os.system("cls" if os.name == "nt" else "clear")
    try:
        banner = pyfiglet.figlet_format("PROXY ULTRA", font="slant")
    except Exception:
        banner = "  PROXY ULTRA\n"
    print(f"{Fore.CYAN}{banner}")
    print(
        f"{Fore.YELLOW}"
        f"  +----------------------------------------------------------+\n"
        f"  |  Auto-Grab + Check + Telegram Bot  v4.0                  |\n"
        f"  +----------------------------------------------------------+"
        f"{Style.RESET_ALL}\n"
    )


def print_report(alive, dead, no_google, elapsed, config):
    total = len(alive) + len(dead) + len(no_google)
    pct_ok = len(alive) / total * 100 if total else 0
    pct_ng = len(no_google) / total * 100 if total else 0
    pct_bad = len(dead) / total * 100 if total else 0

    ms_vals = [ms for _, _, ms, _ in alive]
    g_vals = [gms for _, _, _, gms in alive if gms > 0]
    fastest = min(ms_vals) if ms_vals else 0
    slowest = max(ms_vals) if ms_vals else 0
    avg_ms = sum(ms_vals) / len(ms_vals) if ms_vals else 0
    avg_g = sum(g_vals) / len(g_vals) if g_vals else 0

    print(f"\n{Fore.CYAN}{'=' * 65}")
    print(f"{Fore.YELLOW}                  FINAL REPORT")
    print(f"{Fore.CYAN}{'=' * 65}")
    print(
        f"\n"
        f"  {Fore.WHITE}Total Checked          : {Fore.CYAN}{total}\n"
        f"  {Fore.GREEN}Alive + Google OK      : {len(alive)}  ({pct_ok:.1f}%)\n"
        f"  {Fore.YELLOW}Alive / No Google      : {len(no_google)}  ({pct_ng:.1f}%)\n"
        f"  {Fore.RED}Dead (removed)         : {len(dead)}  ({pct_bad:.1f}%)\n"
        f"  {Fore.YELLOW}Time Elapsed           : {elapsed:.1f}s\n"
        f"  {Fore.CYAN}Speed                  : {total / elapsed:.1f} proxies/sec\n"
    )

    if alive:
        print(f"  {Fore.MAGENTA}Judge Speed Breakdown:")
        fast_count = sum(1 for _, _, ms, _ in alive if ms < 500)
        mid_count = sum(1 for _, _, ms, _ in alive if 500 <= ms < 1000)
        slow_count = sum(1 for _, _, ms, _ in alive if ms >= 1000)
        print(f"     {Fore.GREEN}< 500ms    : {fast_count}")
        print(f"     {Fore.YELLOW}500-999ms  : {mid_count}")
        print(f"     {Fore.RED}>= 1000ms  : {slow_count}")
        print(
            f"\n     Fastest : {Fore.GREEN}{fastest:.0f}ms  "
            f"{Fore.WHITE}Avg : {Fore.YELLOW}{avg_ms:.0f}ms  "
            f"{Fore.WHITE}Slowest : {Fore.RED}{slowest:.0f}ms"
        )
        if config.CHECK_GOOGLE and avg_g > 0:
            print(f"     Avg Google Latency : {Fore.CYAN}{avg_g:.0f}ms")

        type_counts = Counter(ptype for _, ptype, _, _ in alive)
        print(f"\n  {Fore.CYAN}Type Breakdown:")
        for ptype, cnt in sorted(type_counts.items()):
            bar = "#" * min(cnt, 40)
            print(f"     {ptype:<8} : {cnt:>4}  {Fore.BLUE}{bar}")

        print(f"\n  {Fore.GREEN}Top 10 Fastest (Google OK):")
        for i, (proxy, ptype, ms, gms) in enumerate(alive[:10], 1):
            mc = (
                Fore.GREEN
                if ms < 500
                else (Fore.YELLOW if ms < 1000 else Fore.RED)
            )
            gs = f"  {Fore.CYAN}G:{gms:.0f}ms" if config.CHECK_GOOGLE and gms > 0 else ""
            print(
                f"     {i:>2}. {proxy:<22} {Fore.CYAN}{ptype:<8}"
                f"{mc}{ms:>6.0f}ms{gs}"
            )

    print(f"\n  {Fore.GREEN}Output -> {config.OUTPUT_DIR}/")
    print(
        f"     {Fore.GREEN}{config.ALIVE_FILE:<30} ({len(alive)} alive + Google OK)"
    )
    if no_google:
        print(
            f"     {Fore.YELLOW}{config.NO_GOOGLE_FILE:<30} "
            f"({len(no_google)} alive, Google blocked)"
        )
    print(f"     {Fore.RED}{config.DEAD_FILE:<30} ({len(dead)} dead)")
    print(f"\n{Fore.CYAN}{'=' * 65}\n")


async def run_cycle(
    config_obj: CheckerConfig,
    types_to_try: list[str],
    out_scheme: str | None,
    loop_num: int,
    file_path: str | None = None,
) -> tuple:
    config_obj.ALIVE_FILE = "alive_proxies_1.txt"
    config_obj.NO_GOOGLE_FILE = "no_google_proxies.txt"

    proxies = []
    seen_set = set()

    grabbed = await scrape_all()
    for p in grabbed:
        if p not in seen_set:
            proxies.append(p)
            seen_set.add(p)

    if file_path:
        for p in read_proxies_from_file(file_path):
            if p not in seen_set:
                proxies.append(p)
                seen_set.add(p)

    if not proxies:
        print(f"{Fore.RED}  [!] No proxies loaded this cycle.")
        return [], [], []

    print(f"{Fore.GREEN}  [+] [{loop_num}] {len(proxies)} unique proxies -> checking...\n")

    checker = ProxyChecker(config_obj)
    t0 = time.time()
    alive, dead, no_google = await checker.run(proxies, types_to_try)
    elapsed = time.time() - t0

    os.makedirs(config_obj.PROXIES_DIR, exist_ok=True)

    # Save alive proxies (split by type)
    proxy_files = []

    # Main alive file
    alive_path = get_next_filename(config_obj.PROXIES_DIR, config_obj.ALIVE_FILE)
    async with aiofiles.open(alive_path, "w") as f:
        for proxy, ptype, ms, gms in alive:
            line = f"{out_scheme}://{proxy}" if out_scheme else proxy
            await f.write(line + "\n")
    config_obj.ALIVE_FILE = os.path.basename(alive_path)
    proxy_files.append(alive_path)

    # Save per-type files
    type_groups: dict[str, list] = {}
    for proxy, ptype, ms, gms in alive:
        type_groups.setdefault(ptype, []).append((proxy, ms, gms))

    for ptype, items in type_groups.items():
        type_file = os.path.join(
            config_obj.PROXIES_DIR, f"{ptype}_proxies.txt"
        )
        async with aiofiles.open(type_file, "w") as f:
            for proxy, ms, gms in items:
                line = f"{out_scheme}://{proxy}" if out_scheme else proxy
                await f.write(line + "\n")
        proxy_files.append(type_file)

    # Save no-google file
    if no_google:
        ng_path = get_next_filename(config_obj.OUTPUT_DIR, config_obj.NO_GOOGLE_FILE)
        async with aiofiles.open(ng_path, "w") as f:
            await f.write("# Alive but cannot reach Google\n\n")
            for proxy, ptype, ms in no_google:
                line = f"{out_scheme}://{proxy}" if out_scheme else proxy
                await f.write(f"{line} | {ptype} | {ms:.0f}ms\n")
        config_obj.NO_GOOGLE_FILE = os.path.basename(ng_path)
        proxy_files.append(ng_path)

    print_report(alive, dead, no_google, elapsed, config_obj)

    # Auto-send via Telegram
    print(f"{Fore.CYAN}  Sending proxy files via Telegram bot...")
    try:
        await broadcast_proxy_files(
            proxy_files,
            alive_count=len(alive),
            dead_count=len(dead),
            no_google_count=len(no_google),
        )
        print(f"{Fore.GREEN}  [+] Telegram broadcast done.\n")
    except Exception as e:
        print(f"{Fore.YELLOW}  [!] Telegram send error: {e}\n")

    return alive, dead, no_google


async def async_main():
    display_banner()

    user_config = load_config()
    scraper_cfg = user_config.get("scraper", {})

    config = CheckerConfig()
    config.TIMEOUT = scraper_cfg.get("timeout", 5)
    config.MAX_CONCURRENT = scraper_cfg.get("max_concurrent", 300)
    config.CHECK_GOOGLE = scraper_cfg.get("check_google", True)
    config.GOOGLE_TIMEOUT = scraper_cfg.get("google_timeout", 8)
    config.PROXY_TYPES = scraper_cfg.get("proxy_types", ["http", "socks4", "socks5"])

    out_format = scraper_cfg.get("output_format", None)
    refresh_minutes = scraper_cfg.get("refresh_interval_minutes", 30)
    auto_loop = scraper_cfg.get("auto_loop", True)
    types_to_try = config.PROXY_TYPES

    print(f"{Fore.YELLOW}  Configuration loaded from {CONFIG_FILE}:")
    print(f"  {Fore.WHITE}  Types        : {', '.join(types_to_try)}")
    print(f"  {Fore.WHITE}  Concurrency  : {config.MAX_CONCURRENT}")
    print(f"  {Fore.WHITE}  Timeout      : {config.TIMEOUT}s")
    print(f"  {Fore.WHITE}  Google Check : {'Yes' if config.CHECK_GOOGLE else 'No'}")
    print(f"  {Fore.WHITE}  Output Fmt   : {out_format or 'ip:port'}")
    print(f"  {Fore.WHITE}  Auto-Loop    : {'Every ' + str(refresh_minutes) + ' min' if auto_loop else 'Once'}")
    print()

    loop_num = 1
    total_ever = 0

    while True:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{Fore.CYAN}{'=' * 65}")
        if auto_loop:
            print(f"{Fore.YELLOW}  CYCLE #{loop_num}  --  {now}")
        else:
            print(f"{Fore.YELLOW}  Starting  --  {now}")
        print(f"{Fore.CYAN}{'=' * 65}\n")

        alive, dead, no_google = await run_cycle(
            config, types_to_try, out_format, loop_num
        )
        total_ever += len(alive)

        if not auto_loop:
            break

        wait_secs = refresh_minutes * 60
        print(
            f"\n{Fore.CYAN}  Next refresh in {refresh_minutes} min  "
            f"({Fore.GREEN}{total_ever} total alive collected so far{Fore.CYAN})"
        )
        print(f"  {Fore.YELLOW}Press Ctrl+C to stop the loop.\n")

        try:
            for remaining in range(wait_secs, 0, -1):
                mins, secs = divmod(remaining, 60)
                sys.stdout.write(
                    f"\r  {Fore.CYAN}Next run in: {Fore.YELLOW}{mins:02d}:{secs:02d}  "
                    f"{Fore.WHITE}(Cycle #{loop_num} done -- {len(alive)} alive)          "
                )
                sys.stdout.flush()
                await asyncio.sleep(1)
            print()
        except asyncio.CancelledError:
            print(f"\n{Fore.YELLOW}  Loop stopped by user.")
            break

        loop_num += 1

    print(f"\n{Fore.GREEN}  Done! {total_ever} total Google-OK proxies collected.")
    print(f"  {Fore.CYAN}  Output -> {config.OUTPUT_DIR}/{config.ALIVE_FILE}\n")


def main():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}  Force quit. Partial results saved in 'cleaned/'.\n")


if __name__ == "__main__":
    main()
