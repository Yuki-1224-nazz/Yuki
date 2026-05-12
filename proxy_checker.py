"""
proxy_checker.py -- Check proxies against judge servers and Google.
"""

import asyncio
import time
import sys
import os

import aiohttp
import aiofiles
from colorama import Fore

from helpers import get_connector


class CheckerConfig:
    TIMEOUT = 5
    MAX_CONCURRENT = 300
    OUTPUT_DIR = "cleaned"
    PROXIES_DIR = "proxies"
    ALIVE_FILE = "alive_proxies.txt"
    DEAD_FILE = "dead_proxies.txt"
    NO_GOOGLE_FILE = "no_google_proxies.txt"
    JUDGES = [
        "http://api.ipify.org?format=json",
        "http://checkip.amazonaws.com",
        "http://ip-api.com/json",
        "http://httpbin.org/ip",
    ]
    GOOGLE_TARGETS = [
        "https://www.google.com/generate_204",
        "https://www.google.com",
    ]
    GOOGLE_TIMEOUT = 8
    CHECK_GOOGLE = True
    PROXY_TYPES = ["http", "socks4", "socks5"]


class ProxyChecker:
    def __init__(self, config: CheckerConfig):
        self.config = config
        self._judge_sem = asyncio.Semaphore(config.MAX_CONCURRENT)
        self._google_sem = asyncio.Semaphore(max(config.MAX_CONCURRENT // 2, 50))
        self.alive: list[tuple[str, str, float, float]] = []
        self.dead: list[str] = []
        self.no_google: list[tuple[str, str, float]] = []
        self._lock = asyncio.Lock()
        self._judge_idx = 0
        self._checked = 0
        self._total = 0
        self._shutdown = False
        self._start_time = 0.0
        self._display_lock = asyncio.Lock()

    def _next_judge(self) -> str:
        j = self.config.JUDGES[self._judge_idx % len(self.config.JUDGES)]
        self._judge_idx += 1
        return j

    async def _try_proxy(self, proxy: str, ptype: str, judge: str) -> float | None:
        try:
            connector = get_connector(proxy, ptype)
            timeout = aiohttp.ClientTimeout(
                total=self.config.TIMEOUT,
                connect=min(self.config.TIMEOUT * 0.6, 4.0),
                sock_connect=min(self.config.TIMEOUT * 0.6, 4.0),
                sock_read=self.config.TIMEOUT,
            )
            start = time.perf_counter()
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0"},
                connector_owner=True,
            ) as session:
                async with session.get(judge, allow_redirects=False) as resp:
                    if resp.status in (200, 301, 302):
                        await resp.read()
                        return (time.perf_counter() - start) * 1000
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        return None

    async def _try_google(self, proxy: str, ptype: str) -> float | None:
        for target in self.config.GOOGLE_TARGETS:
            try:
                connector = get_connector(proxy, ptype)
                timeout = aiohttp.ClientTimeout(
                    total=self.config.GOOGLE_TIMEOUT,
                    connect=min(self.config.GOOGLE_TIMEOUT * 0.5, 5.0),
                    sock_connect=min(self.config.GOOGLE_TIMEOUT * 0.5, 5.0),
                    sock_read=self.config.GOOGLE_TIMEOUT,
                )
                start = time.perf_counter()
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                    },
                    connector_owner=True,
                ) as session:
                    async with session.get(target, allow_redirects=True) as resp:
                        if resp.status in (200, 204, 301, 302):
                            await resp.read()
                            return (time.perf_counter() - start) * 1000
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
        return None

    async def check(self, proxy: str, types_to_try: list[str]):
        if self._shutdown:
            return

        alive_ms = None
        alive_type = None

        async with self._judge_sem:
            if self._shutdown:
                return
            for ptype in types_to_try:
                judge = self._next_judge()
                ms = await self._try_proxy(proxy, ptype, judge)
                if ms is not None:
                    alive_ms = ms
                    alive_type = ptype
                    break

        google_ms = None
        if alive_ms is not None and self.config.CHECK_GOOGLE:
            async with self._google_sem:
                google_ms = await self._try_google(proxy, alive_type)

        async with self._lock:
            self._checked += 1
            done = self._checked
            total = self._total
            elapsed = time.time() - self._start_time
            speed = done / elapsed if elapsed > 0 else 0
            pct = (done / total * 100) if total else 0

            if alive_ms is None:
                self.dead.append(proxy)
                status = f"{Fore.RED}DEAD"
            elif self.config.CHECK_GOOGLE and google_ms is None:
                self.no_google.append((proxy, alive_type, alive_ms))
                status = f"{Fore.YELLOW}NO-GOOGLE"
            else:
                gms = google_ms or 0.0
                self.alive.append((proxy, alive_type, alive_ms, gms))
                mc = (
                    Fore.GREEN
                    if alive_ms < 500
                    else (Fore.YELLOW if alive_ms < 1000 else Fore.RED)
                )
                g_str = f"  {Fore.CYAN}G:{google_ms:.0f}ms" if google_ms else ""
                status = (
                    f"{Fore.GREEN}OK {alive_type.upper():<6} {mc}{alive_ms:.0f}ms"
                    f"{g_str}"
                )

        bar_width = 30
        filled = int(bar_width * done / total) if total else 0
        bar = f"{Fore.GREEN}{'#' * filled}{Fore.WHITE}{'.' * (bar_width - filled)}"

        async with self._display_lock:
            sys.stdout.write(
                f"\r  [{bar}{Fore.WHITE}] {pct:5.1f}%  "
                f"{Fore.CYAN}{done}/{total}  "
                f"{Fore.YELLOW}{speed:.0f}/s  "
                f"{Fore.WHITE}{proxy:<22}  {status}          "
            )
            sys.stdout.flush()

    async def run(self, proxies: list[str], types_to_try: list[str]):
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.config.PROXIES_DIR, exist_ok=True)
        self._total = len(proxies)
        self._start_time = time.time()

        tasks = [self.check(p, types_to_try) for p in proxies]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self._shutdown = True

        self.alive.sort(key=lambda x: x[2])

        dead_path = os.path.join(self.config.OUTPUT_DIR, self.config.DEAD_FILE)
        async with aiofiles.open(dead_path, "w") as fd:
            for proxy in self.dead:
                await fd.write(f"{proxy}\n")

        if self.no_google:
            ng_path = os.path.join(self.config.OUTPUT_DIR, self.config.NO_GOOGLE_FILE)
            async with aiofiles.open(ng_path, "w") as fng:
                await fng.write("# Alive but cannot reach Google\n\n")
                for proxy, ptype, ms in self.no_google:
                    await fng.write(f"{proxy} | {ptype} | {ms:.0f}ms\n")

        print()
        return self.alive, self.dead, self.no_google


def get_next_filename(directory: str, base_filename: str) -> str:
    import re

    base, ext = os.path.splitext(base_filename)
    base = re.sub(r"_\d+$", "", base)

    counter = 1
    while True:
        candidate = os.path.join(directory, f"{base}_{counter}{ext}")
        if not os.path.exists(candidate):
            return candidate
        counter += 1
