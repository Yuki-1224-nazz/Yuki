"""
proxy_scraper.py -- Fetch proxies from all configured online sources.
"""

import asyncio
import aiohttp
from colorama import Fore

from helpers import validate_proxy, extract_ips
from proxy_sources import PROXY_SOURCES


async def fetch_source(session: aiohttp.ClientSession, source: dict) -> list[str]:
    proxies = []
    try:
        async with session.get(
            source["url"], timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            if resp.status != 200:
                return proxies

            fmt = source["fmt"]

            if fmt == "plain":
                text = await resp.text(errors="ignore")
                for candidate in extract_ips(text):
                    if validate_proxy(candidate):
                        proxies.append(candidate)

            elif fmt == "geonode":
                data = await resp.json(content_type=None)
                for item in data.get("data", []):
                    ip = item.get("ip", "")
                    port = item.get("port", "")
                    if ip and port:
                        candidate = f"{ip}:{port}"
                        if validate_proxy(candidate):
                            proxies.append(candidate)

            elif fmt == "redscrape":
                data = await resp.json(content_type=None)
                items = (
                    data
                    if isinstance(data, list)
                    else data.get("proxies", data.get("data", []))
                )
                for item in items:
                    ip = item.get("ip", "")
                    port = item.get("port", "")
                    if ip and port:
                        candidate = f"{ip}:{port}"
                        if validate_proxy(candidate):
                            proxies.append(candidate)

            elif fmt == "scrape":
                text = await resp.text(errors="ignore")
                for candidate in extract_ips(text):
                    if validate_proxy(candidate):
                        proxies.append(candidate)

    except Exception:
        pass
    return proxies


async def scrape_all(selected_sources: list[dict] | None = None) -> list[str]:
    sources = selected_sources or PROXY_SOURCES
    print(f"\n{Fore.CYAN}  Grabbing from {len(sources)} source(s)...\n")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/plain,text/html,application/json,*/*",
    }
    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(
        headers=headers, connector=connector
    ) as session:
        tasks = [fetch_source(session, src) for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    seen, all_proxies = set(), []
    for src, result in zip(sources, results):
        if isinstance(result, list):
            fresh = [p for p in result if p not in seen]
            seen.update(fresh)
            all_proxies.extend(fresh)
            status = f"{Fore.GREEN}+{len(fresh)}" if fresh else f"{Fore.RED}0"
            print(f"  {Fore.WHITE}{src['name']:<30} {status}{Fore.WHITE} proxies")
        else:
            print(f"  {Fore.WHITE}{src['name']:<30} {Fore.RED}FAILED")

    print(f"\n  {Fore.GREEN}[+] Total unique grabbed: {len(all_proxies)}\n")
    return all_proxies
