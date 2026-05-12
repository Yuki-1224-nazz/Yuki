"""
helpers.py -- Shared utilities: validation, extraction, connectors.
"""

import re
from aiohttp_socks import ProxyConnector, ProxyType


def validate_proxy(proxy: str) -> bool:
    pattern = r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):(\d{1,5})$"
    m = re.match(pattern, proxy.strip())
    if not m:
        return False
    octets = [int(m.group(i)) for i in range(1, 5)]
    port = int(m.group(5))
    return all(0 <= o <= 255 for o in octets) and 1 <= port <= 65535


def extract_ips(text: str) -> list[str]:
    return re.findall(r"\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}", text)


def read_proxies_from_file(file_path: str) -> list[str]:
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.readlines()
    except FileNotFoundError:
        print(f"  [!] File not found: {file_path}")
        return []

    proxies, seen, skipped = [], set(), 0
    for line in raw:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^[a-zA-Z0-9+\-]+://", "", line)
        m = re.search(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})", line)
        if m:
            proxy = m.group(1)
            if validate_proxy(proxy) and proxy not in seen:
                proxies.append(proxy)
                seen.add(proxy)
            else:
                skipped += 1
        else:
            skipped += 1

    if skipped:
        print(f"  [!] Skipped {skipped} invalid/duplicate entries.")
    return proxies


def get_connector(proxy: str, ptype: str):
    host, port_str = proxy.split(":")
    port = int(port_str)
    type_map = {
        "http": ProxyType.HTTP,
        "https": ProxyType.HTTP,
        "socks4": ProxyType.SOCKS4,
        "socks5": ProxyType.SOCKS5,
    }
    return ProxyConnector(
        proxy_type=type_map[ptype],
        host=host,
        port=port,
        rdns=True,
        limit=0,
        enable_cleanup_closed=True,
    )
