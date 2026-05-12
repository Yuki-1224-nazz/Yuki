"""
proxy_sources.py -- All proxy source URLs (auto-refreshed APIs & lists).
"""

PROXY_SOURCES = [
    # -- ProxyScrape -- every 5 minutes
    {
        "name": "ProxyScrape HTTP       [~5min]",
        "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
        "fmt": "plain",
    },
    {
        "name": "ProxyScrape SOCKS4     [~5min]",
        "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=10000&country=all",
        "fmt": "plain",
    },
    {
        "name": "ProxyScrape SOCKS5     [~5min]",
        "url": "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all",
        "fmt": "plain",
    },

    # -- Proxifly CDN (jsdelivr mirror) -- every 5 minutes
    {
        "name": "Proxifly ALL           [~5min]",
        "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt",
        "fmt": "plain",
    },
    {
        "name": "Proxifly HTTP          [~5min]",
        "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
        "fmt": "plain",
    },
    {
        "name": "Proxifly SOCKS4        [~5min]",
        "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt",
        "fmt": "plain",
    },
    {
        "name": "Proxifly SOCKS5        [~5min]",
        "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
        "fmt": "plain",
    },

    # -- iplocate free-proxy-list -- every 30 minutes
    {
        "name": "iplocate HTTP          [~30min]",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/proxies/http.txt",
        "fmt": "plain",
    },
    {
        "name": "iplocate SOCKS4        [~30min]",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/proxies/socks4.txt",
        "fmt": "plain",
    },
    {
        "name": "iplocate SOCKS5        [~30min]",
        "url": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/proxies/socks5.txt",
        "fmt": "plain",
    },

    # -- free-proxy-list.net API -- every 30 minutes
    {
        "name": "free-proxy-list.net    [~30min]",
        "url": "https://free-proxy-list.net/",
        "fmt": "scrape",
    },

    # -- litport.net -- every 3 minutes
    {
        "name": "litport HTTP           [~3min]",
        "url": "https://litport.net/free-proxy/http.txt",
        "fmt": "plain",
    },
    {
        "name": "litport SOCKS5         [~3min]",
        "url": "https://litport.net/free-proxy/socks5.txt",
        "fmt": "plain",
    },

    # -- redscrape.com JSON API -- every 10 minutes
    {
        "name": "redscrape API          [~10min]",
        "url": "https://free.redscrape.com/api/proxies",
        "fmt": "redscrape",
    },

    # -- pubproxy.com API -- live
    {
        "name": "pubproxy HTTP          [live]",
        "url": "http://pubproxy.com/api/proxy?limit=20&format=txt&type=http",
        "fmt": "plain",
    },
    {
        "name": "pubproxy SOCKS5        [live]",
        "url": "http://pubproxy.com/api/proxy?limit=20&format=txt&type=socks5",
        "fmt": "plain",
    },

    # -- GeoNode JSON API -- live checked
    {
        "name": "GeoNode p1             [live]",
        "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc",
        "fmt": "geonode",
    },
    {
        "name": "GeoNode p2             [live]",
        "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc",
        "fmt": "geonode",
    },
    {
        "name": "GeoNode p3             [live]",
        "url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=3&sort_by=lastChecked&sort_type=desc",
        "fmt": "geonode",
    },
]
