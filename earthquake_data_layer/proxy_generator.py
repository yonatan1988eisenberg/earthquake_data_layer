import random
import threading
from re import findall, sub

import requests

from earthquake_data_layer import settings

SSL = "https://www.sslproxies.org/"
GOOGLE = "https://www.google-proxy.net/"
ANANY = "https://free-proxy-list.net/anonymous-proxy.html"
UK = "https://free-proxy-list.net/uk-proxy.html"
US = "https://www.us-proxy.org/"
NEW = "https://free-proxy-list.net/"
SPYS_ME = "http://spys.me/proxy.txt"
PROXYSCRAPE = "https://api.proxyscrape.com/?request=getproxies&proxytype=all&country=all&ssl=all&anonymity=all"
PROXYNOVA = "https://www.proxynova.com/proxy-server-list/"
PROXYLIST_DOWNLOAD_HTTP = "https://www.proxy-list.download/HTTP"
PROXYLIST_DOWNLOAD_HTTPS = "https://www.proxy-list.download/HTTPS"
PROXYLIST_DOWNLOAD_SOCKS4 = "https://www.proxy-list.download/SOCKS4"
PROXYLIST_DOWNLOAD_SOCKS5 = "https://www.proxy-list.download/SOCKS5"


class Proxy:
    """
    Proxy is the class for proxy.
    """

    def __init__(self, ip, port, category, schema):
        """
        Initialization of the proxy class
        :param ip: ip address of proxy
        :param port: port of proxy
        :param category: category (origen) of proxy
        :param schema: schema of proxy (http or https)
        """
        self.ip = ip
        self.port = port
        self.category = category
        self.schema = schema

    @property
    def proxy(self):
        return {self.schema: f"http://{self.ip}:{self.port}"}


class ProxiesGenerator:
    def __init__(self, schema: str = "http", refresh_timeout=5, test_timeout=0.5):
        self.Categories = {
            "SSL": SSL,
            "GOOGLE": GOOGLE,
            "ANANY": ANANY,
            "UK": UK,
            "US": US,
            "NEW": NEW,
            "SPYS.ME": SPYS_ME,
            "PROXYSCRAPE": PROXYSCRAPE,
            "PROXYNOVA": PROXYNOVA,
            "PROXYLIST_DOWNLOAD_HTTP": PROXYLIST_DOWNLOAD_HTTP,
            "PROXYLIST_DOWNLOAD_HTTPS": PROXYLIST_DOWNLOAD_HTTPS,
            "PROXYLIST_DOWNLOAD_SOCKS4": PROXYLIST_DOWNLOAD_SOCKS4,
            "PROXYLIST_DOWNLOAD_SOCKS5": PROXYLIST_DOWNLOAD_SOCKS5,
        }
        self.proxies = list()
        self.schema = schema
        self.lock = threading.Lock()
        self.refresh_timeout = refresh_timeout
        self.test_timeout = test_timeout

    def refresh_proxies(self) -> bool:
        """
        scrapes proxies by regex.
        :return: bool, if any proxy was scraped
        """
        proxies = list()

        settings.logger.debug("refreshing proxies")

        for category_name, category_url in self.Categories.items():
            category_proxies = list()
            try:
                r = requests.get(url=category_url, timeout=self.refresh_timeout)
                if category_name in {"SPYS.ME", "proxyscrape"}:
                    category_proxies = findall(r"\d+\.\d+\.\d+\.\d+:\d+", r.text)
                elif category_name == "PROXYNOVA":
                    matches = findall(
                        r"\d+\.\d+\.\d+\.\d+\'\)\;</script>\s*</abbr>\s*</td>\s*<td\salign=\"left\">\s*\d+",
                        r.text,
                    )
                    category_proxies = [
                        sub(
                            r"\'\)\;</script>\s*</abbr>\s*</td>\s*<td\salign=\"left\">\s*",
                            ":",
                            m,
                        )
                        for m in matches
                    ]
                elif category_name in {
                    "PROXYLIST_DOWNLOAD_HTTP",
                    "PROXYLIST_DOWNLOAD_HTTPS",
                    "PROXYLIST_DOWNLOAD_SOCKS4",
                    "PROXYLIST_DOWNLOAD_SOCKS5",
                }:
                    matches = findall(r"\d+\.\d+\.\d+\.\d+</td>\s*<td>\d+", r.text)
                    category_proxies = [sub(r"</td>\s*<td>", ":", m) for m in matches]
                else:
                    matches = findall(r"\d+\.\d+\.\d+\.\d+</td><td>\d+", r.text)
                    category_proxies = [m.replace("</td><td>", ":") for m in matches]

                category_proxies = [
                    Proxy(
                        proxy.split(":")[0],
                        proxy.split(":")[1],
                        category_name,
                        self.schema,
                    )
                    for proxy in category_proxies
                ]
                proxies.extend(category_proxies)
            except ConnectionError:
                settings.logger.error(
                    f"Connection Error in refreshing {category_name} proxies"
                )
        self.proxies = proxies

        settings.logger.debug(f"fetched {len(self.proxies)} potential proxies")
        return len(self.proxies) > 0

    def gen(self):
        """
        returns a proxy
        """
        while True:
            with self.lock:
                # refresh the list of proxies if its empty
                if len(self.proxies) == 0:
                    self.refresh_proxies()

                # get a working random proxy and remove it from the list of proxies
                proxy = self.proxies.pop(random.randint(0, len(self.proxies) - 1))

            # return the proxy if its working, else test another
            try:
                if self.is_proxy_working(proxy):
                    return proxy.proxy
            except requests.RequestException:
                continue

    def is_proxy_working(self, proxy: Proxy):
        url = settings.IP_VERIFYING_URL
        with requests.get(
            url, proxies=proxy.proxy, timeout=self.test_timeout, stream=True
        ) as r:
            if (
                r.raw.connection.sock
                and r.raw.connection.sock.getpeername()[0] == proxy.ip
            ):
                return True
        return False
