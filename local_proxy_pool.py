#!/usr/bin/env python3
"""
自建轻量级代理池服务
无需 Redis/Docker，直接运行。自动从多源抓取 + 验证 + 持久化存储。

使用方式:
  # 启动代理池（前台运行，持续抓取刷新）
  python3 local_proxy_pool.py

  # 启动并指定输出文件
  python3 local_proxy_pool.py -o proxies.txt

  # 一次性抓取（抓完就退出）
  python3 local_proxy_pool.py --once

  # 指定抓取间隔
  python3 local_proxy_pool.py --interval 300
"""

import asyncio
import argparse
import json
import os
import sys
import time
import signal
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set
from datetime import datetime
from urllib.parse import urlparse

import aiohttp

# ============================================================
# 配置
# ============================================================

DEFAULT_OUTPUT = "proxies.txt"           # 可用代理输出文件
DEFAULT_DB_FILE = "proxy_pool.json"      # 持久化数据库
DEFAULT_INTERVAL = 600                   # 抓取间隔(秒)
DEFAULT_VALIDATE_URL = "https://www.kuaisou.com/"
DEFAULT_VALIDATE_TIMEOUT = 8
DEFAULT_VALIDATE_CONCURRENCY = 30
DEFAULT_MIN_SCORE = 20                   # 最低可用分数
MAX_POOL_SIZE = 200                      # 池中最大代理数

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("proxy_pool")


# ============================================================
# 数据模型
# ============================================================

@dataclass
class ProxyRecord:
    url: str
    source: str
    protocol: str = "http"
    score: int = 50
    speed_ms: float = 0
    last_check: float = 0
    fail_count: int = 0
    success_count: int = 0
    is_alive: bool = True


# ============================================================
# 代理源抓取
# ============================================================

class ProxyCrawler:
    """多源代理抓取器 - 支持轮询延迟，避免单一源被封"""

    # 源间延迟范围(秒)：每次请求不同源之间随机等待，分散压力
    SOURCE_DELAY_MIN = 0.5
    SOURCE_DELAY_MAX = 2.0

    @staticmethod
    async def crawl_all(session: aiohttp.ClientSession, timeout: int = 15,
                        rotate_delay: bool = True) -> List[ProxyRecord]:
        """轮询抓取所有源，支持源间随机延迟防封"""
        import random

        # 所有抓取任务工厂（延迟绑定）
        source_factories = [
            ("89ip",            lambda: ProxyCrawler._from_89ip(session, timeout)),
            ("ip3366",          lambda: ProxyCrawler._from_ip3366(session, timeout)),
            ("kuaidaili",       lambda: ProxyCrawler._from_kuaidaili(session, timeout)),
            ("xicidaili",       lambda: ProxyCrawler._from_xicidaili(session, timeout)),
            ("66ip",            lambda: ProxyCrawler._from_66ip(session, timeout)),
            ("jiangxianli",     lambda: ProxyCrawler._from_jiangxianli(session, timeout)),
            ("free-proxy-list", lambda: ProxyCrawler._from_free_proxy_list(session, timeout)),
            ("proxydb",         lambda: ProxyCrawler._from_proxydb(session, timeout)),
            ("proxylist-dl",    lambda: ProxyCrawler._from_proxylist_download(session, timeout)),
            ("github-raw",      lambda: ProxyCrawler._from_github_raw(session, timeout)),
            ("sslproxies",      lambda: ProxyCrawler._from_sslproxies(session, timeout)),
        ]

        async def crawl_with_delay(name, factory):
            if rotate_delay:
                await asyncio.sleep(random.uniform(
                    ProxyCrawler.SOURCE_DELAY_MIN,
                    ProxyCrawler.SOURCE_DELAY_MAX,
                ))
            try:
                return await factory()
            except Exception as e:
                log.warning(f"[{name}] 轮询异常: {e}")
                return []

        # 并发但带延迟：每个源随机错开请求时间
        tasks = [crawl_with_delay(name, fn) for name, fn in source_factories]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_proxies: List[ProxyRecord] = []
        seen: Set[str] = set()
        for result in results:
            if isinstance(result, Exception):
                continue
            for p in result:
                if p.url not in seen:
                    seen.add(p.url)
                    all_proxies.append(p)

        return all_proxies

    @staticmethod
    async def _from_89ip(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """89ip.cn - 国内免费代理（最佳源）"""
        proxies = []
        try:
            import re
            url = "http://www.89ip.cn/tqdl.html?num=100&address=&kill_address=&port=&kill_port=&isp="
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                text = await resp.text()
            for match in re.finditer(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})', text):
                ip, port = match.group(1), int(match.group(2))
                if 1 <= port <= 65535:
                    proxies.append(ProxyRecord(url=f"http://{ip}:{port}", source="89ip", protocol="http"))
            log.info(f"[89ip] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[89ip] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_ip3366(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """ip3366.net - 云代理"""
        proxies = []
        try:
            import re
            for page in range(1, 4):
                url = f"http://www.ip3366.net/free/?stype=1&page={page}"
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                        raw = await resp.read()
                    try:
                        text = raw.decode("gbk")
                    except Exception:
                        text = raw.decode("utf-8", errors="ignore")
                    # Parse table rows: IP | Port
                    rows = re.findall(r'<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>', text)
                    for ip, port in rows:
                        port_num = int(port)
                        if 1 <= port_num <= 65535:
                            proxies.append(ProxyRecord(url=f"http://{ip}:{port}", source="ip3366", protocol="http"))
                except Exception:
                    pass
            log.info(f"[ip3366] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[ip3366] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_kuaidaili(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """kuaidaili.com - 快代理（SPA页面多策略解析）"""
        proxies = []
        try:
            import re
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.kuaidaili.com/",
            }
            for page in [1, 2, 3]:
                url = f"https://www.kuaidaili.com/free/inha/{page}/"
                try:
                    async with session.get(url, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                        if resp.status != 200:
                            continue
                        text = await resp.text()
                    # 策略1: data-title属性（老版SSR）
                    rows = re.findall(
                        r'data-title="IP">(\d+\.\d+\.\d+\.\d+)</td>.*?data-title="PORT">(\d+)</td>',
                        text, re.DOTALL
                    )
                    # 策略2: JSON数据嵌入SSR
                    if not rows:
                        rows = re.findall(r'"ip"\s*:\s*"(\d+\.\d+\.\d+\.\d+)".*?"port"\s*:\s*(\d+)', text)
                    # 策略3: 通用IP:Port匹配
                    if not rows:
                        rows = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[:\s]+(\d{2,5})', text)
                    for ip, port in rows:
                        port_num = int(port)
                        if 1 <= port_num <= 65535:
                            proxies.append(ProxyRecord(
                                url=f"http://{ip}:{port}", source="kuaidaili", protocol="http"
                            ))
                except Exception:
                    pass
            log.info(f"[kuaidaili] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[kuaidaili] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_xicidaili(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """xicidaili.com - 西刺代理（实时更新，含反爬应对策略）"""
        proxies = []
        try:
            import re
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Referer": "https://www.xicidaili.com/",
            }
            # 多个页面类型
            for page_type in ["wn", "wt", "nn", "nt"]:
                try:
                    url = f"https://www.xicidaili.com/{page_type}/"
                    async with session.get(url, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=timeout),
                                          allow_redirects=True) as resp:
                        if resp.status != 200:
                            continue
                        html = await resp.text()
                        # 跳过JS challenge页面
                        if "Redirecting" in html[:100] or "use strict" in html[:200]:
                            continue
                    rows = re.findall(
                        r'<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>', html
                    )
                    if not rows:
                        rows = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[:\s]+(\d{2,5})', html)
                    for ip, port in rows:
                        port_num = int(port)
                        if 1 <= port_num <= 65535:
                            proxies.append(ProxyRecord(
                                url=f"http://{ip}:{port}", source="xicidaili", protocol="http"
                            ))
                except Exception:
                    pass
            log.info(f"[xicidaili] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[xicidaili] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_66ip(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """66ip.cn - 66免费代理（多URL容错策略）"""
        proxies = []
        try:
            import re
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
            # 多个备选URL（主站可能变化）
            urls = [
                ("http://www.66ip.cn/nmtq.php?getnum=80&isp=0&anonymoustype=3"
                 "&start=&ports=&portsb=&protocol=0&address=&kill_address=&area=0&kill_port="),
                "http://www.66ip.cn/mo.php?sxb=&tqsl=50&port=&export=&ktip=&sxa=&textarea=",
                "http://www.66ip.cn/pt.html",
                "http://www.66ip.cn/",
            ]
            for url in urls:
                try:
                    async with session.get(url, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=timeout),
                                          allow_redirects=True) as resp:
                        if resp.status != 200:
                            continue
                        text = await resp.text()
                    found = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})', text)
                    if found:
                        for ip, port in found:
                            port_num = int(port)
                            if 1 <= port_num <= 65535:
                                proxies.append(ProxyRecord(
                                    url=f"http://{ip}:{port}", source="66ip", protocol="http"
                                ))
                        break  # 成功获取后不再尝试其他URL
                except Exception:
                    pass
            log.info(f"[66ip] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[66ip] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_jiangxianli(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """ip.jiangxianli.com - IP精灵（开源代理池，JSON API）"""
        proxies = []
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }
            # JSON API 获取代理列表
            url = "https://ip.jiangxianli.com/api/proxy_ips"
            async with session.get(url, headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    items = data.get("data", [])
                    for item in items:
                        ip = item.get("ip", "")
                        port = item.get("port", "")
                        if ip and port:
                            try:
                                port_num = int(port)
                            except ValueError:
                                continue
                            if 1 <= port_num <= 65535:
                                proto = item.get("protocol", "http").lower()
                                proxies.append(ProxyRecord(
                                    url=f"http://{ip}:{port_num}",
                                    source="jiangxianli",
                                    protocol=proto,
                                    score=55,
                                ))
            log.info(f"[jiangxianli] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[jiangxianli] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_free_proxy_list(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """free-proxy-list.net 页面抓取（含备用解析策略）"""
        proxies = []
        try:
            import re
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            # 尝试主域名 + 备用域名
            urls = ["https://free-proxy-list.net/", "http://free-proxy-list.net/"]
            html = ""
            for fetch_url in urls:
                try:
                    async with session.get(fetch_url, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            break
                except Exception:
                    continue

            if html:
                # 策略1: 表格行匹配
                matches = re.findall(r'<tr><td>(\d+\.\d+\.\d+\.\d+)</td><td>(\d+)</td>', html)
                if not matches:
                    # 策略2: 通用IP:Port匹配
                    matches = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\s:]+(\d{2,5})', html)
                for ip, port in matches:
                    port_num = int(port)
                    if 1 <= port_num <= 65535:
                        proxies.append(ProxyRecord(
                            url=f"http://{ip}:{port}", source="free-proxy-list", protocol="http"
                        ))
            log.info(f"[free-proxy-list] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[free-proxy-list] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_proxydb(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """proxydb.net API"""
        proxies = []
        try:
            url = "https://proxydb.net/?protocol=http& anonymity_levels%5B%5D=2&anonymity_levels%5B%5D=3&anonymity_levels%5B%5D=4"
            headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                html = await resp.text()

            import re
            blocks = re.findall(
                r'<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>.*?href="/(\d+)"',
                html, re.DOTALL
            )
            for ip, port in blocks:
                port_num = int(port)
                if 1 <= port_num <= 65535:
                    proxies.append(ProxyRecord(
                        url=f"http://{ip}:{port}", source="proxydb", protocol="http"
                    ))
            log.info(f"[proxydb] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[proxydb] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_proxylist_download(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """proxy-list.download HTTP列表"""
        proxies = []
        try:
            url = "https://www.proxy-list.download/api/v1/get?type=http"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                text = await resp.text()
            import re
            for line in text.strip().split("\n"):
                line = line.strip()
                match = re.match(r"(\d+\.\d+\.\d+\.\d+):(\d+)", line)
                if match:
                    port_num = int(match.group(2))
                    if 1 <= port_num <= 65535:
                        proxies.append(ProxyRecord(
                            url=f"http://{line}", source="proxylist-download", protocol="http"
                        ))
            log.info(f"[proxylist-download] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[proxylist-download] 失败: {e}")
        return proxies

    @staticmethod
    async def _from_github_raw(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """GitHub Raw 代理列表 - 多仓库并发，最稳定源"""
        proxies = []
        urls = [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
            "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
            "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
            "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
            "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
        ]
        import re
        for source_url in urls:
            try:
                repo_name = source_url.split("githubusercontent.com/")[1].split("/")[0]
                async with session.get(source_url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    if resp.status != 200:
                        continue
                    text = await resp.text()
                count = 0
                for line in text.strip().split("\n")[:300]:
                    line = line.strip()
                    match = re.match(r"(\d+\.\d+\.\d+\.\d+):(\d+)", line)
                    if match:
                        port_num = int(match.group(2))
                        if 1 <= port_num <= 65535:
                            proxies.append(ProxyRecord(
                                url=f"http://{line}", source="github-raw", protocol="http"
                            ))
                            count += 1
                log.info(f"[github-raw] {repo_name}: 获取 {count} 个代理")
            except Exception as e:
                log.warning(f"[github-raw] {source_url.split('/')[-1]}: {e}")
        return proxies

    @staticmethod
    async def _from_sslproxies(session: aiohttp.ClientSession, timeout: int) -> List[ProxyRecord]:
        """sslproxies.org HTTPS 代理（含备用域名）"""
        proxies = []
        try:
            import re
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            # 尝试多个域名
            urls = [
                "https://www.sslproxies.org/",
                "http://www.sslproxies.org/",
                "https://sslproxies.org/",
            ]
            html = ""
            for fetch_url in urls:
                try:
                    async with session.get(fetch_url, headers=headers,
                                          timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            break
                except Exception:
                    continue

            if html:
                pattern = r'<td>(\d+\.\d+\.\d+\.\d+)</td>\s*<td>(\d+)</td>'
                matches = re.findall(pattern, html)
                if not matches:
                    matches = re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\s:]+(\d{2,5})', html)
                for ip, port in matches:
                    port_num = int(port)
                    if 1 <= port_num <= 65535:
                        proxies.append(ProxyRecord(
                            url=f"http://{ip}:{port}", source="sslproxies", protocol="https", score=60
                        ))
            log.info(f"[sslproxies] 获取 {len(proxies)} 个代理")
        except Exception as e:
            log.warning(f"[sslproxies] 失败: {e}")
        return proxies


# ============================================================
# 代理验证
# ============================================================

class ProxyChecker:
    """异步代理验证器"""

    def __init__(
        self,
        validate_url: str = DEFAULT_VALIDATE_URL,
        timeout: int = DEFAULT_VALIDATE_TIMEOUT,
        concurrency: int = DEFAULT_VALIDATE_CONCURRENCY,
    ):
        self.validate_url = validate_url
        self.timeout = timeout
        self.concurrency = concurrency
        self._semaphore = None

    def _get_semaphore(self):
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.concurrency)
        return self._semaphore

    async def validate_batch(self, proxies: List[ProxyRecord], session: Optional[aiohttp.ClientSession] = None) -> List[ProxyRecord]:
        """批量验证，可传入session或自动创建"""
        if session is not None:
            return await self._do_validate(session, proxies)
        connector = aiohttp.TCPConnector(limit=0, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as own_session:
            return await self._do_validate(own_session, proxies)

    async def _do_validate(self, session: aiohttp.ClientSession, proxies: List[ProxyRecord]) -> List[ProxyRecord]:
        tasks = [self._validate_one(session, p) for p in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        alive = []
        exc_count = 0
        for r in results:
            if isinstance(r, BaseException):
                exc_count += 1
            elif isinstance(r, ProxyRecord) and r.is_alive:
                alive.append(r)
        if exc_count:
            log.warning(f"验证中 {exc_count} 个任务异常")
        return alive

    async def _validate_one(self, session: aiohttp.ClientSession, proxy: ProxyRecord) -> ProxyRecord:
        async with self._get_semaphore():
            proxy.last_check = time.time()
            start = time.time()
            try:
                async with session.get(
                    self.validate_url,
                    proxy=proxy.url,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                    allow_redirects=True,
                ) as resp:
                    elapsed = (time.time() - start) * 1000
                    proxy.speed_ms = round(elapsed, 1)
                    if 200 <= resp.status < 500:
                        proxy.is_alive = True
                        proxy.success_count += 1
                        # 计算分数
                        speed_score = max(0, 100 - proxy.speed_ms / 100)
                        proxy.score = int(speed_score * 0.7 + 30 * 0.3)
                    else:
                        proxy.is_alive = False
                        proxy.fail_count += 1
            except BaseException as e:
                proxy.is_alive = False
                proxy.fail_count += 1
                proxy.score = max(0, proxy.score - 20)
            return proxy


# ============================================================
# 代理池存储
# ============================================================

class ProxyStorage:
    """本地文件持久化存储"""

    def __init__(self, db_file: str, output_file: str):
        self.db_file = db_file
        self.output_file = output_file
        self.pool: Dict[str, ProxyRecord] = {}

    def load(self):
        """从 JSON 文件加载"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, "r") as f:
                    data = json.load(f)
                for item in data:
                    rec = ProxyRecord(**item)
                    self.pool[rec.url] = rec
                log.info(f"从 {self.db_file} 加载 {len(self.pool)} 条记录")
            except Exception as e:
                log.warning(f"加载数据库失败: {e}")

    def save(self):
        """保存到 JSON 文件"""
        data = [asdict(r) for r in self.pool.values()]
        with open(self.db_file, "w") as f:
            json.dump(data, f, indent=2)

    def export_alive(self):
        """导出可用代理到 txt 文件"""
        alive = [r for r in self.pool.values() if r.is_alive and r.score >= DEFAULT_MIN_SCORE]
        alive.sort(key=lambda x: -x.score)

        # 限制数量
        if len(alive) > MAX_POOL_SIZE:
            alive = alive[:MAX_POOL_SIZE]

        with open(self.output_file, "w") as f:
            for r in alive:
                f.write(r.url + "\n")

        log.info(f"导出 {len(alive)} 个可用代理到 {self.output_file}")
        return len(alive)

    def merge(self, new_proxies: List[ProxyRecord]):
        """合并新抓取的代理（已有则保留历史数据）"""
        added = 0
        for p in new_proxies:
            if p.url not in self.pool:
                self.pool[p.url] = p
                added += 1
            # 已有的保留历史分数，不覆盖
        if added:
            log.info(f"新增 {added} 个代理到池中（总计 {len(self.pool)}）")

    def cleanup(self):
        """清理多次失败的代理"""
        before = len(self.pool)
        to_remove = [
            url for url, r in self.pool.items()
            if r.fail_count >= 3 and r.success_count == 0
        ]
        for url in to_remove:
            del self.pool[url]
        removed = before - len(self.pool)
        if removed:
            log.info(f"清理 {removed} 个长期不可用代理")

    def stats(self) -> dict:
        total = len(self.pool)
        alive = sum(1 for r in self.pool.values() if r.is_alive)
        sources = {}
        for r in self.pool.values():
            sources[r.source] = sources.get(r.source, 0) + 1
        avg_speed = 0
        alive_records = [r for r in self.pool.values() if r.is_alive and r.speed_ms > 0]
        if alive_records:
            avg_speed = sum(r.speed_ms for r in alive_records) / len(alive_records)
        return {
            "total": total,
            "alive": alive,
            "dead": total - alive,
            "avg_speed_ms": round(avg_speed, 1),
            "sources": sources,
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


# ============================================================
# 主循环
# ============================================================

class LocalProxyPool:
    """本地代理池服务"""

    def __init__(self, args):
        self.args = args
        self.storage = ProxyStorage(args.db_file, args.output)
        self.crawler = ProxyCrawler()
        self.checker = ProxyChecker(
            validate_url=args.validate_url,
            timeout=args.validate_timeout,
            concurrency=args.concurrency,
        )
        self.running = True

    async def run_once(self):
        """执行一轮：抓取 -> 验证 -> 存储"""
        log.info("=" * 60)
        log.info("开始新一轮抓取...")
        round_start = time.time()

        # 1. 抓取 + 验证（同一session避免event loop问题）
        connector = aiohttp.TCPConnector(limit=0, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            new_proxies = await self.crawler.crawl_all(session, timeout=15)
            log.info(f"共抓取 {len(new_proxies)} 个代理（去重后）")

            if not new_proxies:
                log.warning("本轮未抓取到任何代理")
                return

            # 2. 合并到池中
            self.storage.merge(new_proxies)

            # 3. 验证全部未验证或过期的（同一session）
            to_validate = [
                r for r in self.storage.pool.values()
                if not r.is_alive or r.last_check == 0 or (time.time() - r.last_check) > 300
            ]
            if to_validate:
                log.info(f"验证 {len(to_validate)} 个代理...")
                validated = await self.checker.validate_batch(to_validate, session=session)
                log.info(f"验证通过: {len(validated)}/{len(to_validate)}")
                alive_c = sum(1 for r in to_validate if r.is_alive)
                dead_c = len(to_validate) - alive_c
                log.info(f"池状态: {alive_c} 存活, {dead_c} 失败")

        # 4. 清理 + 保存 + 导出
        self.storage.cleanup()
        self.storage.save()
        count = self.storage.export_alive()

        # 5. 统计
        stats = self.storage.stats()
        elapsed = time.time() - round_start
        log.info(f"本轮完成 ({elapsed:.1f}s): 总计 {stats['total']} | "
                 f"可用 {stats['alive']} | 平均速度 {stats['avg_speed_ms']}ms")
        if stats['sources']:
            source_info = ", ".join(f"{k}:{v}" for k, v in stats['sources'].items())
            log.info(f"来源分布: {source_info}")
        log.info("=" * 60)

    async def run_loop(self):
        """持续运行"""
        self.storage.load()

        while self.running:
            try:
                await self.run_once()
            except Exception as e:
                log.error(f"本轮出错: {e}")

            if self.args.once:
                break

            log.info(f"等待 {self.args.interval} 秒后进行下一轮...")
            for _ in range(self.args.interval):
                if not self.running:
                    break
                await asyncio.sleep(1)

    def stop(self):
        self.running = False
        log.info("\n正在停止...")


def main():
    parser = argparse.ArgumentParser(
        description="本地轻量级代理池 - 自动抓取 + 验证 + 持久化",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT,
                        help=f"可用代理输出文件 (默认: {DEFAULT_OUTPUT})")
    parser.add_argument("--db-file", default=DEFAULT_DB_FILE,
                        help=f"持久化数据库文件 (默认: {DEFAULT_DB_FILE})")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"抓取间隔秒数 (默认: {DEFAULT_INTERVAL})")
    parser.add_argument("--once", action="store_true",
                        help="只执行一轮后退出")
    parser.add_argument("--validate-url", default=DEFAULT_VALIDATE_URL,
                        help=f"验证URL (默认: {DEFAULT_VALIDATE_URL})")
    parser.add_argument("--validate-timeout", type=int, default=DEFAULT_VALIDATE_TIMEOUT,
                        help=f"验证超时秒数 (默认: {DEFAULT_VALIDATE_TIMEOUT})")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_VALIDATE_CONCURRENCY,
                        help=f"验证并发数 (默认: {DEFAULT_VALIDATE_CONCURRENCY})")

    args = parser.parse_args()

    pool = LocalProxyPool(args)

    def handle_signal(sig, frame):
        pool.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("╔══════════════════════════════════════════╗")
    log.info("║      本地轻量级代理池 v1.0               ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info(f"║  输出文件: {args.output:<28s}║")
    log.info(f"║  数据库:   {args.db_file:<28s}║")
    log.info(f"║  抓取间隔: {args.interval}s{' ' * 24}║")
    log.info(f"║  验证目标: kuaisou.com{' ' * 16}║")
    log.info("╚══════════════════════════════════════════╝")

    asyncio.run(pool.run_loop())


if __name__ == "__main__":
    main()
