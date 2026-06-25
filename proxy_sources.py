"""
多源免费代理获取模块
整合多个可靠的免费代理源，支持异步并发获取和去重
"""
import asyncio
import logging
import re
import time
from typing import List, Dict, Optional
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger("proxy_sources")


@dataclass
class RawProxy:
    """原始代理信息"""
    url: str              # 完整代理URL: http://ip:port
    source: str           # 来源名称
    protocol: str = "http"  # http/https/socks
    is_https: bool = False  # 是否原生HTTPS
    speed_ms: float = 0.0   # 响应时间(ms)
    score: float = 0.0      # 质量评分(0-100)


@dataclass
class SourceResult:
    """单个源抓取结果"""
    name: str
    count: int = 0
    elapsed_ms: float = 0.0
    error: Optional[str] = None


class MultiSourceFetcher:
    """多源代理抓取器 - 并发从多个源获取代理并去重"""

    # 按可靠性和速度排序的代理源
    SOURCES = [
        "freeproxy_lib",      # FreeProxy库 (最可靠，300个)
        "free_proxy_list",    # free-proxy-list.net 直接抓取
        "us_proxy_org",       # us-proxy.org
        "proxydb_net",        # proxydb.net
        "sslproxies_org",     # sslproxies.org
        "proxy_list_dl",      # proxy-list.download API
    ]

    def __init__(self, timeout: float = 10.0, max_per_source: int = 300):
        self.timeout = timeout
        self.max_per_source = max_per_source
        self._session: Optional[aiohttp.ClientSession] = None
        self._results: List[SourceResult] = []

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_all(self, sources: Optional[List[str]] = None) -> List[RawProxy]:
        """并发从所有源抓取代理并去重

        Args:
            sources: 指定要使用的源列表，None表示使用所有源

        Returns:
            去重后的代理列表
        """
        source_names = sources or self.SOURCES
        self._results = []
        start = time.time()

        # 并发执行所有源
        tasks = []
        for name in source_names:
            func = getattr(self, f"_fetch_{name}", None)
            if func:
                tasks.append(self._run_source(name, func))

        all_proxies = await asyncio.gather(*tasks, return_exceptions=True)

        # 合并去重
        seen_urls = set()
        merged = []
        for proxy_list in all_proxies:
            if isinstance(proxy_list, list):
                for p in proxy_list:
                    if p.url not in seen_urls:
                        seen_urls.add(p.url)
                        merged.append(p)

        elapsed = (time.time() - start) * 1000
        logger.info(
            f"多源抓取完成: {len(merged)} 个去重代理 "
            f"({len(self._results)} 个源, {elapsed:.0f}ms)"
        )
        return merged

    def get_source_stats(self) -> List[SourceResult]:
        """获取各源抓取统计"""
        return self._results

    async def _run_source(self, name: str, func) -> List[RawProxy]:
        """执行单个源并记录结果"""
        start = time.time()
        result = SourceResult(name=name)
        try:
            proxies = await asyncio.wait_for(func(), timeout=self.timeout + 5)
            result.count = len(proxies)
            result.elapsed_ms = (time.time() - start) * 1000
            self._results.append(result)
            return proxies
        except asyncio.TimeoutError:
            result.error = "超时"
            result.elapsed_ms = (time.time() - start) * 1000
            self._results.append(result)
            return []
        except Exception as e:
            result.error = str(e)[:100]
            result.elapsed_ms = (time.time() - start) * 1000
            self._results.append(result)
            return []

    # ==================== 代理源实现 ====================

    async def _fetch_freeproxy_lib(self) -> List[RawProxy]:
        """从 FreeProxy 库获取 (CharlesPikachu/FreeProxy)"""
        proxies = []
        try:
            from freeproxy.proxy import (
                from_free_proxy_list, from_cn_proxy,
                from_hide_my_ip, from_pachong_org,
            )
            import warnings
            warnings.filterwarnings("ignore")

            loop = asyncio.get_event_loop()
            sources = [
                ("free_proxy_list", from_free_proxy_list),
                ("cn_proxy", from_cn_proxy),
                ("hide_my_ip", from_hide_my_ip),
                ("pachong_org", from_pachong_org),
            ]
            for src_name, func in sources:
                try:
                    raw = await asyncio.wait_for(
                        loop.run_in_executor(None, func), timeout=8.0
                    )
                    for p in raw:
                        if isinstance(p, str) and ":" in p:
                            proxies.append(RawProxy(
                                url=f"http://{p.strip()}", source="freeproxy"
                            ))
                except Exception:
                    continue
        except ImportError:
            logger.debug("FreeProxy库未安装")
        return proxies

    async def _fetch_free_proxy_list(self) -> List[RawProxy]:
        """从 free-proxy-list.net 直接抓取"""
        return await self._scrape_table_site(
            "http://free-proxy-list.net/", "free_proxy_list"
        )

    async def _fetch_us_proxy_org(self) -> List[RawProxy]:
        """从 us-proxy.org 直接抓取"""
        return await self._scrape_table_site(
            "http://www.us-proxy.org/", "us_proxy"
        )

    async def _fetch_sslproxies_org(self) -> List[RawProxy]:
        """从 sslproxies.org 抓取(仅HTTPS代理)"""
        return await self._scrape_table_site(
            "http://www.sslproxies.org/", "sslproxies", https_only=True
        )

    async def _scrape_table_site(
        self, url: str, source: str, https_only: bool = False
    ) -> List[RawProxy]:
        """通用表格站点抓取 (free-proxy-list.net系列站点共用)"""
        proxies = []
        try:
            from bs4 import BeautifulSoup
            session = await self._get_session()
            async with session.get(url) as resp:
                if resp.status != 200:
                    return proxies
                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", {"id": "proxylisttable"})
            if not table:
                table = soup.find("table")
            if not table:
                return proxies

            tbody = table.find("tbody")
            if not tbody:
                return proxies

            for row in tbody.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 7:
                    ip = cols[0].text.strip()
                    port = cols[1].text.strip()
                    https_col = cols[6].text.strip().lower()
                    is_https = https_col == "yes"

                    if not ip or not port:
                        continue
                    if https_only and not is_https:
                        continue

                    prefix = "https" if is_https else "http"
                    proxies.append(RawProxy(
                        url=f"{prefix}://{ip}:{port}",
                        source=source,
                        protocol=prefix,
                        is_https=is_https,
                    ))
                    if len(proxies) >= self.max_per_source:
                        break
        except ImportError:
            logger.debug("bs4 未安装，无法抓取表格站点")
        except Exception as e:
            logger.debug(f"抓取 {source} 失败: {e}")
        return proxies

    async def _fetch_proxydb_net(self) -> List[RawProxy]:
        """从 proxydb.net 抓取"""
        proxies = []
        try:
            from bs4 import BeautifulSoup
            session = await self._get_session()
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            }
            async with session.get("http://proxydb.net/", headers=headers) as resp:
                if resp.status != 200:
                    return proxies
                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            for row in soup.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 4:
                    ip_el = cols[0].find("a")
                    if ip_el:
                        ip = ip_el.text.strip()
                        port = cols[1].text.strip()
                        proto_text = cols[3].text.strip().lower()

                        if ip and port and re.match(r'\d+\.\d+\.\d+\.\d+', ip):
                            try:
                                port_num = int(port)
                                if port_num < 1 or port_num > 65535:
                                    continue
                            except ValueError:
                                continue
                            proto = "https" if "https" in proto_text else "http"
                            proxies.append(RawProxy(
                                url=f"{proto}://{ip}:{port}",
                                source="proxydb",
                                protocol=proto,
                                is_https="https" in proto_text,
                            ))
                            if len(proxies) >= self.max_per_source:
                                break
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"抓取 proxydb 失败: {e}")
        return proxies

    async def _fetch_proxy_list_dl(self) -> List[RawProxy]:
        """从 proxy-list.download API 获取"""
        proxies = []
        try:
            session = await self._get_session()
            async with session.get(
                "https://www.proxy-list.download/api/v1/get?type=https"
            ) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    for line in text.strip().split("\n"):
                        line = line.strip()
                        if line and ":" in line:
                            proxies.append(RawProxy(
                                url=f"https://{line}",
                                source="proxy_list_dl",
                                protocol="https",
                                is_https=True,
                            ))
                            if len(proxies) >= self.max_per_source:
                                break
        except Exception as e:
            logger.debug(f"抓取 proxy-list.download 失败: {e}")
        return proxies


class ProxyValidator:
    """代理验证器 - 支持目标站验证和质量评分"""

    def __init__(
        self,
        validate_url: str = "https://httpbin.org/ip",
        timeout: float = 8.0,
        concurrency: int = 30,
        retry_count: int = 1,
        target_url: Optional[str] = None,
    ):
        """
        Args:
            validate_url: 通用验证URL (默认 httpbin.org)
            timeout: 单个验证超时(秒)
            concurrency: 并发验证数
            retry_count: 验证失败重试次数
            target_url: 目标站URL (用于验证代理能否访问目标站)
        """
        self.validate_url = validate_url
        self.timeout = timeout
        self.concurrency = concurrency
        self.retry_count = retry_count
        self.target_url = target_url

    async def validate_batch(self, proxies: List[RawProxy]) -> List[RawProxy]:
        """批量验证代理，返回可用代理（按质量评分排序）"""
        if not proxies:
            return []

        semaphore = asyncio.Semaphore(self.concurrency)
        start = time.time()
        total = len(proxies)

        async def validate_one(proxy: RawProxy):
            async with semaphore:
                for attempt in range(self.retry_count + 1):
                    result = await self._test_proxy(proxy)
                    if result["ok"]:
                        proxy.speed_ms = result["speed_ms"]
                        proxy.is_https = result["is_https"]
                        # 评分: 速度越快分越高
                        if proxy.speed_ms < 500:
                            proxy.score = 90 + (500 - proxy.speed_ms) / 50
                        elif proxy.speed_ms < 2000:
                            proxy.score = 60 + (2000 - proxy.speed_ms) / 50
                        elif proxy.speed_ms < 5000:
                            proxy.score = 30 + (5000 - proxy.speed_ms) / 54
                        else:
                            proxy.score = max(0, 30 - (proxy.speed_ms - 5000) / 500)
                        return True
                    if attempt < self.retry_count:
                        await asyncio.sleep(0.5)  # 重试前短暂等待
                proxy.score = 0
                return False

        tasks = [validate_one(p) for p in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = (time.time() - start) * 1000
        valid = [p for p, r in zip(proxies, results) if r is True]

        # 按评分排序
        valid.sort(key=lambda p: p.score, reverse=True)

        logger.info(
            f"验证完成: {len(valid)}/{total} 可用 "
            f"({elapsed:.0f}ms, 并发={self.concurrency})"
        )
        if valid:
            avg_speed = sum(p.speed_ms for p in valid) / len(valid)
            avg_score = sum(p.score for p in valid) / len(valid)
            logger.info(
                f"可用代理: 平均速度={avg_speed:.0f}ms, 平均评分={avg_score:.1f}"
            )

        return valid

    async def _test_proxy(self, proxy: RawProxy) -> Dict:
        """测试单个代理"""
        result = {"ok": False, "speed_ms": 9999, "is_https": False}

        # 优先用目标站验证（用首页而非API），回退到通用验证
        urls_to_try = []
        if self.target_url:
            # 从API URL提取域名，用首页验证
            from urllib.parse import urlparse
            parsed = urlparse(self.target_url)
            homepage = f"{parsed.scheme}://{parsed.netloc}/"
            urls_to_try.append(homepage)
        urls_to_try.append(self.validate_url)

        for url in urls_to_try:
            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    start = time.time()
                    async with session.get(
                        url,
                        proxy=proxy.url,
                        ssl=False,
                        allow_redirects=False,
                    ) as resp:
                        speed = (time.time() - start) * 1000
                        # 接受 200-499 (连接成功, 包括404等)
                        # 拒绝 5xx (代理/服务端错误) 和连接失败
                        if 200 <= resp.status < 500:
                            result["ok"] = True
                            result["speed_ms"] = speed
                            result["is_https"] = url.startswith("https")
                            return result
                        # 503 可能是目标站限流，也算代理可用
                        if resp.status == 503:
                            result["ok"] = True
                            result["speed_ms"] = speed
                            result["is_https"] = url.startswith("https")
                            return result
            except asyncio.TimeoutError:
                continue
            except aiohttp.ClientProxyConnectionError:
                return result  # 代理连接失败，直接失败
            except aiohttp.ClientConnectorError:
                continue
            except Exception:
                continue

        return result
