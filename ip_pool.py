"""
IP池管理模块
支持从文件/API/proxy_pool加载代理、验证可用性、轮换策略
集成 jhao104/proxy_pool 项目: https://github.com/jhao104/proxy_pool
"""
import asyncio
import random
import time
import aiohttp
import logging
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from config import StressTestConfig
from proxy_sources import MultiSourceFetcher, ProxyValidator, RawProxy

logger = logging.getLogger("ip_pool")


@dataclass
class ProxyInfo:
    """代理IP信息"""
    url: str                    # 代理URL, 如 http://ip:port 或 socks5://ip:port
    protocol: str = "http"      # 协议类型
    fail_count: int = 0         # 失败次数
    success_count: int = 0      # 成功次数
    avg_response_time: float = 0.0  # 平均响应时间(ms)
    is_alive: bool = True       # 是否存活
    last_check: float = 0.0     # 上次检查时间戳

    @property
    def total_requests(self) -> int:
        return self.fail_count + self.success_count


class IPPoolManager:
    """IP池管理器 - 支持文件/API/proxy_pool多种代理来源"""

    def __init__(self, config: StressTestConfig):
        self.config = config
        self._proxies: List[ProxyInfo] = []
        self._current_index: int = 0
        self._lock = asyncio.Lock()
        self._use_proxy_pool = bool(config.proxy_pool_url)  # 是否使用proxy_pool
        self._last_refresh: float = 0.0  # 上次从proxy_pool刷新的时间
        self._http_session: Optional[aiohttp.ClientSession] = None

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """获取共享的HTTP Session"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._http_session

    async def close(self):
        """关闭HTTP Session"""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def initialize(self, skip_validate: bool = False):
        """初始化IP池"""
        logger.info("正在初始化IP池...")

        # 优先级: proxy_pool > FreeProxy > proxy_api > 文件

        # 1. 从 proxy_pool 加载 (jhao104/proxy_pool 项目)
        if self._use_proxy_pool:
            pool_proxies = await self._load_from_proxy_pool()
            if pool_proxies:
                logger.info(f"从 proxy_pool 加载了 {len(pool_proxies)} 个代理")
                self._proxies.extend(pool_proxies)

        # 2. 从多源抓取代理 (proxy_sources.py)
        if self.config.proxy_multi_source:
            ms_proxies = await self._load_from_multi_source()
            if ms_proxies:
                logger.info(f"从多源抓取了 {len(ms_proxies)} 个代理")
                self._proxies.extend(ms_proxies)

        # 3. 从 FreeProxy 加载 (CharlesPikachu/FreeProxy, 无需Docker)
        if self.config.use_freeproxy and not self.config.proxy_multi_source:
            fp_proxies = await self._load_from_freeproxy()
            if fp_proxies:
                logger.info(f"从 FreeProxy 加载了 {len(fp_proxies)} 个代理")
                self._proxies.extend(fp_proxies)

        # 3. 从文件加载
        file_proxies = self._load_from_file()
        if file_proxies:
            logger.info(f"从文件加载了 {len(file_proxies)} 个代理")
            self._proxies.extend(file_proxies)

        # 4. 从API加载
        if self.config.proxy_api_url:
            api_proxies = await self._load_from_api()
            if api_proxies:
                logger.info(f"从API加载了 {len(api_proxies)} 个代理")
                self._proxies.extend(api_proxies)

        # 验证代理可用性
        if skip_validate and self._proxies:
            for p in self._proxies:
                p.is_alive = True
            logger.info(f"跳过验证，直接使用 {len(self._proxies)} 个代理")
        elif self._proxies and not self._use_proxy_pool:
            await self._validate_all()
        elif self._use_proxy_pool and self._proxies:
            for p in self._proxies:
                p.is_alive = True
            logger.info("proxy_pool模式: 跳过本地验证")
        else:
            logger.warning("未加载到任何代理IP，将使用本机IP直接请求")

        alive_count = sum(1 for p in self._proxies if p.is_alive)
        logger.info(f"IP池初始化完成，可用代理: {alive_count}/{len(self._proxies)}")
        self._last_refresh = time.time()

    # ==================== proxy_pool 集成 ====================

    async def _load_from_proxy_pool(self) -> List[ProxyInfo]:
        """从 jhao104/proxy_pool 项目加载所有代理

        proxy_pool API 文档:
          GET /all       - 获取所有代理 (?type=https 过滤https)
          GET /get       - 随机获取一个代理
          GET /pop       - 获取并删除一个代理
          GET /count     - 查看代理数量
          GET /delete    - 删除代理 ?proxy=host:ip
        """
        proxies = []
        base_url = self.config.proxy_pool_url.rstrip("/")

        try:
            session = await self._get_http_session()

            # 先查询代理数量
            async with session.get(f"{base_url}/count") as resp:
                if resp.status == 200:
                    count_data = await resp.text()
                    logger.info(f"proxy_pool 中当前有 {count_data} 个代理")
                else:
                    logger.warning(f"proxy_pool /count 请求失败: HTTP {resp.status}")

            # 获取全部代理
            async with session.get(f"{base_url}/all") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for item in data:
                            # proxy_pool 返回格式: [{"proxy": "host:port"}, ...]
                            # 或直接返回 ["host:port", ...]
                            if isinstance(item, dict):
                                proxy_str = item.get("proxy", "")
                            elif isinstance(item, str):
                                proxy_str = item
                            else:
                                continue

                            if not proxy_str:
                                continue

                            # proxy_pool 返回的格式为 host:port
                            proxy_url = f"http://{proxy_str}"
                            proxies.append(ProxyInfo(
                                url=proxy_url,
                                protocol="http",
                                is_alive=True,  # proxy_pool已验证
                            ))
                else:
                    logger.error(f"proxy_pool /all 请求失败: HTTP {resp.status}")

        except aiohttp.ClientConnectorError as e:
            logger.error(f"无法连接 proxy_pool ({base_url}): {e}")
            logger.error("请确保 proxy_pool 服务已启动，参考 docker-compose.yml")
        except Exception as e:
            logger.error(f"从 proxy_pool 加载代理失败: {e}")

        return proxies

    async def _get_proxy_from_pool(self) -> Optional[str]:
        """直接从 proxy_pool API 获取一个代理（实时获取，不经过本地缓存）"""
        if not self._use_proxy_pool:
            return None

        base_url = self.config.proxy_pool_url.rstrip("/")
        mode = self.config.proxy_pool_mode or "get"
        endpoint = "/pop" if mode == "pop" else "/get"

        try:
            session = await self._get_http_session()
            async with session.get(f"{base_url}{endpoint}") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # 返回格式: {"proxy": "host:port", ...}
                    if isinstance(data, dict):
                        proxy_str = data.get("proxy", "")
                        if proxy_str:
                            return f"http://{proxy_str}"
                    elif isinstance(data, str) and data:
                        return f"http://{data}"
        except Exception as e:
            logger.debug(f"从 proxy_pool 实时获取代理失败: {e}")

        return None

    async def _delete_proxy_from_pool(self, proxy_url: str):
        """从 proxy_pool 中删除一个失效代理"""
        if not self._use_proxy_pool:
            return

        base_url = self.config.proxy_pool_url.rstrip("/")
        # 从 http://host:port 提取 host:port
        proxy_str = proxy_url.replace("http://", "").replace("https://", "")
        # 去掉可能的认证信息
        if "@" in proxy_str:
            proxy_str = proxy_str.split("@", 1)[1]

        try:
            session = await self._get_http_session()
            async with session.get(
                f"{base_url}/delete",
                params={"proxy": proxy_str}
            ) as resp:
                if resp.status == 200:
                    logger.debug(f"已从 proxy_pool 删除: {proxy_str}")
        except Exception as e:
            logger.debug(f"从 proxy_pool 删除代理失败: {e}")

    async def _refresh_from_proxy_pool(self):
        """从 proxy_pool 刷新代理列表"""
        if not self._use_proxy_pool:
            return

        logger.info("正在从 proxy_pool 刷新代理...")
        new_proxies = await self._load_from_proxy_pool()

        if new_proxies:
            # 替换现有代理列表
            self._proxies = new_proxies
            alive = sum(1 for p in self._proxies if p.is_alive)
            logger.info(f"proxy_pool 刷新完成，可用代理: {alive}/{len(self._proxies)}")
        else:
            logger.warning("proxy_pool 刷新失败，保持现有代理列表")

    # ==================== 文件 & 通用 API ====================

    def _load_from_file(self) -> List[ProxyInfo]:
        """从文件加载代理列表"""
        proxies = []
        try:
            with open(self.config.proxy_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    proxy = self._parse_proxy_line(line)
                    if proxy:
                        proxies.append(proxy)
        except FileNotFoundError:
            logger.debug(f"代理文件 {self.config.proxy_file} 不存在")
        return proxies

    def _parse_proxy_line(self, line: str) -> Optional[ProxyInfo]:
        """解析代理行，支持格式:
        - ip:port
        - protocol://ip:port
        - ip:port:user:pass
        - protocol://user:pass@ip:port
        """
        line = line.strip()
        if "://" in line:
            protocol, rest = line.split("://", 1)
            if "@" in rest:
                auth, host_port = rest.rsplit("@", 1)
                url = f"{protocol}://{auth}@{host_port}"
            else:
                url = f"{protocol}://{rest}"
            return ProxyInfo(url=url, protocol=protocol)
        else:
            parts = line.split(":")
            if len(parts) == 2:
                return ProxyInfo(url=f"http://{line}", protocol="http")
            elif len(parts) == 4:
                ip, port, user, pwd = parts
                return ProxyInfo(url=f"http://{user}:{pwd}@{ip}:{port}", protocol="http")
        return None

    async def _load_from_api(self) -> List[ProxyInfo]:
        """从代理API加载"""
        proxies = []
        try:
            session = await self._get_http_session()
            async with session.get(self.config.proxy_api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, str):
                                proxy = self._parse_proxy_line(item)
                                if proxy:
                                    proxies.append(proxy)
                            elif isinstance(item, dict):
                                ip = item.get("ip", "")
                                port = item.get("port", "")
                                protocol = item.get("protocol", "http")
                                url = f"{protocol}://{ip}:{port}"
                                proxies.append(ProxyInfo(url=url, protocol=protocol))
        except Exception as e:
            logger.error(f"从API加载代理失败: {e}")
        return proxies

    # ==================== 多源抓取 ====================

    async def _load_from_multi_source(self) -> List[ProxyInfo]:
        """使用 MultiSourceFetcher 从多个源并发抓取代理"""
        proxies = []
        try:
            fetcher = MultiSourceFetcher(timeout=10.0, max_per_source=200)
            raw_proxies = await fetcher.fetch_all()

            for rp in raw_proxies:
                proxies.append(ProxyInfo(
                    url=rp.url,
                    protocol=rp.protocol,
                    is_alive=True,  # 后续验证阶段会筛选
                ))

            # 打印各源统计
            for sr in fetcher.get_source_stats():
                status = "✓" if sr.count > 0 else "✗"
                err = f" ({sr.error})" if sr.error else ""
                print(f"    {status} [{sr.name}] {sr.count} 个 ({sr.elapsed_ms:.0f}ms){err}")

            await fetcher.close()
        except Exception as e:
            logger.error(f"多源抓取失败: {e}")

        return proxies

    # ==================== FreeProxy 集成 ====================

    async def _load_from_freeproxy(self) -> List[ProxyInfo]:
        """从 FreeProxy 库加载代理 (CharlesPikachu/FreeProxy)
        无需 Docker/Redis，直接在代码中调用抓取免费代理
        pip install freeproxy

        策略: 直接调用快速源(from_free_proxy_list等)，
        跳过慢源(getproxy.jp需爬98页且全502)
        """
        proxies = []
        try:
            from freeproxy.proxy import (
                from_free_proxy_list, from_cn_proxy, from_proxy_spy,
                from_hide_my_ip, from_pachong_org, from_gather_proxy,
            )
            import warnings
            warnings.filterwarnings("ignore")

            https_only = self.config.freeproxy_https_only
            filter_desc = "仅HTTPS" if https_only else "全部(HTTP/HTTPS)"
            print(f"  FreeProxy: 正在从可靠源抓取免费代理 (过滤: {filter_desc})...")

            # 只调用快速可靠的源，跳过 getproxy.jp (98页全502) 和 xici_daili (HTTP Error)
            fast_sources = [
                ("free_proxy_list", from_free_proxy_list),  # 最可靠，300个
                ("cn_proxy", from_cn_proxy),
                ("proxy_spy", from_proxy_spy),
                ("hide_my_ip", from_hide_my_ip),
                ("gather_proxy", from_gather_proxy),
                ("pachong_org", from_pachong_org),
            ]

            loop = asyncio.get_event_loop()
            all_raw = []
            seen = set()  # 去重

            for name, func in fast_sources:
                try:
                    # 在线程池中运行同步源函数，每个源最多10秒
                    raw = await asyncio.wait_for(
                        loop.run_in_executor(None, func),
                        timeout=10.0
                    )
                    if raw:
                        new_count = 0
                        for p in raw:
                            # 源函数返回 str ("ip:port") 或 peewee Model
                            if isinstance(p, str):
                                proxy_str = p.strip()
                                if proxy_str and proxy_str not in seen:
                                    seen.add(proxy_str)
                                    all_raw.append(proxy_str)
                                    new_count += 1
                            else:
                                # peewee Model 对象
                                proxy_val = getattr(p, 'proxy', '') or ''
                                if proxy_val and proxy_val not in seen:
                                    seen.add(proxy_val)
                                    all_raw.append(proxy_val)
                                    new_count += 1
                        if new_count > 0:
                            print(f"    [{name}] +{new_count} 个新代理")
                except asyncio.TimeoutError:
                    print(f"    [{name}] 超时，跳过")
                except Exception as e:
                    print(f"    [{name}] 错误: {e}")

            if not all_raw:
                logger.warning("FreeProxy 未抓取到任何代理")
                return proxies

            # 构建代理列表
            # 注意: 免费代理多为 HTTP 代理，但通过 CONNECT 方法也能转发 HTTPS
            count = 0
            for proxy_str in all_raw:
                try:
                    # 简单验证格式
                    if ':' not in proxy_str:
                        continue
                    proxy_url = f"http://{proxy_str}"
                    proxies.append(ProxyInfo(
                        url=proxy_url,
                        protocol="http",  # 免费代理多为 HTTP
                        is_alive=True,  # 后续验证阶段会筛选
                    ))
                    count += 1
                except Exception:
                    continue

            print(f"  FreeProxy: 共获取到 {count} 个去重代理 ({filter_desc})")

        except ImportError:
            logger.error("FreeProxy 未安装，请运行: pip install freeproxy")
        except Exception as e:
            logger.error(f"FreeProxy 加载失败: {e}")

        return proxies

    # ==================== 验证 ====================

    async def _validate_all(self):
        """验证所有代理的可用性 (使用改进的ProxyValidator)"""
        if not self._proxies:
            return

        logger.info(f"正在验证 {len(self._proxies)} 个代理...")
        print(f"  验证中: {len(self._proxies)} 个代理 "
              f"(并发={self.config.proxy_validate_concurrency}, "
              f"超时={self.config.proxy_validate_timeout}s, "
              f"重试={self.config.proxy_validate_retry})")

        # 构建目标站验证URL
        target_url = None
        if self.config.proxy_validate_target:
            target_url = self.config.register_url or self.config.send_code_url

        validator = ProxyValidator(
            validate_url=self.config.proxy_validate_url,
            timeout=float(self.config.proxy_validate_timeout),
            concurrency=self.config.proxy_validate_concurrency,
            retry_count=self.config.proxy_validate_retry,
            target_url=target_url,
        )

        # 将 ProxyInfo 转为 RawProxy 进行验证
        raw_proxies = [
            RawProxy(url=p.url, source="local", protocol=p.protocol)
            for p in self._proxies
        ]

        valid = await validator.validate_batch(raw_proxies)
        valid_urls = {p.url for p in valid}

        # 更新 ProxyInfo 状态
        for p in self._proxies:
            if p.url in valid_urls:
                p.is_alive = True
                p.last_check = time.time()
                # 找到对应的 RawProxy 获取速度信息
                for vp in valid:
                    if vp.url == p.url:
                        p.avg_response_time = vp.speed_ms
                        break
            else:
                p.is_alive = False

        alive_count = sum(1 for p in self._proxies if p.is_alive)
        print(f"  验证完成: {alive_count}/{len(self._proxies)} 个代理可用")

    # ==================== 获取 & 报告 ====================

    async def get_proxy(self) -> Optional[str]:
        """根据策略获取一个代理URL"""
        # proxy_pool 实时模式: 每次直接从proxy_pool获取
        if self._use_proxy_pool and self.config.proxy_rotate_strategy == "proxy_pool":
            proxy_url = await self._get_proxy_from_pool()
            if proxy_url:
                return proxy_url
            # 回退到本地缓存

        # 定期从 proxy_pool 刷新本地缓存
        if (self._use_proxy_pool and
                time.time() - self._last_refresh > self.config.proxy_refresh_interval):
            await self._refresh_from_proxy_pool()
            self._last_refresh = time.time()

        async with self._lock:
            alive_proxies = [p for p in self._proxies if p.is_alive]
            if not alive_proxies:
                # 尝试从proxy_pool实时获取
                if self._use_proxy_pool:
                    proxy_url = await self._get_proxy_from_pool()
                    if proxy_url:
                        # 添加到本地缓存
                        new_proxy = ProxyInfo(url=proxy_url, protocol="http", is_alive=True)
                        self._proxies.append(new_proxy)
                        return proxy_url
                return None  # 无可用代理，返回None表示使用本机IP

            strategy = self.config.proxy_rotate_strategy

            if strategy == "round_robin":
                proxy = alive_proxies[self._current_index % len(alive_proxies)]
                self._current_index += 1
            elif strategy == "random":
                proxy = random.choice(alive_proxies)
            elif strategy == "least_used":
                proxy = min(alive_proxies, key=lambda p: p.total_requests)
            elif strategy == "proxy_pool":
                # 已在上面处理了实时获取，这里从缓存中随机取
                proxy = random.choice(alive_proxies)
            else:
                proxy = alive_proxies[self._current_index % len(alive_proxies)]
                self._current_index += 1

            return proxy.url

    async def report_success(self, proxy_url: str, response_time: float):
        """报告代理请求成功"""
        async with self._lock:
            for p in self._proxies:
                if p.url == proxy_url:
                    p.success_count += 1
                    if p.avg_response_time == 0:
                        p.avg_response_time = response_time
                    else:
                        p.avg_response_time = 0.7 * p.avg_response_time + 0.3 * response_time
                    break

    async def report_failure(self, proxy_url: str):
        """报告代理请求失败"""
        async with self._lock:
            for p in self._proxies:
                if p.url == proxy_url:
                    p.fail_count += 1
                    if p.fail_count >= self.config.proxy_max_fails:
                        p.is_alive = False
                        # 从 proxy_pool 中删除该失效代理
                        if self._use_proxy_pool:
                            await self._delete_proxy_from_pool(proxy_url)
                        logger.warning(f"代理 {p.url} 失败次数过多，已标记为不可用")
                    break

    async def refresh_proxies(self):
        """刷新代理池"""
        if self._use_proxy_pool:
            # proxy_pool模式: 直接从proxy_pool刷新
            await self._refresh_from_proxy_pool()
        else:
            # 传统模式: 重新验证 + 从API获取新代理
            logger.info("正在刷新IP池...")

            dead_proxies = [p for p in self._proxies if not p.is_alive]
            if dead_proxies:
                for proxy in dead_proxies:
                    is_valid = await self._validate_proxy(proxy)
                    if is_valid:
                        proxy.is_alive = True
                        proxy.fail_count = 0
                        logger.info(f"代理 {proxy.url} 恢复可用")

            if self.config.proxy_api_url:
                new_proxies = await self._load_from_api()
                if new_proxies:
                    existing_urls = {p.url for p in self._proxies}
                    for np in new_proxies:
                        if np.url not in existing_urls:
                            is_valid = await self._validate_proxy(np)
                            if is_valid:
                                np.is_alive = True
                                self._proxies.append(np)

        alive = sum(1 for p in self._proxies if p.is_alive)
        logger.info(f"IP池刷新完成，可用代理: {alive}/{len(self._proxies)}")

    def get_pool_stats(self) -> Dict:
        """获取IP池统计信息"""
        alive = [p for p in self._proxies if p.is_alive]
        return {
            "total": len(self._proxies),
            "alive": len(alive),
            "dead": len(self._proxies) - len(alive),
            "avg_response_time": (
                sum(p.avg_response_time for p in alive) / len(alive)
                if alive else 0
            ),
            "proxy_pool_enabled": self._use_proxy_pool,
        }

