"""
API接口自动探测模块
分析页面JS资源，探测注册接口的真实地址
"""
import asyncio
import re
import logging
from typing import List, Optional, Set, Dict, Any
from dataclasses import dataclass, field

import aiohttp

from config import StressTestConfig, REGISTER_API_PATTERNS

logger = logging.getLogger("discover")


@dataclass
class DiscoveryResult:
    """探测结果"""
    register_url: Optional[str] = None       # 发现的注册接口
    js_urls: List[str] = field(default_factory=list)  # 页面JS资源
    form_action: Optional[str] = None         # 表单action
    api_endpoints: Set[str] = field(default_factory=set)  # 从JS中提取的所有API路径
    csrf_token_name: Optional[str] = None     # CSRF token字段名


class APIDiscoverer:
    """API接口探测器"""

    def __init__(self, config: StressTestConfig):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def discover(self) -> DiscoveryResult:
        """执行完整的接口探测流程"""
        result = DiscoveryResult()
        session = await self._get_session()

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        print("\n🔍 正在探测注册接口...")

        # 第1步: 获取页面HTML
        try:
            async with session.get(
                self.config.login_page_url,
                headers=headers,
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    print(f"  ⚠ 页面请求失败: HTTP {resp.status}")
                    return result

                html = await resp.text()
                print(f"  ✓ 获取页面成功，大小: {len(html)} 字节")
        except Exception as e:
            print(f"  ⚠ 页面请求异常: {e}")
            return result

        # 第2步: 从HTML提取JS资源URL
        js_urls = self._extract_js_urls(html)
        result.js_urls = js_urls
        print(f"  ✓ 发现 {len(js_urls)} 个JS资源")

        # 第3步: 从HTML提取表单信息
        form_info = self._extract_form_info(html)
        if form_info.get("action"):
            result.form_action = form_info["action"]
            print(f"  ✓ 表单action: {form_info['action']}")
        if form_info.get("csrf_name"):
            result.csrf_token_name = form_info["csrf_name"]

        # 第4步: 分析JS资源，提取API路径
        all_endpoints: Set[str] = set()
        for js_url in js_urls[:10]:  # 最多分析10个JS文件
            endpoints = await self._analyze_js(session, js_url)
            all_endpoints.update(endpoints)
            if endpoints:
                print(f"  ✓ {js_url}: 发现 {len(endpoints)} 个API路径")

        result.api_endpoints = all_endpoints

        # 第5步: 匹配注册相关的API
        register_endpoints = self._filter_register_endpoints(all_endpoints)
        if register_endpoints:
            print(f"  ✓ 发现注册相关API: {register_endpoints}")

        # 第6步: 验证API端点可用性
        verified_url = await self._verify_endpoints(session, register_endpoints)
        if verified_url:
            result.register_url = verified_url
            print(f"  ✅ 注册接口确认: {verified_url}")
        else:
            # 回退: 尝试常见路径列表
            print("  ℹ JS分析未找到明确接口，尝试常见路径...")
            verified_url = await self._probe_common_paths(session)
            if verified_url:
                result.register_url = verified_url
                print(f"  ✅ 通过路径探测确认: {verified_url}")
            else:
                # 最终回退: 使用页面URL本身（可能是表单提交）
                result.register_url = self.config.login_page_url
                print(f"  ⚠ 未找到API接口，将使用页面URL: {result.register_url}")

        return result

    def _extract_js_urls(self, html: str) -> List[str]:
        """从HTML提取JS资源URL"""
        urls = []
        # <script src="...">
        pattern = r'<script[^>]+src=["\']([^"\']+\.js[^"\']*)["\']'
        for match in re.finditer(pattern, html):
            url = match.group(1)
            if not url.startswith("http"):
                if url.startswith("/"):
                    url = self.config.base_url + url
                else:
                    url = self.config.base_url + "/" + url
            urls.append(url)

        # <link href="..."> 中也可能有 chunk
        pattern2 = r'<link[^>]+href=["\']([^"\']+\.js[^"\']*)["\']'
        for match in re.finditer(pattern2, html):
            url = match.group(1)
            if not url.startswith("http"):
                if url.startswith("/"):
                    url = self.config.base_url + url
                else:
                    url = self.config.base_url + "/" + url
            urls.append(url)

        return list(dict.fromkeys(urls))  # 去重保序

    def _extract_form_info(self, html: str) -> Dict[str, Any]:
        """从HTML提取表单信息"""
        info: Dict[str, Any] = {}

        # 表单action
        form_pattern = r'<form[^>]+action=["\']([^"\']+)["\']'
        match = re.search(form_pattern, html)
        if match:
            info["action"] = match.group(1)

        # CSRF token
        csrf_patterns = [
            r'name="(csrf[_-]?token|_token|authenticity_token)"[^>]*value=["\']([^"\']+)["\']',
            r'value=["\']([^"\']+)["\'][^>]*name="(csrf[_-]?token|_token|authenticity_token)"',
            r'meta\s+name="csrf-token"\s+content="([^"]+)"',
        ]
        for pattern in csrf_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) >= 2:
                    info["csrf_name"] = groups[0]
                    info["csrf_value"] = groups[1]
                elif len(groups) == 1:
                    info["csrf_name"] = "csrf-token"
                    info["csrf_value"] = groups[0]
                break

        return info

    async def _analyze_js(self, session: aiohttp.ClientSession, js_url: str) -> Set[str]:
        """分析JS文件内容，提取API路径"""
        endpoints: Set[str] = set()
        try:
            async with session.get(js_url, ssl=False) as resp:
                if resp.status != 200:
                    return endpoints
                content = await resp.text()

            # 常见API路径模式
            patterns = [
                r'["\'](/api/[^"\']+)["\']',
                r'["\'](/user/[^"\']+)["\']',
                r'["\'](/auth/[^"\']+)["\']',
                r'["\'](/register[^"\']*)["\']',
                r'["\'](/signup[^"\']*)["\']',
                r'["\']`(/api/[^`]+)`["\']',
                r'baseURL\s*[:=]\s*["\']([^"\']+)["\']',
                r'url\s*[:=]\s*["\']([^"\']*(?:register|signup|login)[^"\']*)["\']',
                r'axios\.\w+\s*\(\s*["\']([^"\']+)["\']',
                r'request\s*\(\s*{[^}]*url\s*:\s*["\']([^"\']+)["\']',
                r'post\s*\(\s*["\']([^"\']+)["\']',
                r'\$\{[^}]*\}(/api/[^"\']*)',
            ]

            for pattern in patterns:
                for match in re.finditer(pattern, content, re.IGNORECASE):
                    endpoint = match.group(1)
                    if endpoint and not endpoint.endswith(
                        (".js", ".css", ".png", ".jpg", ".svg", ".ico")
                    ):
                        endpoints.add(endpoint)

        except Exception as e:
            logger.debug(f"分析JS失败 {js_url}: {e}")

        return endpoints

    def _filter_register_endpoints(self, endpoints: Set[str]) -> List[str]:
        """筛选注册相关的API端点"""
        register_keywords = ["register", "signup", "sign-up", "regist"]
        filtered = []
        for ep in endpoints:
            ep_lower = ep.lower()
            if any(kw in ep_lower for kw in register_keywords):
                filtered.append(ep)

        # 也加入预定义的常见路径
        for pattern in REGISTER_API_PATTERNS:
            if pattern not in filtered:
                filtered.append(pattern)

        # 按相关性排序
        def relevance(url: str) -> int:
            url_lower = url.lower()
            if "register" in url_lower:
                return 0
            if "signup" in url_lower or "sign-up" in url_lower:
                return 1
            if "auth" in url_lower:
                return 2
            return 3

        filtered.sort(key=relevance)
        return filtered

    async def _verify_endpoints(
        self, session: aiohttp.ClientSession, endpoints: List[str]
    ) -> Optional[str]:
        """验证端点是否可用"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36",
            "Origin": self.config.base_url,
            "Referer": self.config.referer,
            "Content-Type": "application/json",
        }

        for endpoint in endpoints[:8]:
            url = self._resolve_url(endpoint)
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json={},
                    ssl=False,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 404:
                        logger.info(f"端点验证通过: {url} (HTTP {resp.status})")
                        return url
                    else:
                        logger.debug(f"端点不存在: {url} (404)")
            except asyncio.TimeoutError:
                logger.debug(f"端点超时: {url}")
            except Exception as e:
                logger.debug(f"端点验证异常: {url} - {e}")

        return None

    async def _probe_common_paths(self, session: aiohttp.ClientSession) -> Optional[str]:
        """探测常见注册API路径"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36",
            "Content-Type": "application/json",
            "Origin": self.config.base_url,
        }

        for path in REGISTER_API_PATTERNS:
            url = self.config.base_url + path
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json={},
                    ssl=False,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 404:
                        print(f"    ✓ 路径 {path} 响应 HTTP {resp.status}")
                        return url
                    else:
                        print(f"    ✗ 路径 {path} 返回 404")
            except asyncio.TimeoutError:
                print(f"    ⏱ 路径 {path} 超时")
            except Exception as e:
                print(f"    ✗ 路径 {path} 异常: {type(e).__name__}")

        return None

    def _resolve_url(self, endpoint: str) -> str:
        """将相对路径解析为完整URL"""
        if endpoint.startswith("http"):
            return endpoint
        if endpoint.startswith("/"):
            return self.config.base_url + endpoint
        return self.config.base_url + "/" + endpoint
