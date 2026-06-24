"""
注册流程模拟模块
适配快搜实际接口: POST /admin/api/register
Body: {"mobile": "手机号", "password": "密码"}
响应: {"success": true/false, "error": "错误信息"}
"""
import asyncio
import json
import random
import string
import time
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

import aiohttp
from faker import Faker

from config import StressTestConfig
from ip_pool import IPPoolManager

logger = logging.getLogger("register")

# User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]


@dataclass
class RegisterResult:
    """单次注册结果"""
    task_id: int
    success: bool
    status_code: Optional[int] = None
    response_time_ms: float = 0.0
    error_type: Optional[str] = None      # timeout / captcha / network / http_error / other
    error_message: Optional[str] = None
    proxy_used: Optional[str] = None
    timestamp: float = 0.0
    account_used: Optional[str] = None


class RegisterSimulator:
    """注册流程模拟器 - 适配快搜 /admin/api/register 接口"""

    def __init__(self, config: StressTestConfig, ip_pool: IPPoolManager,
                 submit_mode: str = "json"):
        """
        Args:
            submit_mode: 提交模式 (快搜接口固定为json)
        """
        self.config = config
        self.ip_pool = ip_pool
        self.faker = Faker(["zh_CN", "en_US"])
        self._task_counter = 0
        self.submit_mode = submit_mode

        # 手机号前缀池
        self._phone_prefixes = [
            "130", "131", "132", "133", "134", "135", "136", "137",
            "138", "139", "150", "151", "152", "153", "155",
            "156", "157", "158", "159", "170", "171", "172",
            "173", "175", "176", "177", "178", "180", "181",
            "182", "183", "184", "185", "186", "187", "188", "189",
        ]

    def _generate_mobile(self) -> str:
        """生成随机手机号"""
        prefix = random.choice(self._phone_prefixes)
        suffix = "".join(random.choices(string.digits, k=8))
        return prefix + suffix

    def _generate_user_data(self) -> Dict[str, str]:
        """生成注册数据"""
        mobile = self._generate_mobile()
        password = self._generate_password()

        return {
            self.config.register_fields["mobile"]: mobile,
            self.config.register_fields["password"]: password,
        }

    def _generate_password(self) -> str:
        """生成随机密码"""
        length = random.randint(10, 16)
        chars = string.ascii_letters + string.digits + "!@#$%"
        # 确保包含大小写字母和数字
        password = [
            random.choice(string.ascii_uppercase),
            random.choice(string.ascii_lowercase),
            random.choice(string.digits),
            random.choice("!@#$%"),
        ]
        password += random.choices(chars, k=length - 4)
        random.shuffle(password)
        return "".join(password)

    def _get_headers(self) -> Dict[str, str]:
        """生成请求头 - 匹配浏览器实际请求"""
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Content-Type": "application/json",
            "Origin": self.config.base_url,
            "Referer": self.config.referer,
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        if self.config.user_agent_rotate:
            headers["User-Agent"] = random.choice(USER_AGENTS)
        else:
            headers["User-Agent"] = USER_AGENTS[0]

        return headers

    async def execute_register(self) -> RegisterResult:
        """执行一次注册请求 (POST /admin/api/register)"""
        self._task_counter += 1
        task_id = self._task_counter
        start_time = time.time()

        # 获取代理
        proxy_url = await self.ip_pool.get_proxy()

        # 生成注册数据
        user_data = self._generate_user_data()
        account_used = user_data.get(self.config.register_fields["mobile"], "unknown")

        # 随机延迟(反爬虫)
        if self.config.random_delay:
            delay = random.uniform(
                self.config.request_interval_min,
                self.config.request_interval_max,
            )
            await asyncio.sleep(delay)

        headers = self._get_headers()
        timeout = aiohttp.ClientTimeout(
            connect=self.config.connect_timeout,
            total=self.config.read_timeout,
        )

        result = RegisterResult(
            task_id=task_id,
            success=False,
            proxy_used=proxy_url,
            timestamp=start_time,
            account_used=account_used,
        )

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # 提交注册请求
                request_start = time.time()
                async with session.post(
                    self.config.register_url,
                    proxy=proxy_url,
                    headers=headers,
                    json=user_data,
                    ssl=False,
                    allow_redirects=False,
                ) as resp:
                    response_time = (time.time() - request_start) * 1000
                    result.status_code = resp.status
                    result.response_time_ms = response_time
                    body = await resp.text()

                    # 快搜接口: HTTP始终200，通过JSON body的success字段判断
                    if resp.status == 200:
                        try:
                            data = json.loads(body)
                            if data.get("success") is True:
                                result.success = True
                                await self.ip_pool.report_success(proxy_url, response_time)
                            else:
                                error_msg = data.get("error", "")
                                result.error_type = self._classify_error(error_msg)
                                result.error_message = error_msg[:200]
                                await self.ip_pool.report_failure(proxy_url)
                        except json.JSONDecodeError:
                            result.error_type = "invalid_response"
                            result.error_message = body[:200]
                            await self.ip_pool.report_failure(proxy_url)
                    elif resp.status == 429:
                        result.error_type = "rate_limit"
                        result.error_message = "请求频率过高"
                        await self.ip_pool.report_failure(proxy_url)
                    elif resp.status in (403, 451):
                        result.error_type = "blocked"
                        result.error_message = f"IP被封禁 (HTTP {resp.status})"
                        await self.ip_pool.report_failure(proxy_url)
                    elif resp.status == 503:
                        result.error_type = "server_overloaded"
                        result.error_message = "服务不可用(可能过载)"
                        await self.ip_pool.report_failure(proxy_url)
                    else:
                        result.error_type = "http_error"
                        result.error_message = f"HTTP {resp.status}"
                        await self.ip_pool.report_failure(proxy_url)

        except asyncio.TimeoutError:
            result.error_type = "timeout"
            result.error_message = "请求超时"
            result.response_time_ms = (time.time() - start_time) * 1000
            if proxy_url:
                await self.ip_pool.report_failure(proxy_url)
        except aiohttp.ClientConnectorError as e:
            result.error_type = "connection_error"
            result.error_message = str(e)[:200]
            if proxy_url:
                await self.ip_pool.report_failure(proxy_url)
        except aiohttp.ClientProxyConnectionError as e:
            result.error_type = "proxy_error"
            result.error_message = f"代理连接失败: {str(e)[:100]}"
            if proxy_url:
                await self.ip_pool.report_failure(proxy_url)
        except Exception as e:
            result.error_type = "other"
            result.error_message = f"{type(e).__name__}: {str(e)[:100]}"
            if proxy_url:
                await self.ip_pool.report_failure(proxy_url)

        return result

    def _classify_error(self, error_msg: str) -> str:
        """对错误响应分类 (基于快搜实际返回的错误信息)"""
        if "已注册" in error_msg or "已存在" in error_msg:
            return "account_exists"
        if "验证码" in error_msg:
            return "captcha"
        if "手机号" in error_msg or "邮箱" in error_msg or "格式" in error_msg:
            return "validation_error"
        if "频繁" in error_msg or "限制" in error_msg or "流控" in error_msg:
            return "rate_limit"
        if "封禁" in error_msg or "禁止" in error_msg:
            return "blocked"
        if "服务器" in error_msg or "内部" in error_msg:
            return "server_error"
        return "unknown_error"
