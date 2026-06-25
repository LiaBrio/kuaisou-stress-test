"""
短信验证码接口模拟模块
适配快搜接口: POST /admin/api/send-code
Body: {"account": "手机号", "scene": "auto"}
响应: {"success": true/false, "message": "验证码发送成功", "ttl": 300, "type": "register"}
"""
import asyncio
import json
import random
import string
import time
import logging
from typing import Optional, Dict
from dataclasses import dataclass

import aiohttp

from config import StressTestConfig
from ip_pool import IPPoolManager
from register import USER_AGENTS

logger = logging.getLogger("sms")


@dataclass
class SendCodeResult:
    """单次发送验证码结果"""
    task_id: int
    success: bool
    status_code: Optional[int] = None
    response_time_ms: float = 0.0
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    proxy_used: Optional[str] = None
    timestamp: float = 0.0
    account_used: Optional[str] = None
    ttl: Optional[int] = None          # 验证码有效期(秒)
    code_type: Optional[str] = None    # register/login等


class SendCodeSimulator:
    """短信验证码发送模拟器 - 适配快搜 /admin/api/send-code 接口"""

    def __init__(self, config: StressTestConfig, ip_pool: IPPoolManager,
                 scene: str = "auto"):
        self.config = config
        self.ip_pool = ip_pool
        self._task_counter = 0
        self.scene = scene
        self._session: Optional[aiohttp.ClientSession] = None
        self._used_phones: set = set()  # 去重集合，确保号码唯一

        self._phone_prefixes = [
            "130", "131", "132", "133", "134", "135", "136", "137",
            "138", "139", "150", "151", "152", "153", "155",
            "156", "157", "158", "159", "170", "171", "172",
            "173", "175", "176", "177", "178", "180", "181",
            "182", "183", "184", "185", "186", "187", "188", "189",
        ]

    def _generate_mobile(self) -> str:
        """生成随机手机号，确保不重复"""
        for _ in range(100):  # 最多重试100次
            prefix = random.choice(self._phone_prefixes)
            suffix = "".join(random.choices(string.digits, k=8))
            mobile = prefix + suffix
            if mobile not in self._used_phones:
                self._used_phones.add(mobile)
                return mobile
        # 极端情况：加时间戳后缀保证唯一
        ts = str(int(time.time() * 1000))[-8:]
        prefix = random.choice(self._phone_prefixes)
        mobile = prefix + ts
        self._used_phones.add(mobile)
        return mobile

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取共享Session（连接池复用）"""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=0,           # 不限制总连接数
                limit_per_host=0,  # 不限制单host连接数
                ttl_dns_cache=300,
                ssl=False,
            )
            timeout = aiohttp.ClientTimeout(
                connect=self.config.connect_timeout,
                total=self.config.read_timeout,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
            )
        return self._session

    async def close(self):
        """关闭共享Session"""
        if self._session and not self._session.closed:
            await self._session.close()

    def _get_headers(self) -> Dict[str, str]:
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

    async def execute_send_code(self) -> SendCodeResult:
        """执行一次发送验证码请求"""
        self._task_counter += 1
        task_id = self._task_counter
        start_time = time.time()

        proxy_url = await self.ip_pool.get_proxy()
        mobile = self._generate_mobile()

        if self.config.random_delay:
            delay = random.uniform(
                self.config.request_interval_min,
                self.config.request_interval_max,
            )
            await asyncio.sleep(delay)

        headers = self._get_headers()

        payload = {"account": mobile, "scene": self.scene}

        result = SendCodeResult(
            task_id=task_id,
            success=False,
            proxy_used=proxy_url,
            timestamp=start_time,
            account_used=mobile,
        )

        try:
            session = await self._get_session()
            request_start = time.time()
            async with session.post(
                self.config.send_code_url,
                proxy=proxy_url,
                headers=headers,
                json=payload,
                allow_redirects=False,
            ) as resp:
                response_time = (time.time() - request_start) * 1000
                result.status_code = resp.status
                result.response_time_ms = response_time
                body = await resp.text()

                if resp.status == 200:
                    try:
                        data = json.loads(body)
                        if data.get("success") is True:
                            result.success = True
                            result.ttl = data.get("ttl")
                            result.code_type = data.get("type")
                            await self.ip_pool.report_success(proxy_url, response_time)
                        else:
                            error_msg = data.get("error") or data.get("message") or ""
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
                    result.error_message = "服务不可用"
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
        """对错误响应分类"""
        msg = error_msg.lower() if error_msg else ""
        # IP限频优先判断
        if "IP" in error_msg or "ip" in msg:
            if "上限" in error_msg or "限制" in error_msg or "已达" in error_msg:
                return "ip_rate_limit"
        if "频繁" in error_msg or "限制" in error_msg or "流控" in error_msg or "rate" in msg:
            return "rate_limit"
        if "验证码" in error_msg and ("已发送" in error_msg or "重复发送" in error_msg or "已发" in error_msg and "达" not in error_msg):
            return "already_sent"
        if "手机号" in error_msg or "格式" in error_msg or "invalid" in msg:
            return "validation_error"
        if "封禁" in error_msg or "禁止" in error_msg:
            return "blocked"
        if "服务器" in error_msg or "内部" in error_msg or "500" in error_msg:
            return "server_error"
        return "unknown_error"
