"""
压力测试配置文件
"""
from dataclasses import dataclass, field
from typing import List, Optional


# 常见注册接口路径，用于自动探测
REGISTER_API_PATTERNS: List[str] = [
    "/admin/api/register",   # 快搜实际接口
    "/api/register",
    "/api/user/register",
    "/api/v1/register",
    "/api/v1/user/register",
    "/api/auth/register",
    "/api/auth/signup",
    "/user/register",
    "/register",
    "/signup",
    "/api/signup",
    "/api/member/register",
    "/api/account/register",
]


@dataclass
class StressTestConfig:
    """压力测试全局配置"""

    # 目标站点
    base_url: str = "https://www.kuaisou.com"
    register_url: str = "https://www.kuaisou.com/admin/api/register"  # 注册接口(已确认)
    send_code_url: str = "https://www.kuaisou.com/admin/api/send-code"  # 验证码接口
    login_page_url: str = "https://www.kuaisou.com/login"
    auto_discover_api: bool = False  # 已知接口，无需自动探测

    # 并发控制
    concurrent_users: int = 10           # 同时并发用户数
    total_requests: int = 100            # 总注册请求数
    ramp_up_seconds: float = 5.0         # 逐步增加用户的秒数
    request_interval_min: float = 0.5    # 请求最小间隔(秒)
    request_interval_max: float = 2.0    # 请求最大间隔(秒)

    # 超时设置
    connect_timeout: int = 10            # 连接超时(秒)
    read_timeout: int = 30               # 读取超时(秒)

    # IP池配置
    proxy_file: str = "proxies.txt"       # 代理IP文件路径
    proxy_api_url: Optional[str] = None   # 通用代理API地址(可选)
    proxy_pool_url: str = ""              # proxy_pool API地址(如 http://127.0.0.1:5010)
    proxy_pool_mode: str = ""             # proxy_pool获取模式: get随机 / pop获取并删除
    use_freeproxy: bool = False           # 启用 FreeProxy 内置代理抓取(无需Docker)
    freeproxy_https_only: bool = False    # FreeProxy仅获取HTTPS代理(默认False: HTTP代理也能通过CONNECT转发HTTPS)
    proxy_validate_url: str = "https://httpbin.org/ip"
    proxy_validate_timeout: int = 10     # 代理验证超时(秒)
    proxy_max_fails: int = 3             # 代理最大失败次数后移除
    proxy_rotate_strategy: str = "round_robin"  # round_robin / random / least_used / proxy_pool
    proxy_refresh_interval: int = 60     # 从proxy_pool刷新代理的间隔(秒)

    # 反爬虫应对
    user_agent_rotate: bool = True       # 是否轮换User-Agent
    referer: str = "https://www.kuaisou.com/login"
    random_delay: bool = True            # 是否添加随机延迟

    # 测试报告
    report_file: str = "stress_test_report.json"
    log_file: str = "stress_test.log"

    # 注册接口字段 (已通过抓包确认)
    # POST /admin/api/register
    # Body: {"mobile": "手机号", "password": "密码"}
    # 响应: {"success": true/false, "error": "错误信息"}
    # 注意: 不需要验证码即可注册，需要唯一手机号
    register_fields: dict = field(default_factory=lambda: {
        "mobile": "mobile",          # 手机号字段名
        "password": "password",     # 密码字段名
    })

    # 发送验证码接口字段 (备用，压力测试通常跳过)
    # POST /admin/api/send-code
    # Body: {"account": "手机号", "scene": "auto"}
    send_code_fields: dict = field(default_factory=lambda: {
        "account": "account",        # 手机号/邮箱字段名
        "scene": "scene",            # 场景字段名
    })
    send_code_scene: str = "auto"     # 注册场景值

    # 验证码处理策略
    captcha_strategy: str = "skip"       # skip (注册不需要验证码) / send_code
