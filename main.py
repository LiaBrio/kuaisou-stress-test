#!/usr/bin/env python3
"""
快搜注册功能压力测试主程序
支持并发控制、IP轮换、实时统计、报告生成

用法:
  python main.py                                    # 默认配置
  python main.py --concurrent 20 --total 500        # 自定义并发和总数
  python main.py --ramp-up 10 --interval 0.3 1.5    # 自定义爬升和间隔
  python main.py --no-proxy                         # 不使用代理
  python main.py --step-test                        # 阶梯递增加压模式
"""
import asyncio
import argparse
import logging
import sys
import time
import json
from typing import List

from config import StressTestConfig
from ip_pool import IPPoolManager
from register import RegisterSimulator
from sms import SendCodeSimulator
from stats import TestStats, print_live_stats, print_final_report
from discover import APIDiscoverer


def setup_logging(log_file: str, verbose: bool = False):
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO

    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)  # 控制台只显示警告以上

    # 文件日志
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="快搜注册功能压力测试工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py --concurrent 20 --total 200
  python main.py --step-test --max-concurrent 50
  python main.py --proxy-api "http://your-proxy-api.com/get" --proxy-file proxies.txt
        """,
    )
    parser.add_argument("--test-mode", type=str, default="register",
                        choices=["register", "sms"],
                        help="测试模式: register注册(默认)/sms短信发送")
    parser.add_argument("--concurrent", type=int, default=10,
                        help="并发用户数 (默认: 10)")
    parser.add_argument("--total", type=int, default=100,
                        help="总请求次数 (默认: 100)")
    parser.add_argument("--ramp-up", type=float, default=5.0,
                        help="爬升时间/秒 (默认: 5.0)")
    parser.add_argument("--interval", type=float, nargs=2, default=[0.5, 2.0],
                        metavar=("MIN", "MAX"),
                        help="请求间隔范围/秒 (默认: 0.5 2.0)")
    parser.add_argument("--timeout", type=int, default=30,
                        help="请求超时/秒 (默认: 30)")
    parser.add_argument("--proxy-file", type=str, default="proxies.txt",
                        help="代理IP文件路径 (默认: proxies.txt)")
    parser.add_argument("--proxy-api", type=str, default=None,
                        help="通用代理API地址")
    parser.add_argument("--proxy-pool", type=str, default=None,
                        help="proxy_pool API地址 (如 http://127.0.0.1:5010)")
    parser.add_argument("--proxy-pool-mode", type=str, default="get",
                        choices=["get", "pop"],
                        help="proxy_pool获取模式: get随机/pop获取并删除 (默认: get)")
    parser.add_argument("--proxy-strategy", type=str, default="round_robin",
                        choices=["round_robin", "random", "least_used", "proxy_pool"],
                        help="代理轮换策略 (默认: round_robin, proxy_pool模式推荐用proxy_pool)")
    parser.add_argument("--proxy-refresh", type=int, default=60,
                        help="从proxy_pool刷新代理的间隔/秒 (默认: 60)")
    parser.add_argument("--freeproxy", action="store_true",
                        help="启用FreeProxy内置代理抓取(无需Docker, pip install freeproxy)")
    parser.add_argument("--freeproxy-https-only", action="store_true",
                        help="FreeProxy仅获取HTTPS代理(默认获取全部, HTTP代理也能转发HTTPS)")
    parser.add_argument("--multi-source", action="store_true",
                        help="启用多源并发抓取代理(free-proxy-list+us-proxy+proxydb等)")
    parser.add_argument("--validate-target", action="store_true", default=True,
                        help="用目标站URL验证代理(默认启用, 比httpbin更准)")
    parser.add_argument("--no-validate-target", action="store_true",
                        help="禁用目标站验证, 仅用httpbin.org验证")
    parser.add_argument("--validate-timeout", type=int, default=8,
                        help="代理验证超时/秒 (默认: 8)")
    parser.add_argument("--validate-concurrent", type=int, default=30,
                        help="代理验证并发数 (默认: 30)")
    parser.add_argument("--validate-retry", type=int, default=1,
                        help="代理验证失败重试次数 (默认: 1)")
    parser.add_argument("--no-validate", action="store_true",
                        help="跳过代理验证，直接使用所有代理")
    parser.add_argument("--no-proxy", action="store_true",
                        help="不使用代理IP")
    parser.add_argument("--no-delay", action="store_true",
                        help="不添加随机延迟")
    parser.add_argument("--sms-scene", type=str, default="auto",
                        help="短信接口scene参数 (默认: auto)")
    parser.add_argument("--register-url", type=str, default=None,
                        help="注册接口URL (覆盖默认值)")
    parser.add_argument("--submit-mode", type=str, default="json",
                        choices=["auto", "json", "form"],
                        help="提交模式: json(默认)/form表单/auto自动")
    parser.add_argument("--no-discover", action="store_true",
                        help="禁用API接口自动探测")
    parser.add_argument("--report", type=str, default="stress_test_report.json",
                        help="报告输出文件 (默认: stress_test_report.json)")
    parser.add_argument("--burst", action="store_true",
                        help="瞬时爆发模式：所有请求同时发出，无爬升无间隔")
    parser.add_argument("--step-test", action="store_true",
                        help="启用阶梯递增加压模式")
    parser.add_argument("--max-concurrent", type=int, default=50,
                        help="阶梯模式最大并发数 (默认: 50)")
    parser.add_argument("--step-size", type=int, default=10,
                        help="阶梯模式每级增加的并发数 (默认: 10)")
    parser.add_argument("--step-duration", type=int, default=30,
                        help="阶梯模式每级持续时间/秒 (默认: 30)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="详细日志输出")
    return parser.parse_args()


async def run_fixed_load_test(
    config: StressTestConfig,
    ip_pool: IPPoolManager,
    stats: TestStats,
    submit_mode: str = "auto",
    test_mode: str = "register",
    sms_scene: str = "auto",
    burst: bool = False,
) -> TestStats:
    """固定并发量压力测试，burst=True时为瞬时爆发模式"""
    if test_mode == "sms":
        simulator = SendCodeSimulator(config, ip_pool, scene=sms_scene)
    else:
        simulator = RegisterSimulator(config, ip_pool, submit_mode=submit_mode)
    semaphore = asyncio.Semaphore(config.concurrent_users)

    stats.start_time = time.time()

    async def worker(task_id: int):
        async with semaphore:
            if test_mode == "sms":
                result = await simulator.execute_send_code()
            else:
                result = await simulator.execute_register()
            stats.record(result)

    if burst:
        # === 瞬时爆发模式 ===
        print(f"\n💥 瞬时爆发测试: 并发={config.concurrent_users}, 总请求={config.total_requests}")
        print(f"   所有请求将同时发出，无爬升无延迟\n")

        tasks = [asyncio.create_task(worker(i)) for i in range(config.total_requests)]

        try:
            # 实时打印进度
            while not all(t.done() for t in tasks):
                await asyncio.sleep(0.5)
                print_live_stats(stats)
        except KeyboardInterrupt:
            print("\n\n⚠ 收到中断信号，正在等待当前请求完成...")
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # 确保全部完成
        await asyncio.gather(*tasks, return_exceptions=True)
    else:
        # === 常规固定负载模式 ===
        interval_per_task = config.ramp_up_seconds / config.concurrent_users

        print(f"\n🚀 开始压力测试: 并发={config.concurrent_users}, 总请求={config.total_requests}")
        print(f"   爬升时间={config.ramp_up_seconds}s, 请求间隔={config.request_interval_min}-{config.request_interval_max}s")
        print(f"   按 Ctrl+C 可提前终止测试\n")

        tasks: List[asyncio.Task] = []
        launched = 0
        try:
            while launched < config.total_requests:
                batch_size = min(
                    config.concurrent_users,
                    config.total_requests - launched,
                )
                for i in range(batch_size):
                    task = asyncio.create_task(worker(launched + i))
                    tasks.append(task)
                launched += batch_size

                if launched < config.total_requests:
                    await asyncio.sleep(interval_per_task)

                done, pending = await asyncio.wait(
                    tasks, timeout=1.0, return_when=asyncio.FIRST_COMPLETED
                )
                tasks = list(pending)
                print_live_stats(stats)

        except KeyboardInterrupt:
            print("\n\n⚠ 收到中断信号，正在等待当前请求完成...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    stats.end_time = time.time()
    # 关闭模拟器Session
    if hasattr(simulator, 'close'):
        await simulator.close()
    print()
    return stats


async def run_step_load_test(
    config: StressTestConfig,
    ip_pool: IPPoolManager,
    stats: TestStats,
    max_concurrent: int,
    step_size: int,
    step_duration: int,
    submit_mode: str = "auto",
    test_mode: str = "register",
    sms_scene: str = "auto",
) -> TestStats:
    """阶梯递增加压测试"""
    if test_mode == "sms":
        simulator = SendCodeSimulator(config, ip_pool, scene=sms_scene)
    else:
        simulator = RegisterSimulator(config, ip_pool, submit_mode=submit_mode)

    stats.start_time = time.time()
    current_concurrent = step_size

    print(f"\n🏗 开始阶梯加压测试: 起始并发={step_size}, 最大并发={max_concurrent}")
    print(f"   每级持续={step_duration}s, 每级增加={step_size}并发")
    print(f"   按 Ctrl+C 可提前终止测试\n")

    try:
        while current_concurrent <= max_concurrent:
            step_stats = TestStats()
            step_stats.start_time = time.time()

            print(f"\n📌 当前级别: {current_concurrent} 并发用户")
            print("-" * 50)

            semaphore = asyncio.Semaphore(current_concurrent)
            tasks: List[asyncio.Task] = []

            async def worker():
                async with semaphore:
                    if test_mode == "sms":
                        result = await simulator.execute_send_code()
                    else:
                        result = await simulator.execute_register()
                    stats.record(result)
                    step_stats.record(result)

            step_start = time.time()
            while time.time() - step_start < step_duration:
                task = asyncio.create_task(worker())
                tasks.append(task)
                # 控制任务生成速度
                await asyncio.sleep(0.1)
                print_live_stats(step_stats)

            # 等待当前步骤所有任务完成
            await asyncio.gather(*tasks, return_exceptions=True)
            step_stats.end_time = time.time()

            # 输出当前步骤结果
            print(f"\n  ➤ {current_concurrent}并发: "
                  f"成功={step_stats.success_count}, "
                  f"失败={step_stats.failure_count}, "
                  f"成功率={step_stats.success_rate:.1f}%, "
                  f"平均响应={step_stats.avg_response_time:.0f}ms, "
                  f"RPS={step_stats.rps:.1f}")

            # 检查是否需要停止(成功率低于10%或全部超时)
            if step_stats.success_rate < 10 and step_stats.total_requests > 5:
                print(f"\n🔴 成功率低于10%，网站已达到压力极限，停止加压")
                break

            if step_stats.error_counts.get("server_overloaded", 0) > step_stats.total_requests * 0.5:
                print(f"\n🔴 503错误超过50%，网站已过载，停止加压")
                break

            current_concurrent += step_size

    except KeyboardInterrupt:
        print("\n\n⚠ 收到中断信号，正在停止...")

    stats.end_time = time.time()
    print()
    return stats


async def main():
    """主入口"""
    args = parse_args()

    # 构建配置
    config = StressTestConfig(
        concurrent_users=args.concurrent,
        total_requests=args.total,
        ramp_up_seconds=args.ramp_up,
        request_interval_min=args.interval[0],
        request_interval_max=args.interval[1],
        read_timeout=args.timeout,
        proxy_file=args.proxy_file,
        proxy_api_url=args.proxy_api,
        proxy_pool_url=args.proxy_pool or "",
        proxy_pool_mode=args.proxy_pool_mode,
        proxy_rotate_strategy=args.proxy_strategy,
        proxy_refresh_interval=args.proxy_refresh,
        use_freeproxy=args.freeproxy,
        freeproxy_https_only=args.freeproxy_https_only,
        proxy_multi_source=args.multi_source,
        proxy_validate_target=not args.no_validate_target,
        proxy_validate_timeout=args.validate_timeout,
        proxy_validate_concurrency=args.validate_concurrent,
        proxy_validate_retry=args.validate_retry,
        random_delay=not args.no_delay,
        report_file=args.report,
    )

    if args.register_url:
        config.register_url = args.register_url
        config.auto_discover_api = False  # 手动指定URL时跳过自动探测

    if args.no_discover:
        config.auto_discover_api = False

    submit_mode = args.submit_mode

    # 如果禁用代理，清空代理配置
    use_proxy = not args.no_proxy

    # 设置日志
    setup_logging(config.log_file, args.verbose)

    # 打印Banner
    mode_name = "短信发送" if args.test_mode == "sms" else "注册功能"
    print("=" * 60)
    print(f"  快搜(kuaisou.com) {mode_name}压力测试工具")
    print("=" * 60)

    # 自动探测注册API接口
    if config.auto_discover_api:
        discoverer = APIDiscoverer(config)
        try:
            discovery_result = await discoverer.discover()
            if discovery_result.register_url:
                config.register_url = discovery_result.register_url
                print(f"\n  接口地址: {config.register_url}")
                if discovery_result.csrf_token_name:
                    print(f"  CSRF Token: {discovery_result.csrf_token_name}")
                if discovery_result.api_endpoints:
                    print(f"  发现API端点: {len(discovery_result.api_endpoints)} 个")
            # 如果探测到的是页面URL，自动切换为form模式
            if discovery_result.register_url == config.login_page_url:
                submit_mode = "form"
                print(f"  提交模式: 自动切换为 form (表单提交)")
        finally:
            await discoverer.close()
    else:
        if args.test_mode == "sms":
            print(f"\n  短信接口: {config.send_code_url}")
            print(f"  scene: {args.sms_scene}")
        else:
            print(f"\n  注册接口: {config.register_url}")
            print(f"  提交模式: {submit_mode}")

    print(f"\n  目标页面: {config.login_page_url}")
    print()

    # 初始化IP池
    ip_pool = IPPoolManager(config)
    if use_proxy:
        await ip_pool.initialize(skip_validate=args.no_validate)
    else:
        print("\n⚠ 未启用代理IP，将使用本机IP直接请求")
        print("  注意：不使用代理可能导致本机IP被目标网站封禁")

    # 创建统计对象
    stats = TestStats()

    try:
        if args.step_test:
            await run_step_load_test(
                config, ip_pool, stats,
                max_concurrent=args.max_concurrent,
                step_size=args.step_size,
                step_duration=args.step_duration,
                submit_mode=submit_mode,
                test_mode=args.test_mode,
                sms_scene=args.sms_scene,
            )
        else:
            await run_fixed_load_test(
                config, ip_pool, stats,
                submit_mode=submit_mode,
                test_mode=args.test_mode,
                sms_scene=args.sms_scene,
                burst=args.burst,
            )
    except Exception as e:
        logging.getLogger().error(f"测试执行出错: {e}", exc_info=True)
    finally:
        # 刷新IP池统计
        ip_pool_stats = ip_pool.get_pool_stats()

        # 打印最终报告
        print_final_report(stats, ip_pool_stats)

        # 保存JSON报告
        stats.save_report(config.report_file)

        # 关闭IP池HTTP Session
        await ip_pool.close()

    print(f"\n📄 详细报告已保存: {config.report_file}")
    print(f"📋 日志文件: {config.log_file}")


if __name__ == "__main__":
    asyncio.run(main())
