"""
统计与报告模块
收集测试结果，生成详细报告
"""
import json
import time
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import Counter, defaultdict

from register import RegisterResult

logger = logging.getLogger("stats")


@dataclass
class TestStats:
    """测试统计信息"""
    start_time: float = 0.0
    end_time: float = 0.0
    total_requests: int = 0
    success_count: int = 0
    failure_count: int = 0
    results: List[RegisterResult] = field(default_factory=list)

    # 响应时间统计(毫秒)
    response_times: List[float] = field(default_factory=list)

    # 错误分类统计
    error_counts: Counter = field(default_factory=Counter)

    # 状态码统计
    status_counts: Counter = field(default_factory=Counter)

    # 每秒请求数(RPS)跟踪
    rps_history: List[Dict[str, Any]] = field(default_factory=list)

    def record(self, result: RegisterResult):
        """记录一个请求结果"""
        self.results.append(result)
        self.total_requests += 1

        if result.success:
            self.success_count += 1
        else:
            self.failure_count += 1
            if result.error_type:
                self.error_counts[result.error_type] += 1

        if result.status_code:
            self.status_counts[result.status_code] += 1

        if result.response_time_ms > 0:
            self.response_times.append(result.response_time_ms)

    @property
    def duration_seconds(self) -> float:
        """测试持续时间"""
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_requests == 0:
            return 0.0
        return self.success_count / self.total_requests * 100

    @property
    def avg_response_time(self) -> float:
        """平均响应时间"""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    @property
    def rps(self) -> float:
        """每秒请求数"""
        if self.duration_seconds == 0:
            return 0.0
        return self.total_requests / self.duration_seconds

    def get_percentile(self, p: float) -> float:
        """获取响应时间百分位"""
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * p / 100)
        idx = min(idx, len(sorted_times) - 1)
        return sorted_times[idx]

    def generate_report(self) -> Dict[str, Any]:
        """生成完整测试报告"""
        # 响应时间分段统计
        rt_buckets = defaultdict(int)
        for rt in self.response_times:
            if rt < 100:
                rt_buckets["0-100ms"] += 1
            elif rt < 200:
                rt_buckets["100-200ms"] += 1
            elif rt < 500:
                rt_buckets["200-500ms"] += 1
            elif rt < 1000:
                rt_buckets["500-1000ms"] += 1
            elif rt < 2000:
                rt_buckets["1000-2000ms"] += 1
            elif rt < 5000:
                rt_buckets["2000-5000ms"] += 1
            else:
                rt_buckets["5000ms+"] += 1

        report = {
            "summary": {
                "total_requests": self.total_requests,
                "success_count": self.success_count,
                "failure_count": self.failure_count,
                "success_rate": f"{self.success_rate:.2f}%",
                "duration_seconds": f"{self.duration_seconds:.2f}",
                "requests_per_second": f"{self.rps:.2f}",
            },
            "response_time": {
                "min_ms": f"{min(self.response_times):.2f}" if self.response_times else "N/A",
                "max_ms": f"{max(self.response_times):.2f}" if self.response_times else "N/A",
                "avg_ms": f"{self.avg_response_time:.2f}",
                "p50_ms": f"{self.get_percentile(50):.2f}",
                "p90_ms": f"{self.get_percentile(90):.2f}",
                "p95_ms": f"{self.get_percentile(95):.2f}",
                "p99_ms": f"{self.get_percentile(99):.2f}",
                "distribution": dict(rt_buckets),
            },
            "status_codes": dict(self.status_counts),
            "error_breakdown": dict(self.error_counts),
            "rps_over_time": self.rps_history,
        }
        return report

    def save_report(self, filepath: str):
        """保存报告到JSON文件"""
        report = self.generate_report()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"测试报告已保存至: {filepath}")
        return report


def print_live_stats(stats: TestStats):
    """打印实时统计信息(单行覆盖)"""
    if stats.total_requests == 0:
        return

    avg_rt = stats.avg_response_time
    rps = stats.rps
    rate = stats.success_rate

    line = (
        f"\r  请求: {stats.total_requests} | "
        f"成功: {stats.success_count} | "
        f"失败: {stats.failure_count} | "
        f"成功率: {rate:.1f}% | "
        f"平均响应: {avg_rt:.0f}ms | "
        f"RPS: {rps:.1f}"
    )
    print(line, end="", flush=True)


def print_final_report(stats: TestStats, ip_pool_stats: Dict):
    """打印最终测试报告"""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout

    console = Console()

    # 总览面板
    summary = Table(title="📊 压力测试报告", show_header=False, border_style="blue")
    summary.add_column("指标", style="cyan", width=20)
    summary.add_column("值", style="green", width=25)

    summary.add_row("总请求数", str(stats.total_requests))
    summary.add_row("成功次数", f"[green]{stats.success_count}[/green]")
    summary.add_row("失败次数", f"[red]{stats.failure_count}[/red]")
    summary.add_row("成功率", f"{stats.success_rate:.2f}%")
    summary.add_row("测试时长", f"{stats.duration_seconds:.2f} 秒")
    summary.add_row("每秒请求数(RPS)", f"{stats.rps:.2f}")
    console.print(summary)

    # 响应时间表
    rt_table = Table(title="⏱ 响应时间统计", show_header=True, border_style="yellow")
    rt_table.add_column("指标", style="cyan")
    rt_table.add_column("值", style="green")

    if stats.response_times:
        rt_table.add_row("最小值", f"{min(stats.response_times):.2f} ms")
        rt_table.add_row("最大值", f"{max(stats.response_times):.2f} ms")
        rt_table.add_row("平均值", f"{stats.avg_response_time:.2f} ms")
        rt_table.add_row("P50", f"{stats.get_percentile(50):.2f} ms")
        rt_table.add_row("P90", f"{stats.get_percentile(90):.2f} ms")
        rt_table.add_row("P95", f"{stats.get_percentile(95):.2f} ms")
        rt_table.add_row("P99", f"{stats.get_percentile(99):.2f} ms")
    else:
        rt_table.add_row("-", "无数据")
    console.print(rt_table)

    # 响应时间分布
    dist_table = Table(title="📈 响应时间分布", show_header=True, border_style="magenta")
    dist_table.add_column("时间区间", style="cyan")
    dist_table.add_column("请求数", style="green", justify="right")
    dist_table.add_column("占比", style="yellow", justify="right")

    report = stats.generate_report()
    dist = report["response_time"]["distribution"]
    total_rt = sum(dist.values()) if dist else 1
    for bucket in ["0-100ms", "100-200ms", "200-500ms", "500-1000ms", "1000-2000ms", "2000-5000ms", "5000ms+"]:
        count = dist.get(bucket, 0)
        pct = count / total_rt * 100 if total_rt else 0
        bar = "█" * int(pct / 2)
        dist_table.add_row(bucket, str(count), f"{pct:.1f}% {bar}")
    console.print(dist_table)

    # 状态码统计
    if stats.status_counts:
        sc_table = Table(title="🔢 HTTP状态码统计", show_header=True, border_style="cyan")
        sc_table.add_column("状态码", style="cyan")
        sc_table.add_column("次数", style="green", justify="right")
        sc_table.add_column("占比", style="yellow", justify="right")
        for code in sorted(stats.status_counts.keys()):
            count = stats.status_counts[code]
            pct = count / stats.total_requests * 100
            color = "green" if code < 400 else "red"
            sc_table.add_row(f"[{color}]{code}[/{color}]", str(count), f"{pct:.1f}%")
        console.print(sc_table)

    # 错误分类
    if stats.error_counts:
        err_table = Table(title="❌ 错误分类", show_header=True, border_style="red")
        err_table.add_column("错误类型", style="cyan")
        err_table.add_column("次数", style="red", justify="right")
        err_table.add_column("占比", style="yellow", justify="right")
        for err_type, count in stats.error_counts.most_common():
            pct = count / stats.failure_count * 100 if stats.failure_count else 0
            err_table.add_row(err_type, str(count), f"{pct:.1f}%")
        console.print(err_table)

    # IP池状态
    if ip_pool_stats:
        ip_table = Table(title="🌐 IP池状态", show_header=True, border_style="green")
        ip_table.add_column("指标", style="cyan")
        ip_table.add_column("值", style="green")
        ip_table.add_row("总代理数", str(ip_pool_stats.get("total", 0)))
        ip_table.add_row("可用代理", str(ip_pool_stats.get("alive", 0)))
        ip_table.add_row("失效代理", str(ip_pool_stats.get("dead", 0)))
        ip_table.add_row("平均响应时间", f"{ip_pool_stats.get('avg_response_time', 0):.0f} ms")
        console.print(ip_table)

    # 结论与建议
    console.print()
    conclusion = []
    if stats.success_rate >= 95:
        conclusion.append("[green]✓[/green] 网站在当前压力下表现良好，成功率超过95%")
    elif stats.success_rate >= 80:
        conclusion.append("[yellow]⚠[/yellow] 网站在当前压力下出现一定压力，成功率在80%-95%之间")
    elif stats.success_rate >= 50:
        conclusion.append("[yellow]⚠[/yellow] 网站在当前压力下出现明显压力，成功率在50%-80%之间")
    else:
        conclusion.append("[red]✗[/red] 网站在当前压力下已接近或超过承载极限，成功率低于50%")

    if stats.get_percentile(95) > 3000:
        conclusion.append("[yellow]⚠[/yellow] P95响应时间超过3秒，用户体验较差")
    if stats.error_counts.get("timeout", 0) > stats.total_requests * 0.1:
        conclusion.append("[red]✗[/red] 超时请求超过10%，服务器处理能力可能不足")
    if stats.error_counts.get("server_overloaded", 0) > 0:
        conclusion.append("[red]✗[/red] 出现503错误，服务器已过载")
    if stats.error_counts.get("rate_limit", 0) > 0:
        conclusion.append("[yellow]⚠[/yellow] 触发频率限制，建议检查限流策略配置")
    if stats.error_counts.get("blocked", 0) > 0:
        conclusion.append("[yellow]⚠[/yellow] IP被封禁，反爬虫机制已生效")

    console.print(Panel("\n".join(conclusion), title="📋 结论与建议", border_style="blue"))
