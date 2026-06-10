#!/usr/bin/env python3
"""Simple async load tester for gptload-rs proxy."""

import asyncio
import aiohttp
import time
import sys
import json
from dataclasses import dataclass, field
from collections import Counter

@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    status_codes: Counter = field(default_factory=Counter)
    latencies: list = field(default_factory=list)
    errors: Counter = field(default_factory=Counter)
    start_time: float = 0
    end_time: float = 0

    def record(self, status: int, latency: float, error: str = None):
        self.total += 1
        self.status_codes[status] += 1
        self.latencies.append(latency)
        if 200 <= status < 300:
            self.success += 1
        else:
            self.failed += 1
        if error:
            self.errors[error] += 1

    def report(self):
        elapsed = self.end_time - self.start_time
        latencies = sorted(self.latencies)
        if not latencies:
            print("No requests completed.")
            return

        def percentile(p):
            idx = int(len(latencies) * p / 100)
            return latencies[min(idx, len(latencies) - 1)]

        print(f"\n{'='*60}")
        print(f"  LOAD TEST RESULTS")
        print(f"{'='*60}")
        print(f"  Duration:       {elapsed:.2f}s")
        print(f"  Total requests: {self.total}")
        print(f"  Success:        {self.success}")
        print(f"  Failed:         {self.failed}")
        print(f"  RPS:            {self.total / elapsed:.1f}")
        print(f"  Success RPS:    {self.success / elapsed:.1f}")
        print(f"{'='*60}")
        print(f"  Latency (ms):")
        print(f"    Min:    {latencies[0]*1000:.0f}")
        print(f"    Avg:    {sum(latencies)/len(latencies)*1000:.0f}")
        print(f"    P50:    {percentile(50)*1000:.0f}")
        print(f"    P90:    {percentile(90)*1000:.0f}")
        print(f"    P95:    {percentile(95)*1000:.0f}")
        print(f"    P99:    {percentile(99)*1000:.0f}")
        print(f"    Max:    {latencies[-1]*1000:.0f}")
        print(f"{'='*60}")
        print(f"  Status codes:")
        for code, count in sorted(self.status_codes.items()):
            print(f"    {code}: {count}")
        if self.errors:
            print(f"  Errors:")
            for err, count in self.errors.most_common(10):
                print(f"    {err}: {count}")
        print(f"{'='*60}")


async def send_request(session, url, headers, body, stats):
    start = time.monotonic()
    try:
        async with session.post(url, headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            await resp.read()
            latency = time.monotonic() - start
            stats.record(resp.status, latency)
    except asyncio.TimeoutError:
        latency = time.monotonic() - start
        stats.record(0, latency, "timeout")
    except aiohttp.ClientError as e:
        latency = time.monotonic() - start
        stats.record(0, latency, type(e).__name__)
    except Exception as e:
        latency = time.monotonic() - start
        stats.record(0, latency, str(e)[:50])


async def worker(worker_id, session, url, headers, body, stats, stop_event, rps_limit=None):
    """Worker that sends requests continuously."""
    interval = 1.0 / rps_limit if rps_limit else 0
    while not stop_event.is_set():
        await send_request(session, url, headers, body, stats)
        if interval > 0:
            await asyncio.sleep(interval)


async def run_load_test(
    url="http://localhost:8080/v1/chat/completions",
    concurrency=50,
    duration=30,
    rps_limit=None,
    model="mimo-v2.5",
    api_key="test",
):
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }
    body = {
        "model": model,
        "messages": [{"role": "user", "content": "say hi in one word"}],
        "max_tokens": 5,
        "temperature": 0,
    }

    stats = Stats()
    stop_event = asyncio.Event()

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Warm up
        print(f"Warming up...")
        for _ in range(3):
            await send_request(session, url, headers, body, stats)
        stats.total = 0
        stats.success = 0
        stats.failed = 0
        stats.status_codes.clear()
        stats.latencies.clear()
        stats.errors.clear()

        print(f"Starting load test:")
        print(f"  URL:         {url}")
        print(f"  Model:       {model}")
        print(f"  Concurrency: {concurrency}")
        print(f"  Duration:    {duration}s")
        print(f"  RPS limit:   {rps_limit or 'unlimited'}")
        print()

        stats.start_time = time.monotonic()

        # Start workers
        tasks = []
        for i in range(concurrency):
            task = asyncio.create_task(
                worker(i, session, url, headers, body, stats, stop_event,
                       rps_limit / concurrency if rps_limit else None)
            )
            tasks.append(task)

        # Progress reporting
        async def progress():
            while not stop_event.is_set():
                await asyncio.sleep(5)
                elapsed = time.monotonic() - stats.start_time
                rps = stats.total / elapsed if elapsed > 0 else 0
                print(f"  [{elapsed:.0f}s] requests={stats.total} success={stats.success} failed={stats.failed} rps={rps:.0f}")

        progress_task = asyncio.create_task(progress())

        # Wait for duration
        await asyncio.sleep(duration)
        stop_event.set()

        # Wait for workers to finish
        await asyncio.gather(*tasks, return_exceptions=True)
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

        stats.end_time = time.monotonic()

    stats.report()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load tester for gptload-rs")
    parser.add_argument("--url", default="http://localhost:8080/v1/chat/completions")
    parser.add_argument("--concurrency", "-c", type=int, default=50)
    parser.add_argument("--duration", "-d", type=int, default=30)
    parser.add_argument("--rps", type=int, default=None, help="Rate limit (requests per second)")
    parser.add_argument("--model", default="mimo-v2.5")
    parser.add_argument("--api-key", default="test")
    args = parser.parse_args()

    asyncio.run(run_load_test(
        url=args.url,
        concurrency=args.concurrency,
        duration=args.duration,
        rps_limit=args.rps,
        model=args.model,
        api_key=args.api_key,
    ))
