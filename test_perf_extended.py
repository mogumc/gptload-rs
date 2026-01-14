#!/usr/bin/env python3
"""
Extended performance test with detailed key availability monitoring
"""

import concurrent.futures
import requests
import time
import json
from collections import defaultdict

PROXY_URL = "http://localhost:8080"
CONCURRENCY = 50
TOTAL_REQUESTS = 2000
MODEL = "gpt-4o-mini"

PAYLOAD = {
    "model": MODEL,
    "messages": [{"role": "user", "content": "Say 'OK' for 100 times please"}],
    "max_tokens": 4096
}

HEADERS = {"Content-Type": "application/json"}

def make_request(request_id: int) -> dict:
    """Make a request and track detailed metrics"""
    start_time = time.time()
    try:
        response = requests.post(
            f"{PROXY_URL}/v1/chat/completions",
            json=PAYLOAD,
            headers=HEADERS,
            timeout=30
        )
        duration = time.time() - start_time
        return {
            "id": request_id,
            "duration": duration,
            "status": response.status_code,
            "success": response.status_code == 200
        }
    except Exception as e:
        duration = time.time() - start_time
        return {
            "id": request_id,
            "duration": duration,
            "status": 0,
            "success": False,
            "error": str(e)
        }

def get_upstream_stats():
    """Get current upstream key stats"""
    try:
        response = requests.get(
            f"{PROXY_URL}/admin/api/v1/upstreams",
            headers={"X-Admin-Token": "admin-token-1"},
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

print(f"🚀 Extended Performance Test")
print(f"   Concurrency: {CONCURRENCY}, Requests: {TOTAL_REQUESTS}\n")

# Get initial state
initial_stats = get_upstream_stats()
if initial_stats:
    print(f"📊 Initial Key Status:")
    for upstream in initial_stats:
        print(f"   {upstream['id']}: {upstream['keys_total']} keys")
    print()

results = []
start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
    futures = [executor.submit(make_request, i) for i in range(TOTAL_REQUESTS)]
    completed = 0
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        results.append(result)
        completed += 1
        if completed % 50 == 0:
            print(f"   Progress: {completed}/{TOTAL_REQUESTS}")

total_duration = time.time() - start_time

# Get final state
final_stats = get_upstream_stats()

# Analyze results
status_codes = defaultdict(int)
for r in results:
    status_codes[r['status']] += 1

print(f"\n{'='*60}")
print(f"✅ Test Completed")
print(f"{'='*60}")
print(f"\n⏱️  Performance:")
print(f"   Total Time: {total_duration:.2f}s")
print(f"   Throughput: {TOTAL_REQUESTS/total_duration:.2f} req/s")

print(f"\n📈 Status Code Distribution:")
for code in sorted(status_codes.keys()):
    count = status_codes[code]
    pct = count * 100 / TOTAL_REQUESTS
    status_name = {
        200: "OK",
        429: "Rate Limited",
        503: "No Keys Available",
        401: "Unauthorized"
    }.get(code, f"HTTP {code}")
    print(f"   {status_name} ({code}): {count} ({pct:.1f}%)")

print(f"\n📊 Key Availability Analysis:")
if final_stats:
    total_keys = sum(u['keys_total'] for u in final_stats)
    print(f"   Total Keys: {total_keys:,}")
    for upstream in final_stats:
        print(f"   {upstream['id']}: {upstream['keys_total']:,} keys available")
        print(f"      - Selected: {upstream['selected_total']}")
        print(f"      - Responses 2xx: {upstream['responses_2xx']}")
        print(f"      - Responses 4xx: {upstream['responses_4xx']}")
        print(f"      - Responses 5xx: {upstream['responses_5xx']}")

success_count = sum(1 for r in results if r['success'])
failure_count = TOTAL_REQUESTS - success_count
print(f"\n{'='*60}")
if failure_count == 0:
    print(f"✅ SUCCESS: All {TOTAL_REQUESTS} requests succeeded!")
else:
    print(f"⚠️  {success_count} successful, {failure_count} failed ({failure_count*100/TOTAL_REQUESTS:.1f}%)")
    if status_codes.get(503, 0) > 0:
        print(f"\n⚠️  Note: {status_codes[503]} requests got HTTP 503 (no keys available)")
        print(f"    This suggests key cooldown synchronization issue")
print(f"{'='*60}")
