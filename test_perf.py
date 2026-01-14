#!/usr/bin/env python3
"""
Performance test for gptload-rs proxy with concurrent requests.
Tests gpt-4o-mini model availability and throughput.
"""

import concurrent.futures
import requests
import time
import statistics
from typing import List, Tuple
import json
from datetime import datetime
import os

# Configuration
PROXY_URL = "http://localhost:8080"
CONCURRENCY = 100  # Number of concurrent requests
TOTAL_REQUESTS = 200  # Total number of requests to send
MODEL = "gpt-4o-mini"

# Test payload
PAYLOAD = {
    "model": MODEL,
    "messages": [
        {"role": "user", "content": "Say 'OK' in one word"}
    ],
    "max_tokens": 5
}

HEADERS = {
    "Content-Type": "application/json"
}

# Response save directory for unexpected status codes
RESPONSE_DIR = "unexpected_responses"

def save_unexpected_response(request_id: int, response: requests.Response):
    """Save unexpected response to a file"""
    os.makedirs(RESPONSE_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{RESPONSE_DIR}/req_{request_id}_{response.status_code}_{timestamp}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"Request ID: {request_id}\n")
        f.write(f"Status Code: {response.status_code}\n")
        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
        f.write(f"URL: {response.url}\n")
        f.write(f"\n{'='*60}\n")
        f.write(f"Response Headers:\n")
        f.write(f"{'='*60}\n")
        for key, value in response.headers.items():
            f.write(f"{key}: {value}\n")
        f.write(f"\n{'='*60}\n")
        f.write(f"Response Body:\n")
        f.write(f"{'='*60}\n")
        f.write(response.text)
    
    print(f"⚠️  Saved unexpected response (status {response.status_code}) to {filename}")
    return filename

def make_request(request_id: int) -> Tuple[int, float, int, str]:
    """Make a single request to the proxy and return (id, duration, status_code, success)"""
    start_time = time.time()
    try:
        response = requests.post(
            f"{PROXY_URL}/v1/chat/completions",
            json=PAYLOAD,
            headers=HEADERS,
            timeout=30
        )
        duration = time.time() - start_time
        
        # Save response if status code is not 200 or 429
        if response.status_code not in [200, 429]:
            save_unexpected_response(request_id, response)
        
        success = response.status_code == 200
        return request_id, duration, response.status_code, "OK" if success else f"HTTP {response.status_code}"
    except Exception as e:
        duration = time.time() - start_time
        return request_id, duration, 0, f"ERROR: {str(e)}"

def run_load_test():
    """Run the load test with concurrent requests"""
    print(f"🚀 Starting performance test for gptload-rs")
    print(f"   Proxy URL: {PROXY_URL}")
    print(f"   Model: {MODEL}")
    print(f"   Total requests: {TOTAL_REQUESTS}")
    print(f"   Concurrency level: {CONCURRENCY}")
    print(f"   Payload: {json.dumps(PAYLOAD)}\n")
    
    results: List[Tuple[int, float, int, str]] = []
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(make_request, i) for i in range(TOTAL_REQUESTS)]
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            if completed % 10 == 0:
                print(f"   Progress: {completed}/{TOTAL_REQUESTS} requests completed")
    
    total_duration = time.time() - start_time
    
    # Calculate statistics
    durations = [r[1] for r in results]
    status_codes = [r[2] for r in results]
    
    successful = sum(1 for code in status_codes if code == 200)
    failed = TOTAL_REQUESTS - successful
    
    print(f"\n" + "="*60)
    print(f"📊 Performance Test Results")
    print(f"="*60)
    print(f"Total Time: {total_duration:.2f}s")
    print(f"Total Requests: {TOTAL_REQUESTS}")
    print(f"Successful Requests: {successful} ({successful*100/TOTAL_REQUESTS:.1f}%)")
    print(f"Failed Requests: {failed}")
    print(f"Throughput: {TOTAL_REQUESTS/total_duration:.2f} req/s")
    
    print(f"\n⏱️  Response Time Statistics:")
    print(f"   Min: {min(durations):.3f}s")
    print(f"   Max: {max(durations):.3f}s")
    print(f"   Mean: {statistics.mean(durations):.3f}s")
    print(f"   Median: {statistics.median(durations):.3f}s")
    if len(durations) > 1:
        print(f"   Stdev: {statistics.stdev(durations):.3f}s")
    print(f"   P95: {sorted(durations)[int(len(durations)*0.95)]:.3f}s")
    print(f"   P99: {sorted(durations)[int(len(durations)*0.99)]:.3f}s")
    
    print(f"\n📈 Status Code Distribution:")
    status_dist = {}
    for code in status_codes:
        status_dist[code] = status_dist.get(code, 0) + 1
    for code in sorted(status_dist.keys()):
        count = status_dist[code]
        pct = count * 100 / TOTAL_REQUESTS
        print(f"   HTTP {code}: {count} ({pct:.1f}%)")
    
    print(f"\n🔍 Sample Responses:")
    for i, (req_id, duration, status_code, msg) in enumerate(results[:5]):
        print(f"   Request {req_id}: {status_code} - {msg} ({duration:.3f}s)")
    
    print(f"\n✅ Test completed!" if failed == 0 else f"\n⚠️  Test completed with {failed} failures")
    print("="*60)

if __name__ == "__main__":
    try:
        run_load_test()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
