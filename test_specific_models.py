#!/usr/bin/env python3
"""
Test specific models and output detailed response information
"""

import requests
import json
import time
from typing import Dict

# Configuration
PROXY_URL = "http://localhost:8080"

# Models to test
MODELS_TO_TEST = [
    "gpt-5-nano-2025-08-07",
    "gpt-5-nano"
]

def create_payload(model: str) -> Dict:
    return {
        "model": model,
        "messages": [
            {"role": "user", "content": "Say '我喜欢你' in one word"}
        ],
        "max_completion_tokens": 4096
    }

HEADERS = {
    "Content-Type": "application/json"
}

def test_model_detailed(model: str, timeout: int = 100, max_retries: int = 10):
    """Test a specific model and output detailed response"""
    print(f"\n{'='*70}")
    print(f"Testing: {model}")
    print(f"{'='*70}")
    
    for attempt in range(max_retries):
        try:
            print(f"\n📤 Attempt {attempt + 1}/{max_retries}")
            start_time = time.time()
            
            response = requests.post(
                f"{PROXY_URL}/v1/chat/completions",
                json=create_payload(model),
                headers=HEADERS,
                timeout=timeout
            )
            
            duration = time.time() - start_time
            
            print(f"⏱️  Response Time: {duration:.3f}s")
            print(f"📊 Status Code: {response.status_code}")
            print(f"📋 Response Headers:")
            for key, value in response.headers.items():
                print(f"   {key}: {value}")
            
            print(f"\n📄 Response Body:")
            print("-" * 70)
            try:
                # Try to parse as JSON
                response_json = response.json()
                print(json.dumps(response_json, indent=2, ensure_ascii=False))
            except:
                # If not JSON, print as text
                print(response.text)
            print("-" * 70)
            
            # If we get 429, retry
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    print(f"\n⏳ Got 429, waiting 1 second before retry...")
                    time.sleep(1)
                    continue
                else:
                    print(f"\n❌ Max retries reached with 429 responses")
                    return
            
            # Success or other status code
            if response.status_code == 200:
                print(f"\n✅ Success!")
            else:
                print(f"\n⚠️  Non-200 status code received")
            return
            
        except requests.exceptions.Timeout:
            print(f"❌ TIMEOUT")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting 1 second before retry...")
                time.sleep(1)
                continue
            else:
                print(f"❌ Max retries reached with timeouts")
                return
        except requests.exceptions.ConnectionError as e:
            print(f"❌ CONNECTION_ERROR: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting 1 second before retry...")
                time.sleep(1)
                continue
            else:
                print(f"❌ Max retries reached with connection errors")
                return
        except Exception as e:
            print(f"❌ ERROR: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Waiting 1 second before retry...")
                time.sleep(1)
                continue
            else:
                print(f"❌ Max retries reached with errors")
                return

def main():
    print("="*70)
    print("🔍 Specific Model Availability Test")
    print("="*70)
    print(f"\n📋 Models to test:")
    for model in MODELS_TO_TEST:
        print(f"   • {model}")
    
    print(f"\n🌐 Proxy URL: {PROXY_URL}\n")
    
    for model in MODELS_TO_TEST:
        test_model_detailed(model)
    
    print(f"\n{'='*70}")
    print("✅ Test completed!")
    print("="*70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
