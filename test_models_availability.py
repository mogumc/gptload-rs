#!/usr/bin/env python3
"""
Test availability of all models defined in models_routes.json
"""

import requests
import json
import time
from typing import List, Dict, Tuple
from datetime import datetime
import concurrent.futures

# Configuration
PROXY_URL = "http://localhost:8080"
MODELS_FILE = "data/models_routes.json"

# Test payload
def create_payload(model: str) -> Dict:
    return {
        "model": model,
        "messages": [
            {"role": "user", "content": "Say 'OK' in one word"}
        ],
        "max_completion_tokens": 5
    }

HEADERS = {
    "Content-Type": "application/json"
}

def test_model(model: str, timeout: int = 10, max_retries: int = 10) -> Tuple[str, bool, int, str]:
    """Test a single model availability with retry on 429"""
    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{PROXY_URL}/v1/chat/completions",
                json=create_payload(model),
                headers=HEADERS,
                timeout=timeout
            )
            
            # If we get 429, retry
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    continue
                else:
                    # Max retries reached, return the 429 response
                    return model, False, response.status_code, ""
            
            success = response.status_code == 200
            return model, success, response.status_code, ""
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return model, False, 0, "TIMEOUT"
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return model, False, 0, "CONNECTION_ERROR"
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return model, False, 0, str(e)
    
    return model, False, 0, "MAX_RETRIES_EXCEEDED"

def load_models() -> List[str]:
    """Load all models from models_routes.json"""
    try:
        with open(MODELS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return list(data.get("models", {}).keys())
    except Exception as e:
        print(f"❌ Failed to load models: {e}")
        return []

def main():
    print("="*70)
    print("🔍 Model Availability Test")
    print("="*70)
    
    # Load models
    models = load_models()
    if not models:
        print("No models found in models_routes.json")
        return
    
    print(f"📋 Testing {len(models)} models...\n")
    
    available_models = []
    unavailable_models = []
    error_models = []
    
    start_time = time.time()
    
    # Test models concurrently with a limit
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(test_model, model): model for model in models}
        
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            model, success, status_code, error_msg = future.result()
            completed += 1
            
            if success:
                available_models.append(model)
                status = "✅"
            elif status_code == 0 and error_msg:
                error_models.append((model, error_msg))
                status = "⚠️ "
            else:
                unavailable_models.append((model, status_code))
                status = "❌"
            
            if completed % 20 == 0:
                print(f"   Progress: {completed}/{len(models)} models tested")
    
    total_duration = time.time() - start_time
    
    # Print results
    print(f"\n" + "="*70)
    print(f"📊 Test Results (took {total_duration:.2f}s)")
    print("="*70)
    
    print(f"\n✅ Available Models ({len(available_models)}):")
    if available_models:
        for model in sorted(available_models):
            print(f"   • {model}")
    else:
        print("   (none)")
    
    print(f"\n❌ Unavailable Models ({len(unavailable_models)}):")
    if unavailable_models:
        for model, status_code in sorted(unavailable_models, key=lambda x: x[0]):
            print(f"   • {model} (HTTP {status_code})")
    else:
        print("   (none)")
    
    print(f"\n⚠️  Error Models ({len(error_models)}):")
    if error_models:
        for model, error in sorted(error_models, key=lambda x: x[0]):
            print(f"   • {model} ({error})")
    else:
        print("   (none)")
    
    print(f"\n" + "="*70)
    print(f"📈 Summary:")
    print(f"   Total: {len(models)}")
    print(f"   Available: {len(available_models)} ({len(available_models)*100/len(models):.1f}%)")
    print(f"   Unavailable: {len(unavailable_models)} ({len(unavailable_models)*100/len(models):.1f}%)")
    print(f"   Errors: {len(error_models)} ({len(error_models)*100/len(models):.1f}%)")
    print("="*70)
    
    # Save available models to file
    output_file = f"available_models_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "available_models": sorted(available_models),
            "count": len(available_models)
        }, f, indent=2)
    print(f"\n💾 Available models saved to: {output_file}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
