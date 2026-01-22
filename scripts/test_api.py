"""
Test script for FastAPI endpoints
"""

import requests
import json
from pathlib import Path

BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint: str, params: dict = None):
    """Test an API endpoint"""
    try:
        url = f"{BASE_URL}{endpoint}"
        print(f"\n🔍 Testing: {endpoint}")
        print(f"   URL: {url}")
        
        if params:
            print(f"   Params: {params}")
            response = requests.get(url, params=params)
        else:
            response = requests.get(url)
        
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   Response length: {len(str(data))} characters")
            
            # Save sample response
            if endpoint.startswith('/api/'):
                endpoint_name = endpoint.replace('/api/', '').replace('/', '_')
                output_file = Path(f"api_responses/{endpoint_name}.json")
                output_file.parent.mkdir(exist_ok=True)
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"   Saved to: {output_file}")
            
            return True
        else:
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"   Exception: {e}")
        return False

def main():
    """Test all API endpoints"""
    print("=" * 60)
    print("FASTAPI ENDPOINT TESTING")
    print("=" * 60)
    
    # Test endpoints
    tests = [
        # Root and health
        ("/", None),
        ("/health", None),
        
        # Analytics endpoints
        ("/api/reports/top-products", {"limit": 5, "days": 30}),
        ("/api/reports/top-products", {"limit": 10, "days": 7, "channel_name": "lobelia4cosmetics"}),
        
        # Channel endpoints
        ("/api/channels", None),
        ("/api/channels/lobelia4cosmetics/activity", {"period": "weekly", "days": 30}),
        
        # Search endpoint
        ("/api/search/messages", {"query": "paracetamol", "limit": 5}),
        ("/api/search/messages", {"query": "vitamin", "channel_name": "tikvahpharma", "limit": 3}),
        
        # Visual content analysis
        ("/api/reports/visual-content", {"days": 30}),
        ("/api/reports/visual-content", {"days": 7}),
    ]
    
    results = []
    for endpoint, params in tests:
        success = test_endpoint(endpoint, params)
        results.append((endpoint, success))
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    print(f"\nTotal tests: {total_count}")
    print(f"Successful: {success_count}")
    print(f"Failed: {total_count - success_count}")
    
    print("\nDetailed Results:")
    for endpoint, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} {endpoint}")

if __name__ == "__main__":
    # Check if API is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            main()
        else:
            print("❌ API is not running. Start it with: uvicorn api.main:app --reload")
    except requests.ConnectionError:
        print("❌ Cannot connect to API. Make sure it's running on port 8000")
        print("   Start with: uvicorn api.main:app --reload")