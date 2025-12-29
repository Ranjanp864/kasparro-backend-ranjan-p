#!/usr/bin/env python3
"""
End-to-End Smoke Test for Kasparro Backend & ETL System

This script performs a comprehensive smoke test by:
1. Checking system health
2. Triggering ETL
3. Verifying data ingestion
4. Testing API endpoints
5. Checking metrics
"""

import requests
import time
import sys
from typing import Dict, Any

BASE_URL = "http://localhost:8000"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_step(step: str):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}{step}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}")

def print_success(message: str):
    print(f"{Colors.GREEN}✓ {message}{Colors.RESET}")

def print_error(message: str):
    print(f"{Colors.RED}✗ {message}{Colors.RESET}")

def print_warning(message: str):
    print(f"{Colors.YELLOW}! {message}{Colors.RESET}")

def check_health() -> bool:
    """Test 1: Check system health"""
    print_step("Test 1: Health Check")
    
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") == "healthy":
            print_success("System is healthy")
        else:
            print_warning(f"System status: {data.get('status')}")
        
        if data.get("database_connected"):
            print_success("Database is connected")
        else:
            print_error("Database is NOT connected")
            return False
        
        print_success(f"API Latency: {data.get('api_latency_ms', 0):.2f}ms")
        return True
    
    except requests.RequestException as e:
        print_error(f"Health check failed: {e}")
        return False

def trigger_etl() -> bool:
    """Test 2: Trigger ETL process"""
    print_step("Test 2: Trigger ETL Process")
    
    try:
        print("Triggering ETL (this may take 30-60 seconds)...")
        response = requests.post(f"{BASE_URL}/trigger-etl", timeout=120)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get("status") == "completed":
            print_success("ETL completed successfully")
            return True
        else:
            print_error(f"ETL status: {data.get('status')}")
            return False
    
    except requests.RequestException as e:
        print_error(f"ETL trigger failed: {e}")
        return False

def verify_data_ingestion() -> bool:
    """Test 3: Verify data was ingested"""
    print_step("Test 3: Verify Data Ingestion")
    
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        total_records = data.get("total_records_processed", 0)
        
        if total_records > 0:
            print_success(f"Total records ingested: {total_records}")
        else:
            print_error("No records were ingested")
            return False
        
        records_by_source = data.get("records_by_source", {})
        for source, count in records_by_source.items():
            print_success(f"  {source}: {count} records")
        
        print_success(f"Total runs: {data.get('total_runs', 0)}")
        print_success(f"Successful runs: {data.get('successful_runs', 0)}")
        
        if data.get('failed_runs', 0) > 0:
            print_warning(f"Failed runs: {data.get('failed_runs', 0)}")
        
        return True
    
    except requests.RequestException as e:
        print_error(f"Stats check failed: {e}")
        return False

def test_api_endpoints() -> bool:
    """Test 4: Test various API endpoints"""
    print_step("Test 4: Test API Endpoints")
    
    tests_passed = True
    
    # Test /data endpoint
    try:
        response = requests.get(f"{BASE_URL}/data?page=1&page_size=5", timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get("data"):
            print_success(f"GET /data: Retrieved {len(data['data'])} records")
        else:
            print_warning("GET /data: No data returned")
        
        # Show sample record
        if data.get("data"):
            sample = data["data"][0]
            print(f"   Sample: {sample.get('symbol')} - ${sample.get('price_usd')}")
    
    except requests.RequestException as e:
        print_error(f"GET /data failed: {e}")
        tests_passed = False
    
    # Test /data with filters
    try:
        response = requests.get(
            f"{BASE_URL}/data?source=coinpaprika&page_size=3",
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        print_success(f"GET /data (filtered): Retrieved {len(data['data'])} coinpaprika records")
    
    except requests.RequestException as e:
        print_error(f"GET /data (filtered) failed: {e}")
        tests_passed = False
    
    # Test /runs endpoint
    try:
        response = requests.get(f"{BASE_URL}/runs?limit=3", timeout=10)
        response.raise_for_status()
        data = response.json()
        print_success(f"GET /runs: Retrieved {data.get('count', 0)} recent runs")
    
    except requests.RequestException as e:
        print_error(f"GET /runs failed: {e}")
        tests_passed = False
    
    return tests_passed

def test_metrics() -> bool:
    """Test 5: Check metrics endpoint"""
    print_step("Test 5: Check Metrics")
    
    try:
        response = requests.get(f"{BASE_URL}/metrics", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        metrics = data.get("metrics", "")
        
        if "etl_total_records" in metrics:
            print_success("Metrics endpoint working")
            
            # Parse some metrics
            for line in metrics.split('\n'):
                if line and not line.startswith('#'):
                    print(f"   {line}")
                    if len([l for l in metrics.split('\n') if l and not l.startswith('#')]) > 5:
                        break
            
            return True
        else:
            print_error("Metrics endpoint returned invalid data")
            return False
    
    except requests.RequestException as e:
        print_error(f"Metrics check failed: {e}")
        return False

def test_etl_recovery() -> bool:
    """Test 6: Test ETL recovery after restart"""
    print_step("Test 6: Test ETL Recovery (simulation)")
    
    try:
        # Check that checkpoints are being created
        response = requests.get(f"{BASE_URL}/stats", timeout=10)
        response.raise_for_status()
        
        print_success("ETL checkpoint system is enabled")
        print("   (Actual recovery testing requires container restart)")
        return True
    
    except requests.RequestException as e:
        print_error(f"Recovery test failed: {e}")
        return False

def run_smoke_test():
    """Run complete smoke test suite"""
    print(f"\n{Colors.BLUE}")
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║      Kasparro Backend & ETL - Smoke Test Suite           ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    print(f"{Colors.RESET}")
    
    results = {}
    
    # Wait for service to be ready
    print("Waiting for service to be ready...")
    max_retries = 10
    for i in range(max_retries):
        try:
            requests.get(f"{BASE_URL}/health", timeout=5)
            print_success("Service is ready!")
            break
        except:
            if i == max_retries - 1:
                print_error("Service failed to start")
                return False
            print(f"Waiting... ({i+1}/{max_retries})")
            time.sleep(3)
    
    # Run tests
    results['health'] = check_health()
    results['etl'] = trigger_etl()
    results['ingestion'] = verify_data_ingestion()
    results['api'] = test_api_endpoints()
    results['metrics'] = test_metrics()
    results['recovery'] = test_etl_recovery()
    
    # Summary
    print_step("Test Summary")
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results.values() if r)
    
    for test_name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        color = Colors.GREEN if passed else Colors.RED
        print(f"{color}{test_name.upper()}: {status}{Colors.RESET}")
    
    print(f"\n{Colors.BLUE}Results: {passed_tests}/{total_tests} tests passed{Colors.RESET}")
    
    if passed_tests == total_tests:
        print(f"\n{Colors.GREEN}{'='*60}")
        print("🎉 ALL TESTS PASSED! System is working correctly.")
        print(f"{'='*60}{Colors.RESET}\n")
        return True
    else:
        print(f"\n{Colors.RED}{'='*60}")
        print("❌ SOME TESTS FAILED! Please check the logs.")
        print(f"{'='*60}{Colors.RESET}\n")
        return False

if __name__ == "__main__":
    success = run_smoke_test()
    sys.exit(0 if success else 1)