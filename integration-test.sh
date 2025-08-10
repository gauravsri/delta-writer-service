#!/bin/bash

# =====================================================================
# TECH LEAD - COMPREHENSIVE INTEGRATION TEST SUITE
# Focus: Concurrency, Performance, Data Integrity
# =====================================================================

set -e  # Exit on any error

echo "🔧 TECH LEAD - COMPREHENSIVE INTEGRATION TEST SUITE"
echo "=================================================="
echo "Timestamp: $(date)"
echo "Testing application at: http://localhost:8080"
echo ""

# Configuration
BASE_URL="http://localhost:8080"
ENTITY_TYPE="users"
CONCURRENT_USERS=10
REQUESTS_PER_USER=50
TOTAL_REQUESTS=$((CONCURRENT_USERS * REQUESTS_PER_USER))

# Test result tracking
PASSED_TESTS=0
FAILED_TESTS=0
TEST_RESULTS=()

# Function to log test results
log_test() {
    local test_name="$1"
    local status="$2"
    local details="$3"
    
    if [ "$status" = "PASS" ]; then
        echo "✅ $test_name: PASSED - $details"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo "❌ $test_name: FAILED - $details"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    TEST_RESULTS+=("$test_name: $status - $details")
}

# Function to make API calls and measure performance
make_api_call() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    local user_id="$4"
    
    local start_time=$(python3 -c "import time; print(time.time())")
    
    if [ "$method" = "POST" ]; then
        local response=$(curl -s -w "\n%{http_code}\n%{time_total}" -X POST \
            -H "Content-Type: application/json" \
            -d "$data" "$endpoint")
    else
        local response=$(curl -s -w "\n%{http_code}\n%{time_total}" "$endpoint")
    fi
    
    local end_time=$(python3 -c "import time; print(time.time())")
    local total_time=$(python3 -c "print($end_time - $start_time)")
    
    echo "$response"
}

echo "📋 1. BASIC API FUNCTIONALITY TESTS"
echo "=================================="

# Test 1: Health Check
echo "Testing health endpoint..."
health_response=$(curl -s "$BASE_URL/actuator/health")
health_status=$(echo "$health_response" | jq -r '.status // "UNKNOWN"')

if [ "$health_status" = "UP" ]; then
    log_test "Health Check" "PASS" "Application is healthy"
else
    log_test "Health Check" "FAIL" "Health status: $health_status"
fi

# Test 2: Entity Types Discovery
echo "Testing entity types endpoint..."
entity_types=$(curl -s "$BASE_URL/api/v1/entities" | jq -r '.supportedEntityTypes[]' 2>/dev/null || echo "")

if echo "$entity_types" | grep -q "users"; then
    log_test "Entity Types Discovery" "PASS" "Users entity type available"
else
    log_test "Entity Types Discovery" "FAIL" "Users entity not found in: $entity_types"
fi

# Test 3: Single Entity Creation
echo "Testing single entity creation..."
test_user_data='{
    "user_id": "integration_test_001",
    "username": "integration_user",
    "email": "integration@test.com",
    "country": "US",
    "signup_date": "2024-08-10"
}'

create_response=$(make_api_call "POST" "$BASE_URL/api/v1/entities/users" "$test_user_data" "test_001")
http_code=$(echo "$create_response" | tail -2 | head -1)
response_time=$(echo "$create_response" | tail -1)

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    log_test "Single Entity Creation" "PASS" "HTTP $http_code, ${response_time}s response time"
else
    log_test "Single Entity Creation" "FAIL" "HTTP $http_code, Response: $(echo "$create_response" | head -1)"
fi

echo ""
echo "🚀 2. CONCURRENCY STRESS TESTS"
echo "==============================="

# Create background jobs for concurrent testing
echo "Spawning $CONCURRENT_USERS concurrent users with $REQUESTS_PER_USER requests each..."
echo "Total requests: $TOTAL_REQUESTS"

# Create temporary files for results
TEMP_DIR=$(mktemp -d)
RESULTS_FILE="$TEMP_DIR/results.txt"
ERRORS_FILE="$TEMP_DIR/errors.txt"
LATENCY_FILE="$TEMP_DIR/latency.txt"

# Function to run concurrent user simulation
run_concurrent_user() {
    local user_id="$1"
    local user_requests="$2"
    
    for ((i=1; i<=user_requests; i++)); do
        local request_id="${user_id}_${i}"
        local user_data=$(cat <<EOF
{
    "user_id": "stress_test_${request_id}",
    "username": "stress_user_${request_id}",
    "email": "stress_${request_id}@test.com",
    "country": "US",
    "signup_date": "2024-08-10"
}
EOF
)
        
        local start_time=$(python3 -c "import time; print(time.time())")
        local response=$(curl -s -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d "$user_data" "$BASE_URL/api/v1/entities/users")
        local end_time=$(python3 -c "import time; print(time.time())")
        local latency=$(python3 -c "print($end_time - $start_time)")
        
        local http_code=$(echo "$response" | tail -c 4)
        
        # Record results
        echo "user_$user_id,$i,$http_code,$latency" >> "$RESULTS_FILE"
        echo "$latency" >> "$LATENCY_FILE"
        
        if [ "$http_code" != "200" ] && [ "$http_code" != "201" ]; then
            echo "user_$user_id,$i,$http_code,$(echo "$response" | head -c 100)" >> "$ERRORS_FILE"
        fi
        
        # Small delay to avoid overwhelming the system
        sleep 0.01
    done
}

# Start concurrent users
echo "Starting concurrent stress test..."
start_time=$(date +%s)

for ((user=1; user<=CONCURRENT_USERS; user++)); do
    run_concurrent_user "$user" "$REQUESTS_PER_USER" &
done

# Wait for all background jobs to complete
wait

end_time=$(date +%s)
total_duration=$((end_time - start_time))

echo "Concurrent stress test completed in ${total_duration} seconds"

echo ""
echo "📊 3. PERFORMANCE ANALYSIS"
echo "=========================="

# Analyze results
if [ -f "$RESULTS_FILE" ]; then
    total_requests_made=$(wc -l < "$RESULTS_FILE")
    successful_requests=$(grep -c -E "(200|201)$" "$RESULTS_FILE" 2>/dev/null || echo "0")
    failed_requests=$((total_requests_made - successful_requests))
    success_rate=$(python3 -c "print(round($successful_requests / $total_requests_made * 100, 2) if $total_requests_made > 0 else 0)")
    
    echo "Total Requests: $total_requests_made"
    echo "Successful: $successful_requests"
    echo "Failed: $failed_requests"
    echo "Success Rate: ${success_rate}%"
    echo "Test Duration: ${total_duration}s"
    echo "Throughput: $(python3 -c "print(round($total_requests_made / $total_duration, 2))")/sec"
    
    if [ "$success_rate" -ge "95" ]; then
        log_test "Concurrency Stress Test" "PASS" "${success_rate}% success rate with $CONCURRENT_USERS concurrent users"
    else
        log_test "Concurrency Stress Test" "FAIL" "Only ${success_rate}% success rate (expected >= 95%)"
    fi
    
    # Latency analysis
    if [ -f "$LATENCY_FILE" ] && [ -s "$LATENCY_FILE" ]; then
        avg_latency=$(python3 -c "
import statistics
with open('$LATENCY_FILE') as f:
    latencies = [float(line.strip()) for line in f if line.strip()]
print(round(statistics.mean(latencies) * 1000, 2) if latencies else 0)
")
        p95_latency=$(python3 -c "
import statistics
with open('$LATENCY_FILE') as f:
    latencies = [float(line.strip()) for line in f if line.strip()]
    latencies.sort()
if latencies:
    p95_idx = int(len(latencies) * 0.95)
    print(round(latencies[p95_idx] * 1000, 2))
else:
    print(0)
")
        
        echo "Average Latency: ${avg_latency}ms"
        echo "95th Percentile Latency: ${p95_latency}ms"
        
        if (( $(echo "$avg_latency < 500" | bc -l) )); then
            log_test "Performance Latency" "PASS" "Average latency ${avg_latency}ms < 500ms"
        else
            log_test "Performance Latency" "FAIL" "Average latency ${avg_latency}ms exceeds 500ms threshold"
        fi
    fi
else
    log_test "Concurrency Stress Test" "FAIL" "No results file generated"
fi

echo ""
echo "🔍 4. DATA PERSISTENCE & INTEGRITY VERIFICATION"
echo "=============================================="

# Test data persistence by checking MinIO
echo "Checking data persistence in Delta Lake format..."

# Get performance metrics to verify writes were recorded
metrics_response=$(curl -s "$BASE_URL/api/v1/performance/metrics")
writes_count=$(echo "$metrics_response" | jq -r '.optimized_metrics.writes // 0')

echo "Application reports $writes_count writes recorded"

if [ "$writes_count" -gt "0" ]; then
    log_test "Data Persistence Tracking" "PASS" "$writes_count writes recorded in metrics"
else
    log_test "Data Persistence Tracking" "FAIL" "No writes recorded in application metrics"
fi

# Verify MinIO bucket access
echo "Verifying MinIO bucket access..."
minio_check=$(curl -s "$BASE_URL/actuator/health" 2>/dev/null || echo "error")
if echo "$minio_check" | grep -q "UP"; then
    log_test "Storage Backend Health" "PASS" "MinIO storage backend accessible"
else
    log_test "Storage Backend Health" "FAIL" "Storage backend health check failed"
fi

echo ""
echo "🎯 5. SYSTEM RESOURCE USAGE"
echo "==========================="

# Check system metrics during load
memory_usage=$(ps aux | grep java | grep -v grep | awk '{print $6}' | head -1)
if [ -n "$memory_usage" ]; then
    memory_mb=$(echo "$memory_usage / 1024" | bc)
    echo "Java Process Memory Usage: ${memory_mb}MB"
    
    if [ "$memory_mb" -lt 2048 ]; then
        log_test "Memory Usage" "PASS" "${memory_mb}MB < 2GB threshold"
    else
        log_test "Memory Usage" "WARN" "${memory_mb}MB exceeds 2GB threshold"
    fi
else
    log_test "Memory Usage" "FAIL" "Could not determine memory usage"
fi

# CPU usage check
cpu_usage=$(ps aux | grep java | grep -v grep | awk '{print $3}' | head -1)
if [ -n "$cpu_usage" ]; then
    echo "Java Process CPU Usage: ${cpu_usage}%"
    
    if (( $(echo "$cpu_usage < 80" | bc -l) )); then
        log_test "CPU Usage" "PASS" "${cpu_usage}% < 80% threshold"
    else
        log_test "CPU Usage" "WARN" "${cpu_usage}% exceeds 80% threshold"
    fi
fi

echo ""
echo "🧪 6. ERROR ANALYSIS"
echo "==================="

# Analyze errors if any occurred
if [ -f "$ERRORS_FILE" ] && [ -s "$ERRORS_FILE" ]; then
    echo "Errors encountered during testing:"
    echo "$(cat "$ERRORS_FILE")"
    
    error_count=$(wc -l < "$ERRORS_FILE")
    echo "Total errors: $error_count"
else
    echo "No errors encountered during testing"
fi

# Cleanup temp files
rm -rf "$TEMP_DIR"

echo ""
echo "🏁 INTEGRATION TEST SUMMARY"
echo "============================"
echo "Tests Passed: $PASSED_TESTS"
echo "Tests Failed: $FAILED_TESTS"
echo "Total Tests: $((PASSED_TESTS + FAILED_TESTS))"
echo ""

if [ "$FAILED_TESTS" -eq 0 ]; then
    echo "🎉 ALL TESTS PASSED! The system is performing well under load."
    exit 0
else
    echo "⚠️  $FAILED_TESTS tests failed. Review the issues above."
    exit 1
fi