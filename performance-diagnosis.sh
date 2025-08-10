#!/bin/bash

# =====================================================================
# TECH LEAD - PERFORMANCE DIAGNOSIS: MinIO vs Delta Kernel
# =====================================================================

set -e

echo "🔍 TECH LEAD - PERFORMANCE BOTTLENECK DIAGNOSIS"
echo "=============================================="
echo "Timestamp: $(date)"
echo ""

# Test MinIO direct performance
echo "📊 1. TESTING MinIO DIRECT PERFORMANCE"
echo "======================================"

# Test MinIO connectivity and basic operations
echo "Testing MinIO connectivity..."
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minio"
MINIO_SECRET_KEY="minio123"
BUCKET_NAME="my-bucket"

# Test basic MinIO operations
echo "Testing MinIO basic operations..."

# Test 1: MinIO Health Check
start_time=$(python3 -c "import time; print(time.time())")
minio_health=$(curl -s -I "$MINIO_ENDPOINT/minio/health/live" | head -1 | grep -o "200" || echo "failed")
end_time=$(python3 -c "import time; print(time.time())")
minio_health_latency=$(python3 -c "print(round(($end_time - $start_time) * 1000, 2))")

if [ "$minio_health" = "200" ]; then
    echo "✅ MinIO Health: OK (${minio_health_latency}ms)"
else
    echo "❌ MinIO Health: FAILED"
    exit 1
fi

# Test 2: Create small file in MinIO directly (using AWS CLI if available or curl)
echo "Testing direct file operations to MinIO..."

# Create test file
TEST_FILE="/tmp/test_file_$(date +%s).txt"
echo "Test data for performance analysis at $(date)" > "$TEST_FILE"

# Test file upload using curl (simulating S3 PUT)
start_time=$(python3 -c "import time; print(time.time())")
upload_result=$(curl -s -X PUT "$MINIO_ENDPOINT/$BUCKET_NAME/test/$(basename $TEST_FILE)" \
    -H "Host: localhost:9000" \
    -H "Content-Type: text/plain" \
    --data-binary @"$TEST_FILE" \
    -w "%{http_code}" || echo "000")
end_time=$(python3 -c "import time; print(time.time())")
upload_latency=$(python3 -c "print(round(($end_time - $start_time) * 1000, 2))")

if [ "$upload_result" = "200" ] || [ "$upload_result" = "201" ]; then
    echo "✅ MinIO File Upload: OK (${upload_latency}ms)"
else
    echo "⚠️  MinIO File Upload: Status $upload_result (${upload_latency}ms)"
fi

# Clean up
rm -f "$TEST_FILE"

echo ""
echo "📊 2. TESTING HADOOP S3A FILESYSTEM PERFORMANCE"
echo "=============================================="

# Start the application and test S3A operations
mvn spring-boot:run -Dspring-boot.run.profiles=local &
APP_PID=$!
echo "Started application (PID: $APP_PID)"

# Wait for application to start
echo "Waiting for application startup..."
sleep 15

# Check if application is ready
APP_READY=false
for i in {1..30}; do
    if curl -s http://localhost:8080/actuator/health | grep -q "UP"; then
        APP_READY=true
        break
    fi
    sleep 2
done

if [ "$APP_READY" = "false" ]; then
    echo "❌ Application failed to start properly"
    kill $APP_PID 2>/dev/null || true
    exit 1
fi

echo "✅ Application is ready"

echo ""
echo "📊 3. ISOLATING DELTA KERNEL OPERATIONS"
echo "======================================"

# Test single operation timing breakdown
echo "Testing single entity creation with detailed timing..."

# Make one API call and measure different phases
start_time=$(python3 -c "import time; print(time.time())")

single_user_data='{
    "user_id": "perf_test_single",
    "username": "performance_test",
    "email": "perf@test.com",
    "country": "US", 
    "signup_date": "2024-08-10"
}'

response=$(curl -s -w "\n%{time_total}" -X POST \
    -H "Content-Type: application/json" \
    -d "$single_user_data" \
    "http://localhost:8080/api/v1/entities/users")

end_time=$(python3 -c "import time; print(time.time())")
total_api_time=$(echo "$response" | tail -1)
total_wall_time=$(python3 -c "print(round($end_time - $start_time, 3))")

http_response=$(echo "$response" | head -n -1)
echo "Single entity creation results:"
echo "  - Total API Time: ${total_api_time}s"
echo "  - Total Wall Time: ${total_wall_time}s"
echo "  - HTTP Response: $(echo "$http_response" | head -c 100)..."

echo ""
echo "📊 4. TESTING CONCURRENT MinIO OPERATIONS (No Delta)"
echo "=================================================="

# Test direct MinIO operations under load to see if MinIO itself is the bottleneck
echo "Testing MinIO under concurrent load (bypassing Delta)..."

TEMP_DIR=$(mktemp -d)
CONCURRENT_UPLOADS=5

# Function to upload files directly to MinIO
test_concurrent_minio() {
    local thread_id="$1"
    local uploads_per_thread=10
    
    for ((i=1; i<=uploads_per_thread; i++)); do
        local test_file="$TEMP_DIR/direct_test_${thread_id}_${i}.txt"
        echo "Direct MinIO test data $thread_id $i $(date)" > "$test_file"
        
        local start_time=$(python3 -c "import time; print(time.time())")
        local result=$(curl -s -X PUT "$MINIO_ENDPOINT/$BUCKET_NAME/direct_test/$(basename $test_file)" \
            -H "Host: localhost:9000" \
            -H "Content-Type: text/plain" \
            --data-binary @"$test_file" \
            -w "%{http_code}" 2>/dev/null || echo "000")
        local end_time=$(python3 -c "import time; print(time.time())")
        local latency=$(python3 -c "print(round(($end_time - $start_time) * 1000, 2))")
        
        echo "$thread_id,$i,$result,$latency" >> "$TEMP_DIR/minio_direct_results.txt"
        rm -f "$test_file"
        
        sleep 0.01  # Small delay
    done
}

echo "Starting $CONCURRENT_UPLOADS concurrent MinIO uploads..."
start_time=$(date +%s)

# Start concurrent uploads
for ((thread=1; thread<=CONCURRENT_UPLOADS; thread++)); do
    test_concurrent_minio "$thread" &
done

# Wait for all uploads to complete
wait

end_time=$(date +%s)
minio_test_duration=$((end_time - start_time))

# Analyze MinIO direct results
if [ -f "$TEMP_DIR/minio_direct_results.txt" ]; then
    total_minio_ops=$(wc -l < "$TEMP_DIR/minio_direct_results.txt")
    successful_minio_ops=$(grep -c -E "(200|201)$" "$TEMP_DIR/minio_direct_results.txt" 2>/dev/null || echo "0")
    minio_success_rate=$(python3 -c "print(round($successful_minio_ops / $total_minio_ops * 100, 2) if $total_minio_ops > 0 else 0)")
    
    # Calculate average MinIO latency
    avg_minio_latency=$(python3 -c "
import sys
total = 0
count = 0
with open('$TEMP_DIR/minio_direct_results.txt') as f:
    for line in f:
        parts = line.strip().split(',')
        if len(parts) >= 4 and parts[2] in ['200', '201']:
            total += float(parts[3])
            count += 1
print(round(total / count, 2) if count > 0 else 0)
")
    
    echo "MinIO Direct Test Results:"
    echo "  - Total Operations: $total_minio_ops"
    echo "  - Successful: $successful_minio_ops"
    echo "  - Success Rate: ${minio_success_rate}%"
    echo "  - Average Latency: ${avg_minio_latency}ms"
    echo "  - Test Duration: ${minio_test_duration}s"
    echo "  - Throughput: $(python3 -c "print(round($total_minio_ops / $minio_test_duration, 2))")/sec"
fi

echo ""
echo "📊 5. TESTING APPLICATION UNDER LIGHT LOAD"
echo "========================================"

# Test just a few operations through the application to compare
echo "Testing 5 sequential operations through Delta Writer Service..."

APP_RESULTS="$TEMP_DIR/app_results.txt"
for ((i=1; i<=5; i++)); do
    test_user='{
        "user_id": "perf_test_'$i'",
        "username": "perf_user_'$i'", 
        "email": "perf'$i'@test.com",
        "country": "US",
        "signup_date": "2024-08-10"
    }'
    
    start_time=$(python3 -c "import time; print(time.time())")
    response=$(curl -s -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -d "$test_user" \
        "http://localhost:8080/api/v1/entities/users")
    end_time=$(python3 -c "import time; print(time.time())")
    latency=$(python3 -c "print(round(($end_time - $start_time) * 1000, 2))")
    
    http_code=$(echo "$response" | tail -c 4)
    echo "$i,$http_code,$latency" >> "$APP_RESULTS"
    
    echo "Operation $i: HTTP $http_code, ${latency}ms"
    
    # Wait between operations to avoid batching
    sleep 2
done

# Calculate app performance
if [ -f "$APP_RESULTS" ]; then
    avg_app_latency=$(python3 -c "
import sys
total = 0
count = 0
with open('$APP_RESULTS') as f:
    for line in f:
        parts = line.strip().split(',')
        if len(parts) >= 3 and parts[1] in ['200', '201']:
            total += float(parts[2])
            count += 1
print(round(total / count, 2) if count > 0 else 0)
")
    
    echo ""
    echo "Application Sequential Test Results:"
    echo "  - Average Latency: ${avg_app_latency}ms"
fi

echo ""
echo "📊 6. ANALYSIS & CONCLUSIONS"
echo "============================"

echo "Performance Comparison:"
echo "  - MinIO Health Check: ${minio_health_latency}ms"
echo "  - MinIO File Upload: ${upload_latency}ms" 
echo "  - MinIO Concurrent Avg: ${avg_minio_latency}ms"
echo "  - Application Single Op: ${total_api_time}s (${total_api_time}000ms)"
echo "  - Application Sequential Avg: ${avg_app_latency}ms"

# Determine bottleneck
bottleneck="UNKNOWN"
if (( $(echo "$avg_app_latency > ($avg_minio_latency * 10)" | bc -l) )); then
    bottleneck="DELTA_KERNEL"
    echo ""
    echo "🎯 CONCLUSION: DELTA KERNEL is the primary bottleneck"
    echo "    Application latency is >10x higher than direct MinIO operations"
    echo "    Root cause likely in Delta Lake transaction management or S3A filesystem layer"
elif (( $(echo "$avg_minio_latency > 1000" | bc -l) )); then
    bottleneck="MINIO"
    echo ""
    echo "🎯 CONCLUSION: MinIO is the primary bottleneck"
    echo "    MinIO operations themselves are slow (>1000ms)"
else
    bottleneck="CONFIGURATION"
    echo ""
    echo "🎯 CONCLUSION: Configuration or integration issue"
    echo "    Both MinIO and Delta operations show performance issues"
fi

# Cleanup
kill $APP_PID 2>/dev/null || true
rm -rf "$TEMP_DIR"

echo ""
echo "🔧 RECOMMENDED NEXT STEPS based on bottleneck: $bottleneck"
if [ "$bottleneck" = "DELTA_KERNEL" ]; then
    echo "  1. Optimize Delta Kernel configuration (batch sizes, timeouts)"
    echo "  2. Disable Delta checksum validation"
    echo "  3. Optimize S3A filesystem settings for MinIO"
    echo "  4. Review Delta Lake transaction isolation levels"
elif [ "$bottleneck" = "MINIO" ]; then
    echo "  1. Check MinIO server configuration and resources"
    echo "  2. Verify MinIO network connectivity and DNS resolution"
    echo "  3. Consider MinIO performance tuning"
    echo "  4. Check for MinIO storage backend issues"
else
    echo "  1. Review Hadoop S3A configuration for MinIO compatibility"
    echo "  2. Check for network issues between application and MinIO"
    echo "  3. Verify MinIO credentials and permissions"
    echo "  4. Consider using local filesystem for testing"
fi

echo ""
echo "Diagnosis complete. Bottleneck identified as: $bottleneck"