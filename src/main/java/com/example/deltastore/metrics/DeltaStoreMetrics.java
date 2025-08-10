package com.example.deltastore.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DeltaStoreMetrics {
    
    private final MeterRegistry meterRegistry;
    
    // Write operations
    private Counter writeSuccessCounter;
    private Counter writeFailureCounter;
    private Timer writeTimer;
    
    // Read operations
    private Counter readSuccessCounter;
    private Counter readFailureCounter;
    private Timer readTimer;
    
    // Partition read operations
    private Counter partitionReadSuccessCounter;
    private Counter partitionReadFailureCounter;
    private Timer partitionReadTimer;
    
    public void recordWriteSuccess(String tableName, long recordCount) {
        getWriteSuccessCounter().increment();
        meterRegistry.counter("deltastore.records.written", "table", tableName)
                .increment(recordCount);
    }
    
    public void recordWriteFailure(String tableName, String errorType) {
        getWriteFailureCounter().increment();
        meterRegistry.counter("deltastore.write.failures", "table", tableName, "error", errorType)
                .increment();
    }
    
    public Timer.Sample startWriteTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopWriteTimer(Timer.Sample sample, String tableName) {
        sample.stop(Timer.builder("deltastore.write.duration")
                .description("Time taken for write operations")
                .tags("table", tableName)
                .register(meterRegistry));
    }
    
    public void recordReadSuccess(String tableName) {
        getReadSuccessCounter().increment();
        meterRegistry.counter("deltastore.reads", "table", tableName, "result", "success")
                .increment();
    }
    
    public void recordReadFailure(String tableName, String errorType) {
        getReadFailureCounter().increment();
        meterRegistry.counter("deltastore.reads", "table", tableName, "result", "failure", "error", errorType)
                .increment();
    }
    
    public Timer.Sample startReadTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopReadTimer(Timer.Sample sample, String tableName) {
        sample.stop(Timer.builder("deltastore.read.duration")
                .description("Time taken for read operations")
                .tags("table", tableName)
                .register(meterRegistry));
    }
    
    public void recordPartitionReadSuccess(String tableName, long resultCount) {
        getPartitionReadSuccessCounter().increment();
        meterRegistry.counter("deltastore.partition.reads", "table", tableName, "result", "success")
                .increment();
        meterRegistry.counter("deltastore.records.read", "table", tableName)
                .increment(resultCount);
    }
    
    public void recordPartitionReadFailure(String tableName, String errorType) {
        getPartitionReadFailureCounter().increment();
        meterRegistry.counter("deltastore.partition.reads", "table", tableName, "result", "failure", "error", errorType)
                .increment();
    }
    
    public Timer.Sample startPartitionReadTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopPartitionReadTimer(Timer.Sample sample, String tableName) {
        sample.stop(Timer.builder("deltastore.partition.read.duration")
                .description("Time taken for partition read operations")
                .tags("table", tableName)
                .register(meterRegistry));
    }
    
    private Counter getWriteSuccessCounter() {
        if (writeSuccessCounter == null) {
            writeSuccessCounter = Counter.builder("deltastore.writes.success")
                    .description("Number of successful writes")
                    .register(meterRegistry);
        }
        return writeSuccessCounter;
    }
    
    private Counter getWriteFailureCounter() {
        if (writeFailureCounter == null) {
            writeFailureCounter = Counter.builder("deltastore.writes.failure")
                    .description("Number of failed writes")
                    .register(meterRegistry);
        }
        return writeFailureCounter;
    }
    
    private Timer getWriteTimer() {
        if (writeTimer == null) {
            writeTimer = Timer.builder("deltastore.write.duration")
                    .description("Time taken for write operations")
                    .register(meterRegistry);
        }
        return writeTimer;
    }
    
    private Counter getReadSuccessCounter() {
        if (readSuccessCounter == null) {
            readSuccessCounter = Counter.builder("deltastore.reads.success")
                    .description("Number of successful reads")
                    .register(meterRegistry);
        }
        return readSuccessCounter;
    }
    
    private Counter getReadFailureCounter() {
        if (readFailureCounter == null) {
            readFailureCounter = Counter.builder("deltastore.reads.failure")
                    .description("Number of failed reads")
                    .register(meterRegistry);
        }
        return readFailureCounter;
    }
    
    private Timer getReadTimer() {
        if (readTimer == null) {
            readTimer = Timer.builder("deltastore.read.duration")
                    .description("Time taken for read operations")
                    .register(meterRegistry);
        }
        return readTimer;
    }
    
    private Counter getPartitionReadSuccessCounter() {
        if (partitionReadSuccessCounter == null) {
            partitionReadSuccessCounter = Counter.builder("deltastore.partition.reads.success")
                    .description("Number of successful partition reads")
                    .register(meterRegistry);
        }
        return partitionReadSuccessCounter;
    }
    
    private Counter getPartitionReadFailureCounter() {
        if (partitionReadFailureCounter == null) {
            partitionReadFailureCounter = Counter.builder("deltastore.partition.reads.failure")
                    .description("Number of failed partition reads")
                    .register(meterRegistry);
        }
        return partitionReadFailureCounter;
    }
    
    private Timer getPartitionReadTimer() {
        if (partitionReadTimer == null) {
            partitionReadTimer = Timer.builder("deltastore.partition.read.duration")
                    .description("Time taken for partition read operations")
                    .register(meterRegistry);
        }
        return partitionReadTimer;
    }
}