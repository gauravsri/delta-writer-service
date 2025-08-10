package com.example.deltastore.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class DeltaStoreMetricsTest {

    private MeterRegistry meterRegistry;
    private DeltaStoreMetrics deltaStoreMetrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        deltaStoreMetrics = new DeltaStoreMetrics(meterRegistry);
    }

    @Test
    void testRecordWriteSuccess() {
        deltaStoreMetrics.recordWriteSuccess("test_table", 100L);
        
        // Verify records written counter
        Counter recordsCounter = meterRegistry.find("deltastore.records.written")
            .tag("table", "test_table")
            .counter();
        assertNotNull(recordsCounter);
        assertEquals(100.0, recordsCounter.count());
    }

    @Test
    void testRecordWriteFailure() {
        deltaStoreMetrics.recordWriteFailure("test_table", "VALIDATION_ERROR");
        
        Counter failureCounter = meterRegistry.find("deltastore.write.failures")
            .tag("table", "test_table")
            .tag("error", "VALIDATION_ERROR")
            .counter();
        assertNotNull(failureCounter);
        assertEquals(1.0, failureCounter.count());
    }

    @Test
    void testStartAndStopWriteTimer() {
        Timer.Sample sample = deltaStoreMetrics.startWriteTimer();
        assertNotNull(sample);
        
        // Stop the timer
        assertDoesNotThrow(() -> deltaStoreMetrics.stopWriteTimer(sample, "test_table"));
        
        // Verify timer was created (though we can't easily verify the exact duration)
        Timer writeTimer = meterRegistry.find("deltastore.write.duration")
            .tag("table", "test_table")
            .timer();
        assertNotNull(writeTimer);
        assertEquals(1, writeTimer.count());
    }

    @Test
    void testRecordReadSuccess() {
        deltaStoreMetrics.recordReadSuccess("test_table");
        
        Counter readCounter = meterRegistry.find("deltastore.reads")
            .tag("table", "test_table")
            .tag("result", "success")
            .counter();
        assertNotNull(readCounter);
        assertEquals(1.0, readCounter.count());
    }

    @Test
    void testRecordReadFailure() {
        deltaStoreMetrics.recordReadFailure("test_table", "CONNECTION_ERROR");
        
        Counter failureCounter = meterRegistry.find("deltastore.reads")
            .tag("table", "test_table")
            .tag("result", "failure")
            .tag("error", "CONNECTION_ERROR")
            .counter();
        assertNotNull(failureCounter);
        assertEquals(1.0, failureCounter.count());
    }

    @Test
    void testStartAndStopReadTimer() {
        Timer.Sample sample = deltaStoreMetrics.startReadTimer();
        assertNotNull(sample);
        
        assertDoesNotThrow(() -> deltaStoreMetrics.stopReadTimer(sample, "test_table"));
        
        Timer readTimer = meterRegistry.find("deltastore.read.duration")
            .tag("table", "test_table")
            .timer();
        assertNotNull(readTimer);
        assertEquals(1, readTimer.count());
    }

    @Test
    void testRecordPartitionReadSuccess() {
        deltaStoreMetrics.recordPartitionReadSuccess("test_table", 50L);
        
        // Check partition read counter
        Counter partitionCounter = meterRegistry.find("deltastore.partition.reads")
            .tag("table", "test_table")
            .tag("result", "success")
            .counter();
        assertNotNull(partitionCounter);
        assertEquals(1.0, partitionCounter.count());
        
        // Check records read counter
        Counter recordsCounter = meterRegistry.find("deltastore.records.read")
            .tag("table", "test_table")
            .counter();
        assertNotNull(recordsCounter);
        assertEquals(50.0, recordsCounter.count());
    }

    @Test
    void testRecordPartitionReadFailure() {
        deltaStoreMetrics.recordPartitionReadFailure("test_table", "PARTITION_NOT_FOUND");
        
        Counter failureCounter = meterRegistry.find("deltastore.partition.reads")
            .tag("table", "test_table")
            .tag("result", "failure")
            .tag("error", "PARTITION_NOT_FOUND")
            .counter();
        assertNotNull(failureCounter);
        assertEquals(1.0, failureCounter.count());
    }

    @Test
    void testStartAndStopPartitionReadTimer() {
        Timer.Sample sample = deltaStoreMetrics.startPartitionReadTimer();
        assertNotNull(sample);
        
        assertDoesNotThrow(() -> deltaStoreMetrics.stopPartitionReadTimer(sample, "test_table"));
        
        Timer partitionTimer = meterRegistry.find("deltastore.partition.read.duration")
            .tag("table", "test_table")
            .timer();
        assertNotNull(partitionTimer);
        assertEquals(1, partitionTimer.count());
    }

    @Test
    void testMultipleWriteOperations() {
        deltaStoreMetrics.recordWriteSuccess("table1", 10L);
        deltaStoreMetrics.recordWriteSuccess("table2", 20L);
        deltaStoreMetrics.recordWriteFailure("table3", "ERROR");
        
        // Verify table1 records
        Counter table1Counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "table1")
            .counter();
        assertEquals(10.0, table1Counter.count());
        
        // Verify table2 records
        Counter table2Counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "table2")
            .counter();
        assertEquals(20.0, table2Counter.count());
        
        // Verify table3 failure
        Counter table3FailureCounter = meterRegistry.find("deltastore.write.failures")
            .tag("table", "table3")
            .tag("error", "ERROR")
            .counter();
        assertEquals(1.0, table3FailureCounter.count());
    }

    @Test
    void testWithNullTableName() {
        assertDoesNotThrow(() -> deltaStoreMetrics.recordWriteSuccess(null, 10L));
        
        Counter counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "null")
            .counter();
        // The counter should still be created but with "null" as the tag value
        assertNotNull(counter);
    }

    @Test
    void testWithEmptyTableName() {
        assertDoesNotThrow(() -> deltaStoreMetrics.recordWriteSuccess("", 10L));
        
        Counter counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "")
            .counter();
        assertNotNull(counter);
        assertEquals(10.0, counter.count());
    }

    @Test
    void testWithZeroRecordCount() {
        deltaStoreMetrics.recordWriteSuccess("test_table", 0L);
        
        Counter counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "test_table")
            .counter();
        assertEquals(0.0, counter.count());
    }

    @Test
    void testWithLargeRecordCount() {
        deltaStoreMetrics.recordWriteSuccess("test_table", 1000000L);
        
        Counter counter = meterRegistry.find("deltastore.records.written")
            .tag("table", "test_table")
            .counter();
        assertEquals(1000000.0, counter.count());
    }

    @Test
    void testTimerSampleReuse() {
        // Start multiple timers
        Timer.Sample writesSample = deltaStoreMetrics.startWriteTimer();
        Timer.Sample readSample = deltaStoreMetrics.startReadTimer();
        Timer.Sample partitionSample = deltaStoreMetrics.startPartitionReadTimer();
        
        assertNotNull(writesSample);
        assertNotNull(readSample);
        assertNotNull(partitionSample);
        
        // Stop all timers
        assertDoesNotThrow(() -> {
            deltaStoreMetrics.stopWriteTimer(writesSample, "table1");
            deltaStoreMetrics.stopReadTimer(readSample, "table2");
            deltaStoreMetrics.stopPartitionReadTimer(partitionSample, "table3");
        });
    }

    @Test
    void testCompleteWorkflow() {
        // Simulate a complete write workflow
        Timer.Sample writeSample = deltaStoreMetrics.startWriteTimer();
        
        try {
            // Simulate successful write
            deltaStoreMetrics.recordWriteSuccess("users", 100L);
        } finally {
            deltaStoreMetrics.stopWriteTimer(writeSample, "users");
        }
        
        // Simulate a read workflow
        Timer.Sample readSample = deltaStoreMetrics.startReadTimer();
        
        try {
            // Simulate successful read
            deltaStoreMetrics.recordReadSuccess("users");
        } finally {
            deltaStoreMetrics.stopReadTimer(readSample, "users");
        }
        
        // Verify all metrics were recorded
        assertNotNull(meterRegistry.find("deltastore.records.written").counter());
        assertNotNull(meterRegistry.find("deltastore.write.duration").timer());
        assertNotNull(meterRegistry.find("deltastore.reads").counter());
        assertNotNull(meterRegistry.find("deltastore.read.duration").timer());
    }
}