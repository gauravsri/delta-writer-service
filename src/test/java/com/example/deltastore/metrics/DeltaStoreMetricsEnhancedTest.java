package com.example.deltastore.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DeltaStoreMetricsEnhancedTest {

    private MeterRegistry meterRegistry;
    private DeltaStoreMetrics deltaStoreMetrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        deltaStoreMetrics = new DeltaStoreMetrics(meterRegistry);
    }

    @Test
    @DisplayName("Should record write success with correct metrics")
    void testRecordWriteSuccess() {
        // Given
        String tableName = "users";
        long recordCount = 10;

        // When
        deltaStoreMetrics.recordWriteSuccess(tableName, recordCount);

        // Then
        Counter writeSuccessCounter = meterRegistry.find("deltastore.writes.success").counter();
        assertNotNull(writeSuccessCounter);
        assertEquals(1.0, writeSuccessCounter.count());

        Counter recordsWrittenCounter = meterRegistry.find("deltastore.records.written")
            .tag("table", tableName)
            .counter();
        assertNotNull(recordsWrittenCounter);
        assertEquals(recordCount, recordsWrittenCounter.count());
    }

    @Test
    @DisplayName("Should record write failure with correct metrics and tags")
    void testRecordWriteFailure() {
        // Given
        String tableName = "users";
        String errorType = "RuntimeException";

        // When
        deltaStoreMetrics.recordWriteFailure(tableName, errorType);

        // Then
        Counter writeFailureCounter = meterRegistry.find("deltastore.writes.failure").counter();
        assertNotNull(writeFailureCounter);
        assertEquals(1.0, writeFailureCounter.count());

        Counter writeFailuresCounter = meterRegistry.find("deltastore.write.failures")
            .tag("table", tableName)
            .tag("error", errorType)
            .counter();
        assertNotNull(writeFailuresCounter);
        assertEquals(1.0, writeFailuresCounter.count());
    }

    @Test
    @DisplayName("Should create and stop write timer with correct tags")
    void testWriteTimer() throws InterruptedException {
        // Given
        String tableName = "users";

        // When
        Timer.Sample sample = deltaStoreMetrics.startWriteTimer();
        Thread.sleep(10); // Small delay to measure
        deltaStoreMetrics.stopWriteTimer(sample, tableName);

        // Then
        Timer writeTimer = meterRegistry.find("deltastore.write.duration")
            .tag("table", tableName)
            .timer();
        assertNotNull(writeTimer);
        assertEquals(1L, writeTimer.count());
        assertTrue(writeTimer.totalTime(TimeUnit.NANOSECONDS) > 0);
    }

    @Test
    @DisplayName("Should record read success with correct metrics")
    void testRecordReadSuccess() {
        // Given
        String tableName = "orders";

        // When
        deltaStoreMetrics.recordReadSuccess(tableName);

        // Then
        Counter readSuccessCounter = meterRegistry.find("deltastore.reads.success").counter();
        assertNotNull(readSuccessCounter);
        assertEquals(1.0, readSuccessCounter.count());

        Counter readsCounter = meterRegistry.find("deltastore.reads")
            .tag("table", tableName)
            .tag("result", "success")
            .counter();
        assertNotNull(readsCounter);
        assertEquals(1.0, readsCounter.count());
    }

    @Test
    @DisplayName("Should record read failure with correct metrics and error type")
    void testRecordReadFailure() {
        // Given
        String tableName = "products";
        String errorType = "TableReadException";

        // When
        deltaStoreMetrics.recordReadFailure(tableName, errorType);

        // Then
        Counter readFailureCounter = meterRegistry.find("deltastore.reads.failure").counter();
        assertNotNull(readFailureCounter);
        assertEquals(1.0, readFailureCounter.count());

        Counter readsCounter = meterRegistry.find("deltastore.reads")
            .tag("table", tableName)
            .tag("result", "failure")
            .tag("error", errorType)
            .counter();
        assertNotNull(readsCounter);
        assertEquals(1.0, readsCounter.count());
    }

    @Test
    @DisplayName("Should create and stop read timer with correct tags")
    void testReadTimer() throws InterruptedException {
        // Given
        String tableName = "inventory";

        // When
        Timer.Sample sample = deltaStoreMetrics.startReadTimer();
        Thread.sleep(5); // Small delay
        deltaStoreMetrics.stopReadTimer(sample, tableName);

        // Then
        Timer readTimer = meterRegistry.find("deltastore.read.duration")
            .tag("table", tableName)
            .timer();
        assertNotNull(readTimer);
        assertEquals(1L, readTimer.count());
        assertTrue(readTimer.totalTime(TimeUnit.NANOSECONDS) > 0);
    }

    @Test
    @DisplayName("Should record partition read success with result count")
    void testRecordPartitionReadSuccess() {
        // Given
        String tableName = "events";
        long resultCount = 25;

        // When
        deltaStoreMetrics.recordPartitionReadSuccess(tableName, resultCount);

        // Then
        Counter partitionReadSuccessCounter = meterRegistry.find("deltastore.partition.reads.success").counter();
        assertNotNull(partitionReadSuccessCounter);
        assertEquals(1.0, partitionReadSuccessCounter.count());

        Counter partitionReadsCounter = meterRegistry.find("deltastore.partition.reads")
            .tag("table", tableName)
            .tag("result", "success")
            .counter();
        assertNotNull(partitionReadsCounter);
        assertEquals(1.0, partitionReadsCounter.count());

        Counter recordsReadCounter = meterRegistry.find("deltastore.records.read")
            .tag("table", tableName)
            .counter();
        assertNotNull(recordsReadCounter);
        assertEquals(resultCount, recordsReadCounter.count());
    }

    @Test
    @DisplayName("Should record partition read failure with error type")
    void testRecordPartitionReadFailure() {
        // Given
        String tableName = "logs";
        String errorType = "PartitionNotFoundException";

        // When
        deltaStoreMetrics.recordPartitionReadFailure(tableName, errorType);

        // Then
        Counter partitionReadFailureCounter = meterRegistry.find("deltastore.partition.reads.failure").counter();
        assertNotNull(partitionReadFailureCounter);
        assertEquals(1.0, partitionReadFailureCounter.count());

        Counter partitionReadsCounter = meterRegistry.find("deltastore.partition.reads")
            .tag("table", tableName)
            .tag("result", "failure")
            .tag("error", errorType)
            .counter();
        assertNotNull(partitionReadsCounter);
        assertEquals(1.0, partitionReadsCounter.count());
    }

    @Test
    @DisplayName("Should create and stop partition read timer")
    void testPartitionReadTimer() throws InterruptedException {
        // Given
        String tableName = "metrics";

        // When
        Timer.Sample sample = deltaStoreMetrics.startPartitionReadTimer();
        Thread.sleep(8); // Small delay
        deltaStoreMetrics.stopPartitionReadTimer(sample, tableName);

        // Then
        Timer partitionReadTimer = meterRegistry.find("deltastore.partition.read.duration")
            .tag("table", tableName)
            .timer();
        assertNotNull(partitionReadTimer);
        assertEquals(1L, partitionReadTimer.count());
        assertTrue(partitionReadTimer.totalTime(TimeUnit.NANOSECONDS) > 0);
    }

    @Test
    @DisplayName("Should handle multiple write operations correctly")
    void testMultipleWriteOperations() {
        // Given
        String tableName = "users";

        // When
        deltaStoreMetrics.recordWriteSuccess(tableName, 5);
        deltaStoreMetrics.recordWriteSuccess(tableName, 10);
        deltaStoreMetrics.recordWriteFailure(tableName, "ValidationError");

        // Then
        Counter writeSuccessCounter = meterRegistry.find("deltastore.writes.success").counter();
        assertEquals(2.0, writeSuccessCounter.count());

        Counter writeFailureCounter = meterRegistry.find("deltastore.writes.failure").counter();
        assertEquals(1.0, writeFailureCounter.count());

        Counter recordsWrittenCounter = meterRegistry.find("deltastore.records.written")
            .tag("table", tableName)
            .counter();
        assertEquals(15.0, recordsWrittenCounter.count()); // 5 + 10
    }

    @Test
    @DisplayName("Should handle multiple read operations correctly")
    void testMultipleReadOperations() {
        // Given
        String tableName = "orders";

        // When
        deltaStoreMetrics.recordReadSuccess(tableName);
        deltaStoreMetrics.recordReadSuccess(tableName);
        deltaStoreMetrics.recordReadFailure(tableName, "ConnectionTimeout");

        // Then
        Counter readSuccessCounter = meterRegistry.find("deltastore.reads.success").counter();
        assertEquals(2.0, readSuccessCounter.count());

        Counter readFailureCounter = meterRegistry.find("deltastore.reads.failure").counter();
        assertEquals(1.0, readFailureCounter.count());
    }

    @Test
    @DisplayName("Should handle different table names independently")
    void testDifferentTableNames() {
        // Given
        String table1 = "users";
        String table2 = "orders";
        String table3 = "products";

        // When
        deltaStoreMetrics.recordWriteSuccess(table1, 10);
        deltaStoreMetrics.recordWriteSuccess(table2, 20);
        deltaStoreMetrics.recordReadSuccess(table3);

        // Then
        Counter recordsWrittenTable1 = meterRegistry.find("deltastore.records.written")
            .tag("table", table1)
            .counter();
        assertEquals(10.0, recordsWrittenTable1.count());

        Counter recordsWrittenTable2 = meterRegistry.find("deltastore.records.written")
            .tag("table", table2)
            .counter();
        assertEquals(20.0, recordsWrittenTable2.count());

        Counter readsTable3 = meterRegistry.find("deltastore.reads")
            .tag("table", table3)
            .tag("result", "success")
            .counter();
        assertEquals(1.0, readsTable3.count());
    }

    @Test
    @DisplayName("Should handle different error types independently")
    void testDifferentErrorTypes() {
        // Given
        String tableName = "events";
        String error1 = "ValidationException";
        String error2 = "TimeoutException";
        String error3 = "DatabaseException";

        // When
        deltaStoreMetrics.recordWriteFailure(tableName, error1);
        deltaStoreMetrics.recordWriteFailure(tableName, error1);
        deltaStoreMetrics.recordReadFailure(tableName, error2);
        deltaStoreMetrics.recordPartitionReadFailure(tableName, error3);

        // Then
        Counter writeFailuresValidation = meterRegistry.find("deltastore.write.failures")
            .tag("table", tableName)
            .tag("error", error1)
            .counter();
        assertEquals(2.0, writeFailuresValidation.count());

        Counter readFailuresTimeout = meterRegistry.find("deltastore.reads")
            .tag("table", tableName)
            .tag("result", "failure")
            .tag("error", error2)
            .counter();
        assertEquals(1.0, readFailuresTimeout.count());

        Counter partitionReadFailuresDB = meterRegistry.find("deltastore.partition.reads")
            .tag("table", tableName)
            .tag("result", "failure")
            .tag("error", error3)
            .counter();
        assertEquals(1.0, partitionReadFailuresDB.count());
    }

    @Test
    @DisplayName("Should handle concurrent timer operations")
    void testConcurrentTimerOperations() throws InterruptedException {
        // Given
        String tableName = "concurrent_test";

        // When - Start multiple timers concurrently
        Timer.Sample writeTimer1 = deltaStoreMetrics.startWriteTimer();
        Timer.Sample readTimer1 = deltaStoreMetrics.startReadTimer();
        Timer.Sample partitionTimer1 = deltaStoreMetrics.startPartitionReadTimer();

        Thread.sleep(5);

        deltaStoreMetrics.stopWriteTimer(writeTimer1, tableName);
        deltaStoreMetrics.stopReadTimer(readTimer1, tableName);
        deltaStoreMetrics.stopPartitionReadTimer(partitionTimer1, tableName);

        // Then
        Timer writeTimer = meterRegistry.find("deltastore.write.duration")
            .tag("table", tableName).timer();
        assertNotNull(writeTimer);
        assertEquals(1L, writeTimer.count());

        Timer readTimer = meterRegistry.find("deltastore.read.duration")
            .tag("table", tableName).timer();
        assertNotNull(readTimer);
        assertEquals(1L, readTimer.count());

        Timer partitionTimer = meterRegistry.find("deltastore.partition.read.duration")
            .tag("table", tableName).timer();
        assertNotNull(partitionTimer);
        assertEquals(1L, partitionTimer.count());
    }

    @Test
    @DisplayName("Should handle zero record counts correctly")
    void testZeroRecordCounts() {
        // Given
        String tableName = "empty_table";

        // When
        deltaStoreMetrics.recordWriteSuccess(tableName, 0);
        deltaStoreMetrics.recordPartitionReadSuccess(tableName, 0);

        // Then
        Counter recordsWrittenCounter = meterRegistry.find("deltastore.records.written")
            .tag("table", tableName)
            .counter();
        assertEquals(0.0, recordsWrittenCounter.count());

        Counter recordsReadCounter = meterRegistry.find("deltastore.records.read")
            .tag("table", tableName)
            .counter();
        assertEquals(0.0, recordsReadCounter.count());
    }

    @Test
    @DisplayName("Should handle large record counts correctly")
    void testLargeRecordCounts() {
        // Given
        String tableName = "large_table";
        long largeCount = 1_000_000L;

        // When
        deltaStoreMetrics.recordWriteSuccess(tableName, largeCount);
        deltaStoreMetrics.recordPartitionReadSuccess(tableName, largeCount);

        // Then
        Counter recordsWrittenCounter = meterRegistry.find("deltastore.records.written")
            .tag("table", tableName)
            .counter();
        assertEquals(largeCount, recordsWrittenCounter.count());

        Counter recordsReadCounter = meterRegistry.find("deltastore.records.read")
            .tag("table", tableName)
            .counter();
        assertEquals(largeCount, recordsReadCounter.count());
    }

    @Test
    @DisplayName("Should maintain separate counters for lazy initialization")
    void testLazyInitializationSeparation() {
        // Given/When - Trigger lazy initialization of different metrics
        deltaStoreMetrics.recordWriteSuccess("table1", 1);
        deltaStoreMetrics.recordReadSuccess("table2");
        deltaStoreMetrics.recordPartitionReadSuccess("table3", 1);

        // Then - Verify each metric type exists independently
        assertNotNull(meterRegistry.find("deltastore.writes.success").counter());
        assertNotNull(meterRegistry.find("deltastore.reads.success").counter());
        assertNotNull(meterRegistry.find("deltastore.partition.reads.success").counter());

        // Verify counts are independent
        assertEquals(1.0, meterRegistry.find("deltastore.writes.success").counter().count());
        assertEquals(1.0, meterRegistry.find("deltastore.reads.success").counter().count());
        assertEquals(1.0, meterRegistry.find("deltastore.partition.reads.success").counter().count());
    }
}