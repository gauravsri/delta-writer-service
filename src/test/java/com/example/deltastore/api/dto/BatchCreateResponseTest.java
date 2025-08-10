package com.example.deltastore.api.dto;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class BatchCreateResponseTest {

    private BatchCreateResponse response;
    private List<String> successfulUserIds;
    private List<BatchCreateResponse.FailureDetail> failures;

    @BeforeEach
    void setUp() {
        successfulUserIds = List.of("user1", "user2", "user3");
        failures = List.of(
            BatchCreateResponse.FailureDetail.builder()
                .userId("user4")
                .index(3)
                .error("Validation failed")
                .errorType("VALIDATION_ERROR")
                .build()
        );

        response = BatchCreateResponse.builder()
            .totalRequested(4)
            .successCount(3)
            .failureCount(1)
            .successfulUserIds(successfulUserIds)
            .failures(failures)
            .errors(List.of("user4 validation failed"))
            .processedAt(LocalDateTime.now())
            .processingTimeMs(150L)
            .build();
    }

    @Test
    void testBuilder() {
        assertNotNull(response);
        assertEquals(4, response.getTotalRequested());
        assertEquals(3, response.getSuccessCount());
        assertEquals(1, response.getFailureCount());
        assertEquals(successfulUserIds, response.getSuccessfulUserIds());
        assertEquals(failures, response.getFailures());
        assertNotNull(response.getProcessedAt());
        assertEquals(150L, response.getProcessingTimeMs());
    }

    @Test
    void testGettersAndSetters() {
        BatchCreateResponse newResponse = BatchCreateResponse.builder().build();
        newResponse.setTotalRequested(5);
        newResponse.setSuccessCount(4);
        newResponse.setFailureCount(1);
        newResponse.setSuccessfulUserIds(List.of("user1", "user2"));
        newResponse.setFailures(failures);
        newResponse.setErrors(List.of("error"));
        LocalDateTime now = LocalDateTime.now();
        newResponse.setProcessedAt(now);
        newResponse.setProcessingTimeMs(200L);

        assertEquals(5, newResponse.getTotalRequested());
        assertEquals(4, newResponse.getSuccessCount());
        assertEquals(1, newResponse.getFailureCount());
        assertEquals(List.of("user1", "user2"), newResponse.getSuccessfulUserIds());
        assertEquals(failures, newResponse.getFailures());
        assertEquals(List.of("error"), newResponse.getErrors());
        assertEquals(now, newResponse.getProcessedAt());
        assertEquals(200L, newResponse.getProcessingTimeMs());
    }

    @Test
    void testEqualsAndHashCode() {
        BatchCreateResponse response1 = BatchCreateResponse.builder()
            .totalRequested(2)
            .successCount(1)
            .failureCount(1)
            .build();

        BatchCreateResponse response2 = BatchCreateResponse.builder()
            .totalRequested(2)
            .successCount(1)
            .failureCount(1)
            .build();

        assertEquals(response1, response2);
        assertEquals(response1.hashCode(), response2.hashCode());
    }

    @Test
    void testToString() {
        String toString = response.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("BatchCreateResponse"));
    }

    @Test
    void testFailureDetail() {
        BatchCreateResponse.FailureDetail failure = BatchCreateResponse.FailureDetail.builder()
            .userId("testUser")
            .index(0)
            .error("Test error")
            .errorType("TEST_ERROR")
            .build();

        assertEquals("testUser", failure.getUserId());
        assertEquals(0, failure.getIndex());
        assertEquals("Test error", failure.getError());
        assertEquals("TEST_ERROR", failure.getErrorType());
    }

    @Test
    void testBatchStatistics() {
        BatchCreateResponse.BatchStatistics stats = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(5)
            .avgBatchSize(20)
            .avgProcessingTimePerBatch(100L)
            .totalDeltaTransactionTime(500L)
            .deltaTransactionCount(5)
            .additionalMetrics(Map.of("memory_usage", "50MB"))
            .build();

        assertEquals(5, stats.getTotalBatches());
        assertEquals(20, stats.getAvgBatchSize());
        assertEquals(100L, stats.getAvgProcessingTimePerBatch());
        assertEquals(500L, stats.getTotalDeltaTransactionTime());
        assertEquals(5, stats.getDeltaTransactionCount());
        assertEquals(Map.of("memory_usage", "50MB"), stats.getAdditionalMetrics());
    }

    @Test
    void testWithStatistics() {
        BatchCreateResponse.BatchStatistics stats = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(3)
            .avgBatchSize(10)
            .build();

        BatchCreateResponse responseWithStats = BatchCreateResponse.builder()
            .totalRequested(10)
            .successCount(8)
            .failureCount(2)
            .statistics(stats)
            .build();

        assertEquals(10, responseWithStats.getTotalRequested());
        assertEquals(8, responseWithStats.getSuccessCount());
        assertEquals(2, responseWithStats.getFailureCount());
        assertEquals(stats, responseWithStats.getStatistics());
    }

    @Test
    void testEmptyResponse() {
        BatchCreateResponse emptyResponse = BatchCreateResponse.builder()
            .totalRequested(0)
            .successCount(0)
            .failureCount(0)
            .successfulUserIds(List.of())
            .failures(List.of())
            .errors(List.of())
            .build();

        assertEquals(0, emptyResponse.getTotalRequested());
        assertEquals(0, emptyResponse.getSuccessCount());
        assertEquals(0, emptyResponse.getFailureCount());
        assertTrue(emptyResponse.getSuccessfulUserIds().isEmpty());
        assertTrue(emptyResponse.getFailures().isEmpty());
        assertTrue(emptyResponse.getErrors().isEmpty());
    }

    @Test
    void testFailureDetailToString() {
        BatchCreateResponse.FailureDetail failure = failures.get(0);
        String toString = failure.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("FailureDetail"));
    }

    @Test
    void testStatisticsToString() {
        BatchCreateResponse.BatchStatistics stats = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(1)
            .build();
        String toString = stats.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("BatchStatistics"));
    }
}