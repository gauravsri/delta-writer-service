package com.example.deltastore.api.dto;

import com.example.deltastore.schemas.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.Validation;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BatchCreateDTOTest {

    private ObjectMapper objectMapper;
    private Validator validator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    @DisplayName("Should create valid BatchCreateRequest")
    void testBatchCreateRequestValid() {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );
        
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);

        // When
        Set<ConstraintViolation<BatchCreateRequest>> violations = validator.validate(request);

        // Then
        assertTrue(violations.isEmpty());
        assertEquals(2, request.getUsers().size());
        assertNull(request.getOptions()); // Default null
    }

    @Test
    @DisplayName("Should reject BatchCreateRequest with empty users list")
    void testBatchCreateRequestEmptyUsers() {
        // Given
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(Collections.emptyList());

        // When
        Set<ConstraintViolation<BatchCreateRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
        assertTrue(violations.iterator().next().getMessage().contains("Users list cannot be empty"));
    }

    @Test
    @DisplayName("Should reject BatchCreateRequest with null users list")
    void testBatchCreateRequestNullUsers() {
        // Given
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(null);

        // When
        Set<ConstraintViolation<BatchCreateRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty());
        assertEquals(1, violations.size());
        assertTrue(violations.iterator().next().getMessage().contains("Users list cannot be empty"));
    }

    @Test
    @DisplayName("Should reject BatchCreateRequest exceeding size limit")
    void testBatchCreateRequestTooManyUsers() {
        // Given
        List<User> tooManyUsers = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            tooManyUsers.add(createUser("user" + i, "user" + i + "@example.com"));
        }
        
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(tooManyUsers);

        // When
        Set<ConstraintViolation<BatchCreateRequest>> violations = validator.validate(request);

        // Then
        assertFalse(violations.isEmpty());
        assertTrue(violations.iterator().next().getMessage().contains("Batch size must be between 1 and 1000 users"));
    }

    @Test
    @DisplayName("Should create BatchCreateRequest with options")
    void testBatchCreateRequestWithOptions() {
        // Given
        List<User> users = Arrays.asList(createUser("user1", "user1@example.com"));
        
        BatchCreateRequest.BatchCreateOptions options = new BatchCreateRequest.BatchCreateOptions();
        options.setFailFast(true);
        options.setBatchSize(50);
        options.setContinueOnFailure(false);
        options.setValidateDuplicates(false);
        
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);
        request.setOptions(options);

        // When/Then
        assertNotNull(request.getOptions());
        assertTrue(request.getOptions().isFailFast());
        assertEquals(50, request.getOptions().getBatchSize());
        assertFalse(request.getOptions().isContinueOnFailure());
        assertFalse(request.getOptions().isValidateDuplicates());
    }

    @Test
    @DisplayName("Should use default values for BatchCreateOptions")
    void testBatchCreateOptionsDefaults() {
        // Given/When
        BatchCreateRequest.BatchCreateOptions options = new BatchCreateRequest.BatchCreateOptions();

        // Then
        assertFalse(options.isFailFast());
        assertEquals(100, options.getBatchSize());
        assertTrue(options.isContinueOnFailure());
        assertTrue(options.isValidateDuplicates());
    }

    @Test
    @DisplayName("Should serialize and deserialize BatchCreateRequest")
    void testBatchCreateRequestSerialization() throws Exception {
        // Given
        List<User> users = Arrays.asList(createUser("user1", "user1@example.com"));
        BatchCreateRequest.BatchCreateOptions options = new BatchCreateRequest.BatchCreateOptions();
        options.setFailFast(true);
        
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);
        request.setOptions(options);

        // When
        String json = objectMapper.writeValueAsString(request);
        BatchCreateRequest deserialized = objectMapper.readValue(json, BatchCreateRequest.class);

        // Then
        assertNotNull(deserialized);
        assertEquals(1, deserialized.getUsers().size());
        assertEquals("user1", deserialized.getUsers().get(0).getUserId());
        assertTrue(deserialized.getOptions().isFailFast());
    }

    @Test
    @DisplayName("Should create valid BatchCreateResponse")
    void testBatchCreateResponse() {
        // Given
        LocalDateTime now = LocalDateTime.now();
        List<String> successIds = Arrays.asList("user1", "user2");
        List<BatchCreateResponse.FailureDetail> failures = Arrays.asList(
            BatchCreateResponse.FailureDetail.builder()
                .userId("user3")
                .index(2)
                .error("Validation failed")
                .errorType("ValidationException")
                .build()
        );
        
        BatchCreateResponse.BatchStatistics statistics = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(1)
            .avgBatchSize(3)
            .avgProcessingTimePerBatch(150L)
            .totalDeltaTransactionTime(150L)
            .deltaTransactionCount(1)
            .additionalMetrics(Map.of("totalMemoryUsed", "128MB"))
            .build();

        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(3)
            .successCount(2)
            .failureCount(1)
            .successfulUserIds(successIds)
            .failures(failures)
            .processedAt(now)
            .processingTimeMs(200L)
            .statistics(statistics)
            .build();

        // When/Then
        assertEquals(3, response.getTotalRequested());
        assertEquals(2, response.getSuccessCount());
        assertEquals(1, response.getFailureCount());
        assertEquals(successIds, response.getSuccessfulUserIds());
        assertEquals(1, response.getFailures().size());
        assertEquals("user3", response.getFailures().get(0).getUserId());
        assertEquals(now, response.getProcessedAt());
        assertEquals(200L, response.getProcessingTimeMs());
        assertNotNull(response.getStatistics());
    }

    @Test
    @DisplayName("Should create FailureDetail with all fields")
    void testFailureDetail() {
        // Given/When
        BatchCreateResponse.FailureDetail failure = BatchCreateResponse.FailureDetail.builder()
            .userId("user123")
            .index(5)
            .error("Database connection timeout")
            .errorType("TimeoutException")
            .build();

        // Then
        assertEquals("user123", failure.getUserId());
        assertEquals(5, failure.getIndex());
        assertEquals("Database connection timeout", failure.getError());
        assertEquals("TimeoutException", failure.getErrorType());
    }

    @Test
    @DisplayName("Should create BatchStatistics with all fields")
    void testBatchStatistics() {
        // Given
        Map<String, Object> additionalMetrics = Map.of(
            "avgRecordSize", "2KB",
            "compressionRatio", 0.85,
            "peakMemoryUsage", "256MB"
        );

        // When
        BatchCreateResponse.BatchStatistics statistics = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(5)
            .avgBatchSize(100)
            .avgProcessingTimePerBatch(300L)
            .totalDeltaTransactionTime(1500L)
            .deltaTransactionCount(5)
            .additionalMetrics(additionalMetrics)
            .build();

        // Then
        assertEquals(5, statistics.getTotalBatches());
        assertEquals(100, statistics.getAvgBatchSize());
        assertEquals(300L, statistics.getAvgProcessingTimePerBatch());
        assertEquals(1500L, statistics.getTotalDeltaTransactionTime());
        assertEquals(5, statistics.getDeltaTransactionCount());
        assertEquals(3, statistics.getAdditionalMetrics().size());
        assertEquals("2KB", statistics.getAdditionalMetrics().get("avgRecordSize"));
    }

    @Test
    @DisplayName("Should serialize and deserialize BatchCreateResponse")
    void testBatchCreateResponseSerialization() throws Exception {
        // Given
        BatchCreateResponse.FailureDetail failure = BatchCreateResponse.FailureDetail.builder()
            .userId("user1")
            .index(0)
            .error("Test error")
            .errorType("TestException")
            .build();
        
        BatchCreateResponse.BatchStatistics statistics = BatchCreateResponse.BatchStatistics.builder()
            .totalBatches(1)
            .avgBatchSize(1)
            .avgProcessingTimePerBatch(100L)
            .totalDeltaTransactionTime(100L)
            .deltaTransactionCount(1)
            .additionalMetrics(Map.of("test", "value"))
            .build();

        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(1)
            .successCount(0)
            .failureCount(1)
            .successfulUserIds(Collections.emptyList())
            .failures(Arrays.asList(failure))
            .processedAt(LocalDateTime.of(2024, 1, 1, 12, 0))
            .processingTimeMs(150L)
            .statistics(statistics)
            .build();

        // When
        String json = objectMapper.writeValueAsString(response);
        BatchCreateResponse deserialized = objectMapper.readValue(json, BatchCreateResponse.class);

        // Then
        assertEquals(1, deserialized.getTotalRequested());
        assertEquals(0, deserialized.getSuccessCount());
        assertEquals(1, deserialized.getFailureCount());
        assertEquals(1, deserialized.getFailures().size());
        assertEquals("user1", deserialized.getFailures().get(0).getUserId());
        assertNotNull(deserialized.getStatistics());
        assertEquals(1, deserialized.getStatistics().getTotalBatches());
    }

    @Test
    @DisplayName("Should handle empty responses")
    void testEmptyBatchCreateResponse() {
        // Given/When
        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(0)
            .successCount(0)
            .failureCount(0)
            .successfulUserIds(Collections.emptyList())
            .failures(Collections.emptyList())
            .processedAt(LocalDateTime.now())
            .processingTimeMs(0L)
            .build();

        // Then
        assertEquals(0, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertTrue(response.getFailures().isEmpty());
        assertNull(response.getStatistics()); // Not set
    }

    @Test
    @DisplayName("Should handle response with errors field")
    void testBatchCreateResponseWithErrors() {
        // Given/When
        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(2)
            .successCount(0)
            .failureCount(0)
            .errors(Arrays.asList("Validation error 1", "Validation error 2"))
            .build();

        // Then
        assertNotNull(response.getErrors());
        assertEquals(2, response.getErrors().size());
        assertEquals("Validation error 1", response.getErrors().get(0));
        assertEquals("Validation error 2", response.getErrors().get(1));
    }

    @Test
    @DisplayName("Should calculate success and failure counts correctly")
    void testBatchCreateResponseCalculations() {
        // Given
        List<String> successIds = Arrays.asList("user1", "user2", "user3");
        List<BatchCreateResponse.FailureDetail> failures = Arrays.asList(
            BatchCreateResponse.FailureDetail.builder().userId("user4").index(3).error("Error").build(),
            BatchCreateResponse.FailureDetail.builder().userId("user5").index(4).error("Error").build()
        );

        // When
        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(5)
            .successCount(successIds.size())
            .failureCount(failures.size())
            .successfulUserIds(successIds)
            .failures(failures)
            .build();

        // Then
        assertEquals(5, response.getTotalRequested());
        assertEquals(3, response.getSuccessCount());
        assertEquals(2, response.getFailureCount());
        assertEquals(response.getSuccessCount() + response.getFailureCount(), response.getTotalRequested());
        assertEquals(successIds.size(), response.getSuccessCount());
        assertEquals(failures.size(), response.getFailureCount());
    }

    @Test
    @DisplayName("Should support fluent builder pattern")
    void testFluentBuilderPattern() {
        // Given/When
        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(10)
            .successCount(8)
            .failureCount(2)
            .successfulUserIds(Arrays.asList("user1", "user2"))
            .failures(Collections.emptyList())
            .processedAt(LocalDateTime.now())
            .processingTimeMs(500L)
            .statistics(BatchCreateResponse.BatchStatistics.builder()
                .totalBatches(2)
                .avgBatchSize(5)
                .build())
            .build();

        // Then
        assertEquals(10, response.getTotalRequested());
        assertEquals(8, response.getSuccessCount());
        assertEquals(2, response.getFailureCount());
        assertNotNull(response.getProcessedAt());
        assertEquals(500L, response.getProcessingTimeMs());
        assertNotNull(response.getStatistics());
        assertEquals(2, response.getStatistics().getTotalBatches());
    }

    private User createUser(String userId, String email) {
        return User.newBuilder()
            .setUserId(userId)
            .setUsername(userId + "_name")
            .setEmail(email)
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();
    }
}