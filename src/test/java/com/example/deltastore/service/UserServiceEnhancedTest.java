package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.schemas.User;
import com.example.deltastore.storage.DeltaTableManager;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

class UserServiceEnhancedTest {

    @Mock
    private DeltaTableManager deltaTableManager;

    @Mock
    private DeltaStoreMetrics metrics;

    @Mock
    private Timer.Sample timerSample;

    private UserServiceImpl userService;
    private User testUser;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        userService = new UserServiceImpl(deltaTableManager, metrics);
        
        // Create test user
        testUser = User.newBuilder()
            .setUserId("test123")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // Mock timer behavior
        when(metrics.startWriteTimer()).thenReturn(timerSample);
        when(metrics.startReadTimer()).thenReturn(timerSample);
        when(metrics.startPartitionReadTimer()).thenReturn(timerSample);
    }

    @Test
    @DisplayName("Should save single user successfully")
    void testSaveUser() {
        // When
        userService.save(testUser);

        // Then
        verify(deltaTableManager).write(eq("users"), argThat(list -> 
            list.size() == 1 && list.get(0) == testUser), eq(testUser.getSchema()));
        verify(metrics).startWriteTimer();
        verify(metrics).recordWriteSuccess("users", 1);
        verify(metrics).stopWriteTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should handle save user failure with proper metrics")
    void testSaveUserFailure() {
        // Given
        RuntimeException exception = new RuntimeException("Write failed");
        doThrow(exception).when(deltaTableManager).write(any(), any(), any());

        // When/Then
        assertThrows(RuntimeException.class, () -> userService.save(testUser));

        verify(metrics).startWriteTimer();
        verify(metrics).recordWriteFailure("users", "RuntimeException");
        verify(metrics).stopWriteTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should find user by ID successfully")
    void testFindByIdSuccess() {
        // Given
        Map<String, Object> userData = Map.of(
            "user_id", "test123",
            "username", "testuser", 
            "email", "test@example.com",
            "country", "US",
            "signup_date", "2024-01-01"
        );
        when(deltaTableManager.read("users", "user_id", "test123"))
            .thenReturn(Optional.of(userData));

        // When
        Optional<User> result = userService.findById("test123");

        // Then
        assertTrue(result.isPresent());
        assertEquals("test123", result.get().getUserId());
        assertEquals("testuser", result.get().getUsername());
        verify(metrics).startReadTimer();
        verify(metrics).recordReadSuccess("users");
        verify(metrics).stopReadTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should return empty when user not found")
    void testFindByIdNotFound() {
        // Given
        when(deltaTableManager.read("users", "user_id", "nonexistent"))
            .thenReturn(Optional.empty());

        // When
        Optional<User> result = userService.findById("nonexistent");

        // Then
        assertFalse(result.isPresent());
        verify(metrics).startReadTimer();
        verify(metrics).recordReadSuccess("users");
        verify(metrics).stopReadTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should handle find by ID failure with proper metrics")
    void testFindByIdFailure() {
        // Given
        RuntimeException exception = new RuntimeException("Read failed");
        when(deltaTableManager.read("users", "user_id", "test123"))
            .thenThrow(exception);

        // When/Then
        assertThrows(RuntimeException.class, () -> userService.findById("test123"));

        verify(metrics).startReadTimer();
        verify(metrics).recordReadFailure("users", "RuntimeException");
        verify(metrics).stopReadTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should find users by partitions successfully")
    void testFindByPartitionsSuccess() {
        // Given
        Map<String, String> partitionFilters = Map.of("country", "US");
        List<Map<String, Object>> userData = Arrays.asList(
            Map.of("user_id", "user1", "username", "user1", "email", "user1@example.com", 
                   "country", "US", "signup_date", "2024-01-01"),
            Map.of("user_id", "user2", "username", "user2", "email", "user2@example.com", 
                   "country", "US", "signup_date", "2024-01-02")
        );
        when(deltaTableManager.readByPartitions("users", partitionFilters))
            .thenReturn(userData);

        // When
        List<User> result = userService.findByPartitions(partitionFilters);

        // Then
        assertEquals(2, result.size());
        assertEquals("user1", result.get(0).getUserId());
        assertEquals("user2", result.get(1).getUserId());
        verify(metrics).startPartitionReadTimer();
        verify(metrics).recordPartitionReadSuccess("users", 2);
        verify(metrics).stopPartitionReadTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should handle partition read failure with proper metrics")
    void testFindByPartitionsFailure() {
        // Given
        Map<String, String> partitionFilters = Map.of("country", "US");
        RuntimeException exception = new RuntimeException("Partition read failed");
        when(deltaTableManager.readByPartitions("users", partitionFilters))
            .thenThrow(exception);

        // When/Then
        assertThrows(RuntimeException.class, () -> userService.findByPartitions(partitionFilters));

        verify(metrics).startPartitionReadTimer();
        verify(metrics).recordPartitionReadFailure("users", "RuntimeException");
        verify(metrics).stopPartitionReadTimer(timerSample, "users");
    }

    @Test
    @DisplayName("Should save small batch successfully")
    void testSaveBatchSmall() {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        assertEquals(2, response.getTotalRequested());
        assertEquals(2, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertEquals(Arrays.asList("user1", "user2"), response.getSuccessfulUserIds());
        assertTrue(response.getFailures().isEmpty());
        assertNotNull(response.getProcessedAt());
        assertTrue(response.getProcessingTimeMs() >= 0);
        
        // Verify statistics
        assertNotNull(response.getStatistics());
        assertEquals(1, response.getStatistics().getTotalBatches());
        assertEquals(2, response.getStatistics().getAvgBatchSize());
        
        verify(deltaTableManager, times(1)).write(eq("users"), any(), any());
        verify(metrics).recordWriteSuccess("users", 2);
    }

    @Test
    @DisplayName("Should handle large batch with chunking")
    void testSaveBatchLarge() {
        // Given
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 250; i++) {
            users.add(createUser("user" + i, "user" + i + "@example.com"));
        }

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        assertEquals(250, response.getTotalRequested());
        assertEquals(250, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        
        // Should be processed in chunks of 100 = 3 batches (100, 100, 50)
        assertEquals(3, response.getStatistics().getTotalBatches());
        assertEquals(83, response.getStatistics().getAvgBatchSize()); // 250/3 = 83.33... rounded
        
        verify(deltaTableManager, times(3)).write(eq("users"), any(), any());
        verify(metrics).recordWriteSuccess("users", 250);
    }

    @Test
    @DisplayName("Should handle partial batch failure")
    void testSaveBatchPartialFailure() {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );
        
        // Mock first chunk to succeed, second chunk to fail (if it existed)
        // For small batch, this will be one chunk that fails completely
        doThrow(new RuntimeException("Write failed")).when(deltaTableManager)
            .write(eq("users"), any(), any());

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        assertEquals(2, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(2, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertEquals(2, response.getFailures().size());
        
        // Check failure details
        assertEquals("user1", response.getFailures().get(0).getUserId());
        assertEquals("user2", response.getFailures().get(1).getUserId());
        assertEquals("Write failed", response.getFailures().get(0).getError());
        assertEquals("RuntimeException", response.getFailures().get(0).getErrorType());
        
        verify(metrics).recordWriteFailure("users", "BatchPartialFailure");
    }

    @Test
    @DisplayName("Should handle complete batch failure")
    void testSaveBatchCompleteFailure() {
        // Given
        List<User> users = Arrays.asList(createUser("user1", "user1@example.com"));
        RuntimeException exception = new RuntimeException("Complete failure");
        
        // Mock the partitioning method to throw exception
        when(metrics.startWriteTimer()).thenThrow(exception);

        // When/Then
        assertThrows(RuntimeException.class, () -> userService.saveBatch(users));
        
        verify(metrics).recordWriteFailure("users", "RuntimeException");
    }

    @Test
    @DisplayName("Should handle empty batch gracefully")
    void testSaveBatchEmpty() {
        // Given
        List<User> emptyUsers = new ArrayList<>();

        // When
        BatchCreateResponse response = userService.saveBatch(emptyUsers);

        // Then
        assertEquals(0, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertTrue(response.getFailures().isEmpty());
        assertEquals(0, response.getStatistics().getTotalBatches());
        
        verify(deltaTableManager, never()).write(any(), any(), any());
    }

    @Test
    @DisplayName("Should handle null user data in mapping")
    void testMapToUserWithNullValues() {
        // Given
        Map<String, Object> userData = Map.of(
            "user_id", "test123",
            "username", "testuser",
            "email", (String) null, // Explicitly null email
            "country", "US",
            "signup_date", "2024-01-01"
        );
        when(deltaTableManager.read("users", "user_id", "test123"))
            .thenReturn(Optional.of(userData));

        // When
        Optional<User> result = userService.findById("test123");

        // Then
        assertTrue(result.isPresent());
        assertEquals("test123", result.get().getUserId());
        assertNull(result.get().getEmail());
    }

    @Test
    @DisplayName("Should validate batch statistics calculations")
    void testBatchStatisticsCalculations() {
        // Given
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 150; i++) {
            users.add(createUser("user" + i, "user" + i + "@example.com"));
        }

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        BatchCreateResponse.BatchStatistics stats = response.getStatistics();
        assertNotNull(stats);
        assertEquals(2, stats.getTotalBatches()); // 150 users = 2 batches (100, 50)
        assertEquals(75, stats.getAvgBatchSize()); // 150/2 = 75
        assertTrue(stats.getTotalDeltaTransactionTime() >= 0);
        assertEquals(2, stats.getDeltaTransactionCount());
        
        // Verify additional metrics
        Map<String, Object> additionalMetrics = stats.getAdditionalMetrics();
        assertEquals(2, additionalMetrics.get("chunksProcessed"));
        assertEquals(100, additionalMetrics.get("averageChunkSize"));
        assertTrue((Long) additionalMetrics.get("totalProcessingTimeMs") >= 0);
    }

    @Test
    @DisplayName("Should handle batch with exactly chunk size boundary")
    void testSaveBatchChunkSizeBoundary() {
        // Given - exactly 100 users (one chunk)
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            users.add(createUser("user" + i, "user" + i + "@example.com"));
        }

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        assertEquals(100, response.getTotalRequested());
        assertEquals(100, response.getSuccessCount());
        assertEquals(1, response.getStatistics().getTotalBatches());
        assertEquals(100, response.getStatistics().getAvgBatchSize());
        
        verify(deltaTableManager, times(1)).write(eq("users"), any(), any());
    }

    @Test
    @DisplayName("Should handle batch with multiple chunk failures")
    void testSaveBatchMultipleChunkFailures() {
        // Given
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= 250; i++) {
            users.add(createUser("user" + i, "user" + i + "@example.com"));
        }
        
        // Mock all chunks to fail
        doThrow(new RuntimeException("Chunk failed")).when(deltaTableManager)
            .write(eq("users"), any(), any());

        // When
        BatchCreateResponse response = userService.saveBatch(users);

        // Then
        assertEquals(250, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(250, response.getFailureCount());
        assertEquals(250, response.getFailures().size());
        
        verify(deltaTableManager, times(3)).write(eq("users"), any(), any());
        verify(metrics).recordWriteFailure("users", "BatchPartialFailure");
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