package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.entity.EntityOperationResult;
import com.example.deltastore.entity.GenericEntityService;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.storage.DeltaTableManager;
import io.micrometer.core.instrument.Timer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenericEntityServiceImplIntegrationTest {

    @Mock
    private DeltaTableManager deltaTableManager;
    
    @Mock
    private DeltaStoreMetrics metrics;
    
    @Mock
    private GenericEntityService genericEntityService;
    
    @Mock
    private Timer.Sample timerSample;
    
    private GenericEntityServiceImpl<GenericRecord> service;
    private Schema testSchema;
    private GenericRecord testEntity;
    
    @BeforeEach
    void setUp() {
        service = new GenericEntityServiceImpl<>("users", deltaTableManager, metrics, genericEntityService);
        
        testSchema = Schema.parse("{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\"}]}");
        
        testEntity = new GenericRecordBuilder(testSchema)
            .set("id", "user123")
            .set("name", "John Doe")
            .set("email", "john@example.com")
            .build();
        
        lenient().when(metrics.startWriteTimer()).thenReturn(timerSample);
    }
    
    @Test
    void testConstructor() {
        assertNotNull(service);
        assertEquals("users", service.getEntityType());
    }
    
    @Test
    void testGetEntityType() {
        assertEquals("users", service.getEntityType());
    }
    
    @Test
    void testSaveSuccess() throws Exception {
        // Given
        doNothing().when(deltaTableManager).write(eq("users"), eq(Collections.singletonList(testEntity)), eq(testSchema));
        
        // When
        assertDoesNotThrow(() -> service.save(testEntity));
        
        // Then
        verify(metrics).startWriteTimer();
        verify(deltaTableManager).write("users", Collections.singletonList(testEntity), testSchema);
        verify(metrics).recordWriteSuccess("users", 1L);
        verify(metrics).stopWriteTimer(timerSample, "users");
    }
    
    @Test
    void testSaveFailure() throws Exception {
        // Given
        RuntimeException exception = new RuntimeException("Database error");
        doThrow(exception).when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When & Then
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> service.save(testEntity));
        assertEquals("Database error", thrown.getMessage());
        
        verify(metrics).startWriteTimer();
        verify(deltaTableManager).write("users", Collections.singletonList(testEntity), testSchema);
        verify(metrics).recordWriteFailure("users", "RuntimeException");
        verify(metrics).stopWriteTimer(timerSample, "users");
        verify(metrics, never()).recordWriteSuccess(anyString(), anyLong());
    }
    
    @Test
    void testSaveWithResult() {
        // Given
        EntityOperationResult<GenericRecord> mockResult = mock(EntityOperationResult.class);
        when(genericEntityService.save("users", testEntity)).thenReturn(mockResult);
        
        // When
        EntityOperationResult<GenericRecord> result = service.saveWithResult(testEntity);
        
        // Then
        assertEquals(mockResult, result);
        verify(genericEntityService).save("users", testEntity);
    }
    
    @Test
    void testSaveAllWithResult() {
        // Given
        List<GenericRecord> entities = Arrays.asList(testEntity);
        EntityOperationResult<GenericRecord> mockResult = mock(EntityOperationResult.class);
        when(genericEntityService.saveAll("users", entities)).thenReturn(mockResult);
        
        // When
        EntityOperationResult<GenericRecord> result = service.saveAllWithResult(entities);
        
        // Then
        assertEquals(mockResult, result);
        verify(genericEntityService).saveAll("users", entities);
    }
    
    @Test
    void testSaveBatchSuccess() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(5);
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertNotNull(response);
        assertEquals(5, response.getTotalRequested());
        assertEquals(5, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertEquals(5, response.getSuccessfulUserIds().size());
        assertTrue(response.getFailures().isEmpty());
        assertNotNull(response.getProcessedAt());
        assertTrue(response.getProcessingTimeMs() >= 0);
        assertNotNull(response.getStatistics());
        
        verify(metrics).startWriteTimer();
        verify(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        verify(metrics).recordWriteSuccess("users", 5L);
        verify(metrics).stopWriteTimer(timerSample, "users");
    }
    
    @Test
    void testSaveBatchPartialFailure() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(5);
        RuntimeException exception = new RuntimeException("Partial failure");
        doThrow(exception).when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertNotNull(response);
        assertEquals(5, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(5, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertEquals(5, response.getFailures().size());
        
        // Verify failure details
        BatchCreateResponse.FailureDetail firstFailure = response.getFailures().get(0);
        assertEquals("user0", firstFailure.getUserId());
        assertEquals(0, firstFailure.getIndex());
        assertEquals("Partial failure", firstFailure.getError());
        assertEquals("RuntimeException", firstFailure.getErrorType());
        
        verify(metrics).startWriteTimer();
        verify(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        verify(metrics).recordWriteFailure("users", "BatchPartialFailure");
        verify(metrics).stopWriteTimer(timerSample, "users");
    }
    
    @Test
    void testSaveBatchCompleteFailure() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(3);
        RuntimeException exception = new RuntimeException("Complete failure");
        doThrow(exception).when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then - saveBatch handles failures gracefully and returns response
        assertNotNull(response);
        assertEquals(3, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(3, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertEquals(3, response.getFailures().size());
        
        // Verify failure details
        BatchCreateResponse.FailureDetail firstFailure = response.getFailures().get(0);
        assertEquals("user0", firstFailure.getUserId());
        assertEquals(0, firstFailure.getIndex());
        assertEquals("Complete failure", firstFailure.getError());
        assertEquals("RuntimeException", firstFailure.getErrorType());
        
        verify(metrics).startWriteTimer();
        verify(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        verify(metrics).recordWriteSuccess("users", 0L);
        verify(metrics).recordWriteFailure("users", "BatchPartialFailure");
        verify(metrics).stopWriteTimer(timerSample, "users");
    }
    
    @Test
    void testSaveBatchLargeDataset() throws Exception {
        // Given - more than chunk size (100)
        List<GenericRecord> entities = createTestEntities(150);
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertNotNull(response);
        assertEquals(150, response.getTotalRequested());
        assertEquals(150, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertEquals(150, response.getSuccessfulUserIds().size());
        
        // Should have processed in 2 chunks (100 + 50)
        BatchCreateResponse.BatchStatistics stats = response.getStatistics();
        assertEquals(2, stats.getTotalBatches());
        assertEquals(2, stats.getDeltaTransactionCount());
        assertTrue(stats.getTotalDeltaTransactionTime() >= 0);
        
        // Should have called deltaTableManager.write twice (for each chunk)
        verify(deltaTableManager, times(2)).write(eq("users"), any(), eq(testSchema));
        verify(metrics).recordWriteSuccess("users", 150L);
    }
    
    @Test
    void testSaveBatchEmptyList() throws Exception {
        // Given
        List<GenericRecord> entities = Collections.emptyList();
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertNotNull(response);
        assertEquals(0, response.getTotalRequested());
        assertEquals(0, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        assertTrue(response.getSuccessfulUserIds().isEmpty());
        assertTrue(response.getFailures().isEmpty());
        
        verify(deltaTableManager, never()).write(any(), any(), any());
        verify(metrics).recordWriteSuccess("users", 0L);
    }
    
    @Test
    void testSaveBatchWithMixedChunkResults() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(150);
        
        // First call succeeds, second call fails
        doNothing()
            .doThrow(new RuntimeException("Second chunk failed"))
            .when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertNotNull(response);
        assertEquals(150, response.getTotalRequested());
        assertEquals(100, response.getSuccessCount()); // Only first chunk succeeded
        assertEquals(50, response.getFailureCount()); // Second chunk failed
        assertEquals(100, response.getSuccessfulUserIds().size());
        assertEquals(50, response.getFailures().size());
        
        // Verify failure details for second chunk
        BatchCreateResponse.FailureDetail firstFailureInSecondChunk = response.getFailures().get(0);
        assertEquals("user100", firstFailureInSecondChunk.getUserId());
        assertEquals(100, firstFailureInSecondChunk.getIndex());
        assertEquals("Second chunk failed", firstFailureInSecondChunk.getError());
        
        verify(deltaTableManager, times(2)).write(eq("users"), any(), eq(testSchema));
        verify(metrics).recordWriteSuccess("users", 100L);
        verify(metrics).recordWriteFailure("users", "BatchPartialFailure");
    }
    
    @Test
    void testExtractEntityIdWithIdField() throws Exception {
        // Given
        GenericRecord entityWithId = new GenericRecordBuilder(testSchema)
            .set("id", "custom123")
            .set("name", "Test User")
            .set("email", "test@example.com")
            .build();
        
        List<GenericRecord> entities = Arrays.asList(entityWithId);
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertEquals("custom123", response.getSuccessfulUserIds().get(0));
    }
    
    @Test
    void testExtractEntityIdWithUserIdField() throws Exception {
        // Given
        Schema schemaWithUserId = Schema.parse("{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"userId\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}");
        GenericRecord entityWithUserId = new GenericRecordBuilder(schemaWithUserId)
            .set("userId", "userId123")
            .set("name", "Test User")
            .build();
        
        List<GenericRecord> entities = Arrays.asList(entityWithUserId);
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(schemaWithUserId));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertEquals("userId123", response.getSuccessfulUserIds().get(0));
    }
    
    @Test
    void testExtractEntityIdFallbackToIndex() throws Exception {
        // Given
        Schema schemaWithoutId = Schema.parse("{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":\"string\"}]}");
        GenericRecord entityWithoutId = new GenericRecordBuilder(schemaWithoutId)
            .set("name", "Test User")
            .set("email", "test@example.com")
            .build();
        
        List<GenericRecord> entities = Arrays.asList(entityWithoutId, entityWithoutId);
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(schemaWithoutId));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        assertEquals("users_0", response.getSuccessfulUserIds().get(0));
        assertEquals("users_1", response.getSuccessfulUserIds().get(1));
    }
    
    @Test
    void testBatchStatisticsCalculation() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(75); // Will create 1 chunk
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        BatchCreateResponse.BatchStatistics stats = response.getStatistics();
        assertNotNull(stats);
        assertEquals(1, stats.getTotalBatches());
        assertEquals(75, stats.getAvgBatchSize()); // 75/1
        assertTrue(stats.getAvgProcessingTimePerBatch() >= 0);
        assertTrue(stats.getTotalDeltaTransactionTime() >= 0);
        assertEquals(1, stats.getDeltaTransactionCount());
        
        assertNotNull(stats.getAdditionalMetrics());
        assertEquals(1, stats.getAdditionalMetrics().get("chunksProcessed"));
        assertEquals(100, stats.getAdditionalMetrics().get("averageChunkSize"));
        assertEquals("users", stats.getAdditionalMetrics().get("entityType"));
        assertTrue((Long) stats.getAdditionalMetrics().get("totalProcessingTimeMs") >= 0);
    }
    
    @Test
    void testBatchStatisticsWithMultipleChunks() throws Exception {
        // Given
        List<GenericRecord> entities = createTestEntities(250); // Will create 3 chunks
        doNothing().when(deltaTableManager).write(eq("users"), any(), eq(testSchema));
        
        // When
        BatchCreateResponse response = service.saveBatch(entities);
        
        // Then
        BatchCreateResponse.BatchStatistics stats = response.getStatistics();
        assertEquals(3, stats.getTotalBatches());
        assertEquals(83, stats.getAvgBatchSize()); // 250/3 = 83 (rounded down)
        assertEquals(3, stats.getDeltaTransactionCount());
        assertEquals(3, stats.getAdditionalMetrics().get("chunksProcessed"));
    }
    
    private List<GenericRecord> createTestEntities(int count) {
        List<GenericRecord> entities = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            GenericRecord entity = new GenericRecordBuilder(testSchema)
                .set("id", "user" + i)
                .set("name", "User " + i)
                .set("email", "user" + i + "@example.com")
                .build();
            entities.add(entity);
        }
        return entities;
    }
}