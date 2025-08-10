package com.example.deltastore.entity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EntityOperationResultTest {

    @Test
    void testSuccessfulResult() {
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Entity saved successfully")
            .executionTimeMs(245L)
            .data("test-data")
            .build();

        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        assertEquals("Entity saved successfully", result.getMessage());
        assertEquals(245L, result.getExecutionTimeMs());
        assertEquals("test-data", result.getData());
    }

    @Test
    void testFailureResult() {
        RuntimeException exception = new RuntimeException("Record with ID 'user123' not found");
        
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(false)
            .entityType("users")
            .operationType(OperationType.READ)
            .recordCount(0)
            .message("Entity not found")
            .executionTimeMs(50L)
            .error(exception)
            .build();

        assertFalse(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.READ, result.getOperationType());
        assertEquals(0, result.getRecordCount());
        assertEquals("Entity not found", result.getMessage());
        assertEquals(50L, result.getExecutionTimeMs());
        assertEquals(exception, result.getError());
        assertEquals("Record with ID 'user123' not found", result.getError().getMessage());
        assertNull(result.getData());
    }

    @Test
    void testBatchOperationResult() {
        List<String> batchData = List.of("item1", "item2", "item3");
        
        EntityOperationResult<List<String>> result = EntityOperationResult.<List<String>>builder()
            .success(true)
            .entityType("products")
            .operationType(OperationType.BATCH_WRITE)
            .recordCount(3)
            .message("Batch operation completed")
            .executionTimeMs(500L)
            .data(batchData)
            .build();

        assertTrue(result.isSuccess());
        assertEquals("products", result.getEntityType());
        assertEquals(OperationType.BATCH_WRITE, result.getOperationType());
        assertEquals(3, result.getRecordCount());
        assertEquals("Batch operation completed", result.getMessage());
        assertEquals(500L, result.getExecutionTimeMs());
        assertEquals(batchData, result.getData());
        assertEquals(3, result.getData().size());
    }

    @Test
    void testMinimalResult() {
        EntityOperationResult<Object> result = EntityOperationResult.builder()
            .success(true)
            .operationType(OperationType.READ)
            .build();

        assertTrue(result.isSuccess());
        assertEquals(OperationType.READ, result.getOperationType());
        assertNull(result.getEntityType());
        assertEquals(0, result.getRecordCount()); // Default int value
        assertNull(result.getMessage());
        assertEquals(0L, result.getExecutionTimeMs()); // Default long value
        assertNull(result.getData());
        assertNull(result.getError());
    }

    @Test
    void testDeleteOperationResult() {
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.DELETE)
            .recordCount(1)
            .message("Entity deleted successfully")
            .executionTimeMs(180L)
            .data("user123")
            .build();

        assertTrue(result.isSuccess());
        assertEquals(OperationType.DELETE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        assertEquals("Entity deleted successfully", result.getMessage());
        assertEquals("user123", result.getData());
    }

    @Test
    void testUpdateOperationResult() {
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.UPDATE)
            .recordCount(1)
            .message("Entity updated successfully")
            .executionTimeMs(220L)
            .data("updated-user-data")
            .build();

        assertTrue(result.isSuccess());
        assertEquals(OperationType.UPDATE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        assertEquals("Entity updated successfully", result.getMessage());
        assertEquals("updated-user-data", result.getData());
    }

    @Test
    void testBatchReadOperationResult() {
        List<String> searchResults = List.of("user1", "user2", "user3");
        
        EntityOperationResult<List<String>> result = EntityOperationResult.<List<String>>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.BATCH_READ)
            .recordCount(3)
            .message("Batch read completed")
            .executionTimeMs(125L)
            .data(searchResults)
            .build();

        assertTrue(result.isSuccess());
        assertEquals(OperationType.BATCH_READ, result.getOperationType());
        assertEquals(3, result.getRecordCount());
        assertEquals(searchResults, result.getData());
    }

    @Test
    void testResultWithNullData() {
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(false)
            .entityType("users")
            .operationType(OperationType.READ)
            .recordCount(0)
            .message("No data found")
            .executionTimeMs(25L)
            .data(null)
            .build();

        assertFalse(result.isSuccess());
        assertNull(result.getData());
        assertEquals(0, result.getRecordCount());
        assertEquals("No data found", result.getMessage());
    }

    @Test
    void testExecutionTimeCalculation() {
        long startTime = System.currentTimeMillis();
        
        // Simulate some processing time
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(true)
            .operationType(OperationType.WRITE)
            .executionTimeMs(executionTime)
            .build();

        assertTrue(result.getExecutionTimeMs() >= 10L);
    }

    @Test
    void testAllOperationTypes() {
        for (OperationType opType : OperationType.values()) {
            EntityOperationResult<String> result = EntityOperationResult.<String>builder()
                .success(true)
                .operationType(opType)
                .recordCount(1)
                .build();

            assertEquals(opType, result.getOperationType());
            assertTrue(result.isSuccess());
        }
    }

    @Test
    void testToString() {
        EntityOperationResult<String> result = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Success")
            .build();

        String toString = result.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("users"));
        assertTrue(toString.contains("WRITE"));
        assertTrue(toString.contains("Success"));
    }

    @Test
    void testEqualsAndHashCode() {
        EntityOperationResult<String> result1 = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Success")
            .build();

        EntityOperationResult<String> result2 = EntityOperationResult.<String>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Success")
            .build();

        EntityOperationResult<String> result3 = EntityOperationResult.<String>builder()
            .success(false)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(0)
            .message("Failed")
            .build();

        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
        
        assertNotEquals(result1, result3);
        assertNotEquals(result1.hashCode(), result3.hashCode());
    }
}