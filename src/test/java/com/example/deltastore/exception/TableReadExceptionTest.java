package com.example.deltastore.exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

class TableReadExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void testExceptionWithMessage() {
        // Given
        String message = "Failed to read from Delta table";
        
        // When
        TableReadException exception = new TableReadException(message);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testExceptionWithMessageAndCause() {
        // Given
        String message = "Failed to read from Delta table";
        Throwable cause = new RuntimeException("Connection error");
        
        // When
        TableReadException exception = new TableReadException(message, cause);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with null message")
    void testExceptionWithNullMessage() {
        // When
        TableReadException exception = new TableReadException(null);
        
        // Then
        assertNotNull(exception);
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with null cause")
    void testExceptionWithNullCause() {
        // Given
        String message = "Failed to read";
        
        // When
        TableReadException exception = new TableReadException(message, null);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should extend DeltaStoreException")
    void testExtendsCorrectException() {
        // Given
        TableReadException exception = new TableReadException("Test");
        
        // Then
        assertTrue(exception instanceof DeltaStoreException);
        assertTrue(exception instanceof RuntimeException);
    }

    @Test
    @DisplayName("Should be throwable")
    void testIsThrowable() {
        // When/Then
        assertThrows(TableReadException.class, () -> {
            throw new TableReadException("Test exception");
        });
    }

    @Test
    @DisplayName("Should preserve stack trace")
    void testStackTrace() {
        // Given
        TableReadException exception = new TableReadException("Test");
        
        // When
        StackTraceElement[] stackTrace = exception.getStackTrace();
        
        // Then
        assertNotNull(stackTrace);
        assertTrue(stackTrace.length > 0);
    }

    @Test
    @DisplayName("Should handle chained exceptions")
    void testChainedExceptions() {
        // Given
        Exception rootCause = new IllegalStateException("Invalid state");
        RuntimeException middleCause = new RuntimeException("Middle layer error", rootCause);
        TableReadException topException = new TableReadException("Read failed", middleCause);
        
        // Then
        assertEquals("Read failed", topException.getMessage());
        assertEquals(middleCause, topException.getCause());
        assertEquals(rootCause, middleCause.getCause());
    }

    @Test
    @DisplayName("Should support custom error messages with table info")
    void testCustomErrorMessages() {
        // Given
        String tableName = "orders";
        String partition = "date=2024-01-01";
        String detailedMessage = String.format("Failed to read table '%s' partition '%s'", tableName, partition);
        
        // When
        TableReadException exception = new TableReadException(detailedMessage);
        
        // Then
        assertTrue(exception.getMessage().contains(tableName));
        assertTrue(exception.getMessage().contains(partition));
    }

    @Test
    @DisplayName("Should be catchable as DeltaStoreException")
    void testCatchAsDeltaStoreException() {
        // Given
        boolean caught = false;
        
        // When
        try {
            throw new TableReadException("Test");
        } catch (DeltaStoreException e) {
            caught = true;
        }
        
        // Then
        assertTrue(caught);
    }

    @Test
    @DisplayName("Should differentiate from TableWriteException")
    void testDifferentFromWriteException() {
        // Given
        TableReadException readException = new TableReadException("Read error");
        TableWriteException writeException = new TableWriteException("Write error");
        
        // Then
        assertNotEquals(readException.getClass(), writeException.getClass());
        // Both extend DeltaStoreException but are different types
        assertTrue(readException instanceof DeltaStoreException);
        assertTrue(writeException instanceof DeltaStoreException);
    }

    @Test
    @DisplayName("Should handle query timeout scenarios")
    void testQueryTimeoutScenario() {
        // Given
        Exception timeoutCause = new RuntimeException("Query timeout after 30000ms");
        TableReadException exception = new TableReadException("Read operation timed out", timeoutCause);
        
        // Then
        assertTrue(exception.getMessage().contains("timed out"));
        assertTrue(exception.getCause().getMessage().contains("30000ms"));
    }

    @Test
    @DisplayName("Should handle partition not found scenarios")
    void testPartitionNotFoundScenario() {
        // Given
        String errorMessage = "Partition 'country=US/year=2024' not found in table 'users'";
        TableReadException exception = new TableReadException(errorMessage);
        
        // Then
        assertTrue(exception.getMessage().contains("Partition"));
        assertTrue(exception.getMessage().contains("not found"));
    }

    @Test
    @DisplayName("Should handle corrupted data scenarios")
    void testCorruptedDataScenario() {
        // Given
        Exception corruptionCause = new RuntimeException("Corrupted parquet file");
        TableReadException exception = new TableReadException("Failed to read corrupted data", corruptionCause);
        
        // Then
        assertTrue(exception.getMessage().contains("corrupted"));
        assertNotNull(exception.getCause());
    }

    @Test
    @DisplayName("Should compare different instances")
    void testDifferentInstances() {
        // Given
        TableReadException exception1 = new TableReadException("Error 1");
        TableReadException exception2 = new TableReadException("Error 2");
        TableReadException exception3 = new TableReadException("Error 1");
        
        // Then
        assertNotEquals(exception1, exception2);
        assertNotEquals(exception1, exception3); // Different instances even with same message
        assertEquals(exception1.getMessage(), exception3.getMessage());
    }
}