package com.example.deltastore.exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

class TableWriteExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void testExceptionWithMessage() {
        // Given
        String message = "Failed to write to Delta table";
        
        // When
        TableWriteException exception = new TableWriteException(message);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testExceptionWithMessageAndCause() {
        // Given
        String message = "Failed to write to Delta table";
        Throwable cause = new RuntimeException("Underlying error");
        
        // When
        TableWriteException exception = new TableWriteException(message, cause);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with null message")
    void testExceptionWithNullMessage() {
        // When
        TableWriteException exception = new TableWriteException(null);
        
        // Then
        assertNotNull(exception);
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with null cause")
    void testExceptionWithNullCause() {
        // Given
        String message = "Failed to write";
        
        // When
        TableWriteException exception = new TableWriteException(message, null);
        
        // Then
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should extend DeltaStoreException")
    void testExtendsCorrectException() {
        // Given
        TableWriteException exception = new TableWriteException("Test");
        
        // Then
        assertTrue(exception instanceof DeltaStoreException);
        assertTrue(exception instanceof RuntimeException);
    }

    @Test
    @DisplayName("Should be throwable")
    void testIsThrowable() {
        // When/Then
        assertThrows(TableWriteException.class, () -> {
            throw new TableWriteException("Test exception");
        });
    }

    @Test
    @DisplayName("Should preserve stack trace")
    void testStackTrace() {
        // Given
        TableWriteException exception = new TableWriteException("Test");
        
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
        Exception rootCause = new IllegalArgumentException("Invalid argument");
        RuntimeException middleCause = new RuntimeException("Middle layer error", rootCause);
        TableWriteException topException = new TableWriteException("Write failed", middleCause);
        
        // Then
        assertEquals("Write failed", topException.getMessage());
        assertEquals(middleCause, topException.getCause());
        assertEquals(rootCause, middleCause.getCause());
    }

    @Test
    @DisplayName("Should support custom error messages")
    void testCustomErrorMessages() {
        // Given
        String tableName = "users";
        String operation = "INSERT";
        String detailedMessage = String.format("Failed to %s into table '%s'", operation, tableName);
        
        // When
        TableWriteException exception = new TableWriteException(detailedMessage);
        
        // Then
        assertTrue(exception.getMessage().contains(tableName));
        assertTrue(exception.getMessage().contains(operation));
    }

    @Test
    @DisplayName("Should be catchable as DeltaStoreException")
    void testCatchAsDeltaStoreException() {
        // Given
        boolean caught = false;
        
        // When
        try {
            throw new TableWriteException("Test");
        } catch (DeltaStoreException e) {
            caught = true;
        }
        
        // Then
        assertTrue(caught);
    }

    @Test
    @DisplayName("Should compare different instances")
    void testDifferentInstances() {
        // Given
        TableWriteException exception1 = new TableWriteException("Error 1");
        TableWriteException exception2 = new TableWriteException("Error 2");
        
        // Then
        assertNotEquals(exception1, exception2);
        assertNotEquals(exception1.getMessage(), exception2.getMessage());
    }

    @Test
    @DisplayName("Should handle very long error messages")
    void testLongErrorMessage() {
        // Given
        String longMessage = "Error: " + "x".repeat(10000);
        
        // When
        TableWriteException exception = new TableWriteException(longMessage);
        
        // Then
        assertEquals(longMessage, exception.getMessage());
        assertEquals(10007, exception.getMessage().length());
    }
}