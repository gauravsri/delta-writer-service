package com.example.deltastore.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeltaStoreExceptionTest {

    @Test
    void testConstructorWithMessage() {
        String message = "Test exception message";
        DeltaStoreException exception = new DeltaStoreException(message);
        
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testConstructorWithMessageAndCause() {
        String message = "Test exception message";
        RuntimeException cause = new RuntimeException("Root cause");
        DeltaStoreException exception = new DeltaStoreException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testConstructorWithCause() {
        RuntimeException cause = new RuntimeException("Root cause");
        DeltaStoreException exception = new DeltaStoreException(cause);
        
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.RuntimeException: Root cause", exception.getMessage());
    }

    @Test
    void testTableReadException() {
        String message = "Failed to read table";
        TableReadException exception = new TableReadException(message);
        
        assertEquals(message, exception.getMessage());
        assertTrue(exception instanceof DeltaStoreException);
    }

    @Test
    void testTableWriteException() {
        String message = "Failed to write table";
        TableWriteException exception = new TableWriteException(message);
        
        assertEquals(message, exception.getMessage());
        assertTrue(exception instanceof DeltaStoreException);
    }

    @Test
    void testTableReadExceptionWithCause() {
        String message = "Failed to read table";
        RuntimeException cause = new RuntimeException("IO error");
        TableReadException exception = new TableReadException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testTableWriteExceptionWithCause() {
        String message = "Failed to write table";
        RuntimeException cause = new RuntimeException("Storage error");
        TableWriteException exception = new TableWriteException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}