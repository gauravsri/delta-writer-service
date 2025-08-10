package com.example.deltastore.exception;

public class DeltaStoreException extends RuntimeException {
    public DeltaStoreException(String message) {
        super(message);
    }

    public DeltaStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}