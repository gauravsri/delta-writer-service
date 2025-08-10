package com.example.deltastore.exception;

public class TableWriteException extends DeltaStoreException {
    public TableWriteException(String message) {
        super(message);
    }

    public TableWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}