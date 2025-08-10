package com.example.deltastore.exception;

public class TableReadException extends DeltaStoreException {
    public TableReadException(String message) {
        super(message);
    }

    public TableReadException(String message, Throwable cause) {
        super(message, cause);
    }
}