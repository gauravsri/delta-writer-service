package com.example.deltastore.entity;

/**
 * Enumeration of entity operation types
 */
public enum OperationType {
    WRITE,
    READ,
    UPDATE,
    DELETE,
    BATCH_WRITE,
    BATCH_READ
}