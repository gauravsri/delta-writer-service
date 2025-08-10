package com.example.deltastore.entity;

import lombok.Builder;
import lombok.Data;

/**
 * Result object for entity operations with comprehensive status information
 */
@Data
@Builder
public class EntityOperationResult<T> {
    private boolean success;
    private String entityType;
    private OperationType operationType;
    private int recordCount;
    private String message;
    private Exception error;
    private T data; // For operations that return data
    private long executionTimeMs;
}