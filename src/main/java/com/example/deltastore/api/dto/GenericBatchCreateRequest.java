package com.example.deltastore.api.dto;

import lombok.Data;
import org.apache.avro.generic.GenericRecord;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Generic batch create request that can handle any entity type.
 * This replaces the hardcoded User-specific BatchCreateRequest.
 * 
 * @param <T> The entity type that extends GenericRecord (User, Product, Order, etc.)
 */
@Data
public class GenericBatchCreateRequest<T extends GenericRecord> {
    
    @NotEmpty(message = "Entity list cannot be empty")
    @Size(min = 1, max = 1000, message = "Batch size must be between 1 and 1000 entities")
    @Valid
    private List<T> entities;
    
    private BatchCreateOptions options;
    
    @Data
    public static class BatchCreateOptions {
        // Fail fast - stop on first error (default: false)
        private boolean failFast = false;
        
        // Batch size for processing - split large batches into smaller chunks
        private Integer batchSize = 100;
        
        // Continue on individual failures (default: true)
        private boolean continueOnFailure = true;
        
        // Validate duplicates within batch (default: true)
        private boolean validateDuplicates = true;
    }
}