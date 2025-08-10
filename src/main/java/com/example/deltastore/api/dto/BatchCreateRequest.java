package com.example.deltastore.api.dto;

import com.example.deltastore.schemas.User;
import lombok.Data;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

@Data
public class BatchCreateRequest {
    
    @NotEmpty(message = "Users list cannot be empty")
    @Size(min = 1, max = 1000, message = "Batch size must be between 1 and 1000 users")
    @Valid
    private List<User> users;
    
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