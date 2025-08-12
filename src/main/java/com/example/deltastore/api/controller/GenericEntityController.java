package com.example.deltastore.api.controller;

import com.example.deltastore.api.dto.GenericBatchCreateRequest;
import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.service.EntityService;
import com.example.deltastore.validation.EntityValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * Generic controller that can handle any entity type without requiring entity-specific controllers.
 * Routes are: /api/v1/entities/{entityType} where entityType = "users", "products", "orders", etc.
 * 
 * New entity types can be added with only configuration - no new controller code needed!
 */
@RestController
@RequestMapping("/api/v1/entities")
@RequiredArgsConstructor
@Slf4j
@Profile("local")
@Tag(name = "Generic Entity Operations", description = "Universal API for managing any entity type with zero-code configuration")
public class GenericEntityController {

    private final EntityControllerRegistry controllerRegistry;
    private final ObjectMapper objectMapper;

    /**
     * Create a single entity
     * POST /api/v1/entities/{entityType}
     */
    @Operation(
            summary = "Create a single entity",
            description = "Creates a single entity of the specified type. The entity data should match the Avro schema for the entity type."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Entity created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid request data or validation errors",
                    content = @Content(mediaType = "application/json", 
                            examples = @ExampleObject(value = "{\"error\": \"Entity type 'invalid' is not supported\"}"))),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/{entityType}")
    public ResponseEntity<?> createEntity(
            @Parameter(description = "The type of entity to create (e.g., users, orders, products)", example = "users")
            @PathVariable 
            @Pattern(regexp = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$", message = "Entity type must start with letter and contain only alphanumeric characters and underscores (max 64 chars)")
            String entityType,
            @Parameter(description = "Entity data as JSON object matching the entity's Avro schema")
            @RequestBody 
            @Valid 
            @Size(min = 1, max = 1000, message = "Entity data must contain between 1 and 1000 fields")
            Map<String, Object> entityData) {
        
        log.info("Creating {} entity", entityType);
        
        // ENHANCED INPUT VALIDATION
        try {
            // Validate entity type format
            List<String> validationErrors = new ArrayList<>();
            validateEntityType(entityType, validationErrors);
            
            // Validate entity data content
            validateEntityData(entityData, validationErrors);
            
            if (!validationErrors.isEmpty()) {
                log.warn("Input validation failed for {}: {}", entityType, validationErrors);
                return ResponseEntity.badRequest().body(Map.of("errors", validationErrors));
            }
            
            // Check if entity type is supported
            EntityControllerConfig config = controllerRegistry.getConfig(entityType);
            if (config == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Entity type '" + entityType + "' is not supported"));
            }

            // Convert Map to GenericRecord with error handling
            GenericRecord entity;
            try {
                entity = config.getEntityConverter().convertFromMap(entityData);
            } catch (Exception e) {
                log.warn("Failed to convert entity data for {}: {}", entityType, e.getMessage());
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Invalid entity data format: " + e.getMessage()));
            }
            
            // Validate entity using domain validation rules
            List<String> domainValidationErrors = config.getEntityValidator().validate(entityType, entity);
            if (!domainValidationErrors.isEmpty()) {
                log.warn("{} validation failed with errors: {}", entityType, domainValidationErrors);
                return ResponseEntity.badRequest().body(Map.of("errors", domainValidationErrors));
            }
            
            // Save entity
            config.getEntityService().save(entity);
            log.info("Successfully created {} entity", entityType);
            
            return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("message", "Entity created successfully"));
            
        } catch (IllegalArgumentException e) {
            log.warn("Invalid input for creating {} entity: {}", entityType, e.getMessage());
            return ResponseEntity.badRequest()
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to create {} entity", entityType, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to create " + entityType + " entity: " + e.getMessage()));
        }
    }
    
    /**
     * Validates entity type format and constraints
     */
    private void validateEntityType(String entityType, List<String> errors) {
        if (entityType == null || entityType.trim().isEmpty()) {
            errors.add("Entity type cannot be null or empty");
            return;
        }
        
        String trimmed = entityType.trim();
        
        // Length validation
        if (trimmed.length() > 64) {
            errors.add("Entity type is too long (max 64 characters), got: " + trimmed.length());
        }
        
        // Format validation (already covered by @Pattern, but double-check for security)
        if (!trimmed.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
            errors.add("Entity type must start with a letter and contain only alphanumeric characters and underscores");
        }
        
        // Reserved keywords check
        List<String> reservedKeywords = List.of("admin", "system", "internal", "null", "undefined");
        if (reservedKeywords.contains(trimmed.toLowerCase())) {
            errors.add("Entity type '" + trimmed + "' is a reserved keyword");
        }
    }
    
    /**
     * Validates entity data content and structure
     */
    private void validateEntityData(Map<String, Object> entityData, List<String> errors) {
        if (entityData == null) {
            errors.add("Entity data cannot be null");
            return;
        }
        
        if (entityData.isEmpty()) {
            errors.add("Entity data cannot be empty");
            return;
        }
        
        // Check data size limits
        if (entityData.size() > 1000) {
            errors.add("Too many fields in entity data (max 1000), got: " + entityData.size());
        }
        
        // Validate field names and values
        int totalDataSize = 0;
        for (Map.Entry<String, Object> entry : entityData.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            
            // Field name validation
            if (fieldName == null || fieldName.trim().isEmpty()) {
                errors.add("Field name cannot be null or empty");
                continue;
            }
            
            if (fieldName.length() > 255) {
                errors.add("Field name is too long (max 255 chars): " + fieldName);
            }
            
            if (!fieldName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
                errors.add("Invalid field name '" + fieldName + "': must start with letter or underscore and contain only alphanumeric characters and underscores");
            }
            
            // Value validation
            if (value != null) {
                // Check string value length
                if (value instanceof String) {
                    String strValue = (String) value;
                    if (strValue.length() > 10000) {
                        errors.add("String field '" + fieldName + "' is too long (max 10000 chars), got: " + strValue.length());
                    }
                    totalDataSize += strValue.length();
                }
                
                // Check for nested objects depth (prevent deeply nested JSON attacks)
                if (value instanceof Map) {
                    int depth = calculateMapDepth(value, 0);
                    if (depth > 10) {
                        errors.add("Field '" + fieldName + "' has too much nesting (max 10 levels), got: " + depth);
                    }
                }
                
                // Estimate total data size
                totalDataSize += estimateObjectSize(value);
            }
        }
        
        // Total entity size check (rough estimate)
        if (totalDataSize > 1_000_000) { // 1MB limit
            errors.add("Entity data is too large (max 1MB), estimated size: " + (totalDataSize / 1024) + "KB");
        }
    }
    
    /**
     * Calculates nesting depth of Map objects
     */
    private int calculateMapDepth(Object obj, int currentDepth) {
        if (currentDepth > 10) return currentDepth; // Prevent stack overflow
        
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            int maxDepth = currentDepth;
            for (Object value : map.values()) {
                if (value instanceof Map) {
                    maxDepth = Math.max(maxDepth, calculateMapDepth(value, currentDepth + 1));
                }
            }
            return maxDepth + 1;
        }
        return currentDepth;
    }
    
    /**
     * Estimates memory size of an object
     */
    private int estimateObjectSize(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof String) return ((String) obj).length() * 2; // UTF-16
        if (obj instanceof Number) return 8; // Rough estimate
        if (obj instanceof Boolean) return 1;
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            int size = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size += estimateObjectSize(entry.getKey());
                size += estimateObjectSize(entry.getValue());
            }
            return size;
        }
        if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            int size = 0;
            for (Object item : list) {
                size += estimateObjectSize(item);
            }
            return size;
        }
        return obj.toString().length(); // Fallback
    }
    
    /**
     * Validates batch request structure and constraints
     */
    private void validateBatchRequest(GenericBatchCreateRequest<GenericRecord> request, List<String> errors) {
        if (request == null) {
            errors.add("Batch request cannot be null");
            return;
        }
        
        if (request.getEntities() == null) {
            errors.add("Entities list cannot be null");
            return;
        }
        
        if (request.getEntities().isEmpty()) {
            errors.add("Entities list cannot be empty");
            return;
        }
        
        // Batch size validation
        int batchSize = request.getEntities().size();
        if (batchSize > 1000) {
            errors.add("Batch size too large (max 1000), got: " + batchSize);
        }
        
        // Check for null entities in the batch
        for (int i = 0; i < request.getEntities().size(); i++) {
            if (request.getEntities().get(i) == null) {
                errors.add("Entity at index " + i + " cannot be null");
            }
        }
        
        // Estimate total batch data size
        if (batchSize > 100) { // Only check for large batches to avoid performance impact
            long estimatedSize = estimateBatchSize(request.getEntities());
            if (estimatedSize > 10_000_000) { // 10MB limit
                errors.add("Batch data is too large (max 10MB), estimated size: " + (estimatedSize / 1024 / 1024) + "MB");
            }
        }
    }
    
    /**
     * Estimates total size of batch request
     */
    private long estimateBatchSize(List<GenericRecord> entities) {
        long totalSize = 0;
        int sampleSize = Math.min(10, entities.size()); // Sample first 10 entities
        
        for (int i = 0; i < sampleSize; i++) {
            GenericRecord entity = entities.get(i);
            if (entity != null) {
                // Rough estimate based on string representation
                totalSize += entity.toString().length();
            }
        }
        
        // Extrapolate to full batch
        return (totalSize / sampleSize) * entities.size();
    }

    /**
     * Create multiple entities in batch
     * POST /api/v1/entities/{entityType}/batch
     */
    @Operation(
            summary = "Create multiple entities in batch",
            description = "Creates multiple entities of the specified type in a single batch operation for improved performance. Supports up to 1000 entities per batch."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "All entities created successfully",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = BatchCreateResponse.class))),
            @ApiResponse(responseCode = "206", description = "Partial success - some entities failed",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = BatchCreateResponse.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request data or validation errors"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/{entityType}/batch")
    public ResponseEntity<?> createEntitiesBatch(
            @Parameter(description = "The type of entity to create (e.g., users, orders, products)", example = "users")
            @PathVariable 
            @Pattern(regexp = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$", message = "Entity type must start with letter and contain only alphanumeric characters and underscores (max 64 chars)")
            String entityType,
            @Parameter(description = "Batch create request containing array of entities and optional processing options")
            @Valid @RequestBody GenericBatchCreateRequest<GenericRecord> request) {
        
        log.info("Creating batch of {} {} entities", 
                request != null && request.getEntities() != null ? request.getEntities().size() : 0, 
                entityType);
        
        try {
            // ENHANCED INPUT VALIDATION
            List<String> validationErrors = new ArrayList<>();
            validateEntityType(entityType, validationErrors);
            
            if (!validationErrors.isEmpty()) {
                log.warn("Entity type validation failed for batch create {}: {}", entityType, validationErrors);
                return ResponseEntity.badRequest().body(Map.of("errors", validationErrors));
            }
            
            // Validate batch request
            validateBatchRequest(request, validationErrors);
            
            if (!validationErrors.isEmpty()) {
                log.warn("Batch request validation failed for {}: {}", entityType, validationErrors);
                return ResponseEntity.badRequest().body(Map.of("errors", validationErrors));
            }
            
            EntityControllerConfig config = controllerRegistry.getConfig(entityType);
            if (config == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Entity type '" + entityType + "' is not supported"));
            }
            
            // Validate all entities in the batch
            List<String> allValidationErrors = new ArrayList<>();
            List<GenericRecord> validEntities = new ArrayList<>();
            
            for (int i = 0; i < request.getEntities().size(); i++) {
                GenericRecord entity = request.getEntities().get(i);
                List<String> entityErrors = config.getEntityValidator().validate(entityType, entity);
                final int entityIndex = i;
                
                if (!entityErrors.isEmpty()) {
                    entityErrors.forEach(error -> 
                        allValidationErrors.add(entityType + " " + entityIndex + ": " + error));
                } else {
                    validEntities.add(entity);
                }
            }
            
            if (!allValidationErrors.isEmpty()) {
                log.warn("Batch validation failed with {} errors for {} entities", 
                        allValidationErrors.size(), request.getEntities().size());
                return ResponseEntity.badRequest()
                        .body(Map.of("errors", allValidationErrors));
            }
            
            // Save batch
            BatchCreateResponse response = config.getEntityService().saveBatch(validEntities);
            log.info("Successfully created batch: {} {} entities, {} successes, {} failures", 
                    validEntities.size(), entityType, response.getSuccessCount(), response.getFailureCount());
            
            if (response.getFailureCount() > 0) {
                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).body(response);
            } else {
                return ResponseEntity.status(HttpStatus.CREATED).body(response);
            }
            
        } catch (Exception e) {
            log.error("Failed to create {} entity batch", entityType, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to create " + entityType + " batch"));
        }
    }

    /**
     * Get supported entity types
     * GET /api/v1/entities
     */
    @Operation(
            summary = "Get supported entity types",
            description = "Returns a list of all entity types that are currently registered and available for operations."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved supported entity types",
                    content = @Content(mediaType = "application/json",
                            examples = @ExampleObject(value = "{\"supportedEntityTypes\": [\"users\", \"orders\", \"products\"], \"message\": \"Supported entity types for this API\"}"))),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @GetMapping
    public ResponseEntity<?> getSupportedEntityTypes() {
        try {
            List<String> supportedTypes = controllerRegistry.getSupportedEntityTypes();
            return ResponseEntity.ok(Map.of(
                "supportedEntityTypes", supportedTypes,
                "message", "Supported entity types for this API"
            ));
        } catch (Exception e) {
            log.error("Failed to get supported entity types", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to retrieve supported entity types"));
        }
    }
}