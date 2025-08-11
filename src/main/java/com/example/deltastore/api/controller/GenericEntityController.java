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
            @PathVariable String entityType,
            @Parameter(description = "Entity data as JSON object matching the entity's Avro schema")
            @RequestBody Map<String, Object> entityData) {
        
        log.info("Creating {} entity", entityType);
        
        try {
            EntityControllerConfig config = controllerRegistry.getConfig(entityType);
            if (config == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Entity type '" + entityType + "' is not supported"));
            }

            // Convert Map to GenericRecord
            GenericRecord entity = config.getEntityConverter().convertFromMap(entityData);
            
            // Validate entity
            List<String> validationErrors = config.getEntityValidator().validate(entityType, entity);
            if (!validationErrors.isEmpty()) {
                log.warn("{} validation failed with errors: {}", entityType, validationErrors);
                return ResponseEntity.badRequest().body(Map.of("errors", validationErrors));
            }
            
            // Save entity
            config.getEntityService().save(entity);
            log.info("Successfully created {} entity", entityType);
            
            return ResponseEntity.status(HttpStatus.CREATED).build();
            
        } catch (Exception e) {
            log.error("Failed to create {} entity", entityType, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to create " + entityType + " entity"));
        }
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
            @PathVariable String entityType,
            @Parameter(description = "Batch create request containing array of entities and optional processing options")
            @Valid @RequestBody GenericBatchCreateRequest<GenericRecord> request) {
        
        log.info("Creating batch of {} {} entities", 
                request != null && request.getEntities() != null ? request.getEntities().size() : 0, 
                entityType);
        
        try {
            EntityControllerConfig config = controllerRegistry.getConfig(entityType);
            if (config == null) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Entity type '" + entityType + "' is not supported"));
            }

            if (request == null || request.getEntities() == null || request.getEntities().isEmpty()) {
                log.warn("Batch create request with empty or null entities list for {}", entityType);
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Entities list cannot be empty"));
            }
            
            if (request.getEntities().size() > 1000) {
                log.warn("Batch create request with too many entities: {} for {}", request.getEntities().size(), entityType);
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Batch size cannot exceed 1000 entities"));
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