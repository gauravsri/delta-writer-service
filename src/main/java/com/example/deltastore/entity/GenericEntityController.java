package com.example.deltastore.entity;

import org.apache.avro.generic.GenericRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generic REST controller that handles all entity types dynamically.
 * Eliminates the need for per-entity controllers.
 */
@RestController
@RequestMapping("/api/v1/entities")
@Slf4j
public class GenericEntityController {
    
    private final GenericEntityService entityService;
    
    public GenericEntityController(GenericEntityService entityService) {
        this.entityService = entityService;
    }
    
    /**
     * Creates a single entity
     */
    @PostMapping("/{entityType}")
    public ResponseEntity<EntityOperationResult<?>> createEntity(
            @PathVariable String entityType,
            @RequestBody GenericRecord entity) {
        
        log.info("Creating entity of type: {}", entityType);
        
        EntityOperationResult<?> result = entityService.save(entityType, entity);
        
        if (result.isSuccess()) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.badRequest().body(result);
        }
    }
    
    /**
     * Creates multiple entities in batch
     */
    @PostMapping("/{entityType}/batch")
    public ResponseEntity<EntityOperationResult<?>> createEntitiesBatch(
            @PathVariable String entityType,
            @RequestBody List<GenericRecord> entities) {
        
        log.info("Creating batch of {} entities of type: {}", entities.size(), entityType);
        
        EntityOperationResult<?> result = entityService.saveAll(entityType, entities);
        
        if (result.isSuccess()) {
            return ResponseEntity.ok(result);
        } else {
            return ResponseEntity.badRequest().body(result);
        }
    }
    
    /**
     * Gets an entity by ID
     */
    @GetMapping("/{entityType}/{id}")
    public ResponseEntity<Map<String, Object>> getEntity(
            @PathVariable String entityType,
            @PathVariable String id) {
        
        log.info("Getting entity of type: {} with id: {}", entityType, id);
        
        Optional<Map<String, Object>> entity = entityService.findById(entityType, id);
        
        return entity.map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Gets entities by partition filters
     */
    @PostMapping("/{entityType}/search")
    public ResponseEntity<List<Map<String, Object>>> searchEntities(
            @PathVariable String entityType,
            @RequestBody Map<String, String> partitionFilters) {
        
        log.info("Searching entities of type: {} with filters: {}", entityType, partitionFilters);
        
        List<Map<String, Object>> entities = entityService.findByPartition(entityType, partitionFilters);
        
        return ResponseEntity.ok(entities);
    }
    
    /**
     * Gets all registered entity types
     */
    @GetMapping("/types")
    public ResponseEntity<List<String>> getEntityTypes() {
        List<String> entityTypes = entityService.getRegisteredEntityTypes();
        return ResponseEntity.ok(entityTypes);
    }
    
    /**
     * Gets metadata for a specific entity type
     */
    @GetMapping("/{entityType}/metadata")
    public ResponseEntity<EntityMetadata> getEntityMetadata(@PathVariable String entityType) {
        Optional<EntityMetadata> metadata = entityService.getEntityMetadata(entityType);
        
        return metadata.map(ResponseEntity::ok)
                      .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Registers a new entity type with metadata
     */
    @PostMapping("/{entityType}/register")
    public ResponseEntity<Map<String, String>> registerEntityType(
            @PathVariable String entityType,
            @RequestBody EntityMetadata metadata) {
        
        try {
            entityService.registerEntityType(entityType, metadata);
            
            Map<String, String> response = Map.of(
                "status", "success",
                "message", "Entity type registered successfully",
                "entityType", entityType
            );
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Failed to register entity type: {}", entityType, e);
            
            Map<String, String> response = Map.of(
                "status", "error",
                "message", "Failed to register entity type: " + e.getMessage(),
                "entityType", entityType
            );
            
            return ResponseEntity.badRequest().body(response);
        }
    }
}