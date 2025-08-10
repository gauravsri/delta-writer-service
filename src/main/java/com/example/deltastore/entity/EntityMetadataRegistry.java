package com.example.deltastore.entity;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing entity metadata and schemas.
 * Provides dynamic entity registration and schema management.
 */
@Component
@Slf4j
public class EntityMetadataRegistry {
    
    private final Map<String, EntityMetadata> entityMetadata = new ConcurrentHashMap<>();
    private final Map<String, Schema> entitySchemas = new ConcurrentHashMap<>();
    
    /**
     * Registers a new entity type with metadata
     */
    public void registerEntity(String entityType, EntityMetadata metadata) {
        validateEntityType(entityType);
        
        EntityMetadata enrichedMetadata = EntityMetadata.builder()
            .entityType(entityType)
            .schema(metadata.getSchema())
            .primaryKeyColumn(metadata.getPrimaryKeyColumn())
            .partitionColumns(metadata.getPartitionColumns())
            .properties(metadata.getProperties() != null ? metadata.getProperties() : new HashMap<>())
            .registeredAt(LocalDateTime.now())
            .lastUpdated(LocalDateTime.now())
            .schemaVersion(generateSchemaVersion(metadata.getSchema()))
            .active(true)
            .build();
        
        entityMetadata.put(entityType, enrichedMetadata);
        entitySchemas.put(entityType, metadata.getSchema());
        
        log.info("Registered entity type: {} with schema version: {}", 
            entityType, enrichedMetadata.getSchemaVersion());
    }
    
    /**
     * Gets entity metadata by type
     */
    public Optional<EntityMetadata> getEntityMetadata(String entityType) {
        return Optional.ofNullable(entityMetadata.get(entityType));
    }
    
    /**
     * Gets entity schema by type
     */
    public Optional<Schema> getEntitySchema(String entityType) {
        return Optional.ofNullable(entitySchemas.get(entityType));
    }
    
    /**
     * Checks if entity type is registered
     */
    public boolean isEntityRegistered(String entityType) {
        return entityMetadata.containsKey(entityType);
    }
    
    /**
     * Updates entity metadata
     */
    public void updateEntityMetadata(String entityType, EntityMetadata metadata) {
        EntityMetadata existing = entityMetadata.get(entityType);
        if (existing == null) {
            throw new IllegalArgumentException("Entity type not registered: " + entityType);
        }
        
        EntityMetadata updated = EntityMetadata.builder()
            .entityType(entityType)
            .schema(metadata.getSchema())
            .primaryKeyColumn(metadata.getPrimaryKeyColumn())
            .partitionColumns(metadata.getPartitionColumns())
            .properties(metadata.getProperties())
            .registeredAt(existing.getRegisteredAt())
            .lastUpdated(LocalDateTime.now())
            .schemaVersion(generateSchemaVersion(metadata.getSchema()))
            .active(metadata.isActive())
            .build();
        
        entityMetadata.put(entityType, updated);
        entitySchemas.put(entityType, metadata.getSchema());
        
        log.info("Updated entity metadata for type: {}", entityType);
    }
    
    /**
     * Deactivates an entity type
     */
    public void deactivateEntity(String entityType) {
        EntityMetadata existing = entityMetadata.get(entityType);
        if (existing != null) {
            EntityMetadata deactivated = EntityMetadata.builder()
                .entityType(existing.getEntityType())
                .schema(existing.getSchema())
                .primaryKeyColumn(existing.getPrimaryKeyColumn())
                .partitionColumns(existing.getPartitionColumns())
                .properties(existing.getProperties())
                .registeredAt(existing.getRegisteredAt())
                .lastUpdated(LocalDateTime.now())
                .schemaVersion(existing.getSchemaVersion())
                .active(false)
                .build();
            
            entityMetadata.put(entityType, deactivated);
            log.info("Deactivated entity type: {}", entityType);
        }
    }
    
    /**
     * Gets all registered entity types
     */
    public List<String> getRegisteredEntityTypes() {
        return new ArrayList<>(entityMetadata.keySet());
    }
    
    /**
     * Gets all active entity types
     */
    public List<String> getActiveEntityTypes() {
        return entityMetadata.values().stream()
            .filter(EntityMetadata::isActive)
            .map(EntityMetadata::getEntityType)
            .toList();
    }
    
    /**
     * Gets registry statistics
     */
    public Map<String, Object> getRegistryStats() {
        long activeCount = entityMetadata.values().stream()
            .filter(EntityMetadata::isActive)
            .count();
        
        return Map.of(
            "total_registered", entityMetadata.size(),
            "active_entities", activeCount,
            "inactive_entities", entityMetadata.size() - activeCount,
            "entity_types", getRegisteredEntityTypes()
        );
    }
    
    /**
     * Clears all registered entities (for testing)
     */
    public void clearAll() {
        entityMetadata.clear();
        entitySchemas.clear();
        log.info("Cleared all registered entities");
    }
    
    /**
     * Validates entity type name
     */
    private void validateEntityType(String entityType) {
        if (entityType == null || entityType.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity type cannot be null or empty");
        }
        
        if (!entityType.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException(
                "Entity type must start with a letter and contain only letters, numbers, and underscores");
        }
    }
    
    /**
     * Generates a version string for a schema
     */
    private String generateSchemaVersion(Schema schema) {
        if (schema == null) {
            return "unknown";
        }
        
        // Use schema fingerprint as version
        return "v" + Math.abs(schema.toString().hashCode());
    }
}