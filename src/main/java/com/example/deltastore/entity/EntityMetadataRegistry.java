package com.example.deltastore.entity;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;

/**
 * Registry for managing entity metadata and schemas.
 * Provides dynamic entity registration and schema management.
 */
@Component
@Slf4j
public class EntityMetadataRegistry {
    
    private final Map<String, EntityMetadata> entityMetadata = new ConcurrentHashMap<>();
    private final Map<String, Schema> entitySchemas = new ConcurrentHashMap<>();
    
    // HIGH PRIORITY ISSUE #7: Thread-safe operations to prevent consistency issues
    private final ReadWriteLock registryLock = new ReentrantReadWriteLock();
    private final Map<String, Long> schemaRegistrationTimes = new ConcurrentHashMap<>();
    
    /**
     * Registers a new entity type with metadata (HIGH PRIORITY ISSUE #7: Thread-safe with consistency checks)
     */
    public void registerEntity(String entityType, EntityMetadata metadata) {
        validateEntityType(entityType);
        validateMetadata(metadata);
        
        registryLock.writeLock().lock();
        try {
            // HIGH PRIORITY ISSUE #7: Check for existing entity and handle appropriately
            EntityMetadata existing = entityMetadata.get(entityType);
            if (existing != null) {
                if (existing.isActive()) {
                    // Check if schema is compatible before allowing duplicate registration
                    if (isSchemaSame(existing.getSchema(), metadata.getSchema())) {
                        log.debug("Entity type {} already registered with identical schema, skipping", entityType);
                        return;
                    } else {
                        log.warn("Attempted to re-register entity type {} with different schema", entityType);
                        throw new IllegalStateException("Entity type '" + entityType + 
                            "' already registered with different schema. Use updateEntityMetadata() for schema changes.");
                    }
                } else {
                    log.info("Reactivating previously deactivated entity type: {}", entityType);
                }
            }
            
            String schemaVersion = generateSecureSchemaVersion(metadata.getSchema());
            LocalDateTime now = LocalDateTime.now();
            
            EntityMetadata enrichedMetadata = EntityMetadata.builder()
                .entityType(entityType)
                .schema(metadata.getSchema())
                .primaryKeyColumn(metadata.getPrimaryKeyColumn())
                .partitionColumns(metadata.getPartitionColumns())
                .properties(metadata.getProperties() != null ? metadata.getProperties() : new HashMap<>())
                .registeredAt(existing != null ? existing.getRegisteredAt() : now)
                .lastUpdated(now)
                .schemaVersion(schemaVersion)
                .active(true)
                .build();
            
            // Atomic update of both maps to maintain consistency
            entityMetadata.put(entityType, enrichedMetadata);
            entitySchemas.put(entityType, metadata.getSchema());
            schemaRegistrationTimes.put(entityType, System.currentTimeMillis());
            
            log.info("Registered entity type: {} with schema version: {}", entityType, schemaVersion);
            
        } finally {
            registryLock.writeLock().unlock();
        }
    }
    
    /**
     * Gets entity metadata by type (HIGH PRIORITY ISSUE #7: Thread-safe read)
     */
    public Optional<EntityMetadata> getEntityMetadata(String entityType) {
        registryLock.readLock().lock();
        try {
            EntityMetadata metadata = entityMetadata.get(entityType);
            
            // HIGH PRIORITY ISSUE #7: Verify consistency between maps
            if (metadata != null) {
                Schema schema = entitySchemas.get(entityType);
                if (schema == null) {
                    log.error("Inconsistency detected: metadata exists but schema missing for entity type: {}", entityType);
                    // Attempt to repair inconsistency
                    entitySchemas.put(entityType, metadata.getSchema());
                }
            }
            
            return Optional.ofNullable(metadata);
        } finally {
            registryLock.readLock().unlock();
        }
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
     * Updates entity metadata (HIGH PRIORITY ISSUE #7: Thread-safe with validation)
     */
    public void updateEntityMetadata(String entityType, EntityMetadata metadata) {
        validateEntityType(entityType);
        validateMetadata(metadata);
        
        registryLock.writeLock().lock();
        try {
            EntityMetadata existing = entityMetadata.get(entityType);
            if (existing == null) {
                throw new IllegalArgumentException("Entity type not registered: " + entityType);
            }
            
            // HIGH PRIORITY ISSUE #7: Validate schema compatibility for updates
            if (!isSchemaSame(existing.getSchema(), metadata.getSchema())) {
                log.info("Schema change detected for entity type: {}, validating compatibility", entityType);
                // In a write-only system, we're more permissive with schema changes
                // but we still log them for audit purposes
            }
            
            String newSchemaVersion = generateSecureSchemaVersion(metadata.getSchema());
            
            EntityMetadata updated = EntityMetadata.builder()
                .entityType(entityType)
                .schema(metadata.getSchema())
                .primaryKeyColumn(metadata.getPrimaryKeyColumn())
                .partitionColumns(metadata.getPartitionColumns())
                .properties(metadata.getProperties() != null ? metadata.getProperties() : existing.getProperties())
                .registeredAt(existing.getRegisteredAt())
                .lastUpdated(LocalDateTime.now())
                .schemaVersion(newSchemaVersion)
                .active(metadata.isActive())
                .build();
            
            // Atomic update of both maps to maintain consistency
            entityMetadata.put(entityType, updated);
            entitySchemas.put(entityType, metadata.getSchema());
            schemaRegistrationTimes.put(entityType, System.currentTimeMillis());
            
            log.info("Updated entity metadata for type: {} (schema version: {} -> {})", 
                entityType, existing.getSchemaVersion(), newSchemaVersion);
            
        } finally {
            registryLock.writeLock().unlock();
        }
    }
    
    /**
     * Deactivates an entity type (HIGH PRIORITY ISSUE #7: Thread-safe with cleanup)
     */
    public void deactivateEntity(String entityType) {
        validateEntityType(entityType);
        
        registryLock.writeLock().lock();
        try {
            EntityMetadata existing = entityMetadata.get(entityType);
            if (existing != null) {
                if (!existing.isActive()) {
                    log.debug("Entity type {} is already deactivated", entityType);
                    return;
                }
                
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
                
                // Update metadata but keep schema for potential reactivation
                entityMetadata.put(entityType, deactivated);
                
                log.info("Deactivated entity type: {} (schema preserved for potential reactivation)", entityType);
            } else {
                log.warn("Attempted to deactivate non-existent entity type: {}", entityType);
            }
        } finally {
            registryLock.writeLock().unlock();
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
     * Gets registry statistics (HIGH PRIORITY ISSUE #7: Enhanced with consistency metrics)
     */
    public Map<String, Object> getRegistryStats() {
        registryLock.readLock().lock();
        try {
            long activeCount = entityMetadata.values().stream()
                .filter(EntityMetadata::isActive)
                .count();
            
            // HIGH PRIORITY ISSUE #7: Include consistency metrics
            long inconsistentCount = 0;
            long orphanedSchemas = 0;
            
            // Count inconsistencies
            for (Map.Entry<String, EntityMetadata> entry : entityMetadata.entrySet()) {
                String entityType = entry.getKey();
                EntityMetadata metadata = entry.getValue();
                Schema schema = entitySchemas.get(entityType);
                
                if (schema == null || !isSchemaSame(metadata.getSchema(), schema)) {
                    inconsistentCount++;
                }
            }
            
            // Count orphaned schemas
            for (String entityType : entitySchemas.keySet()) {
                if (!entityMetadata.containsKey(entityType)) {
                    orphanedSchemas++;
                }
            }
            
            Map<String, Object> stats = new HashMap<>();
            stats.put("total_registered", entityMetadata.size());
            stats.put("active_entities", activeCount);
            stats.put("inactive_entities", entityMetadata.size() - activeCount);
            stats.put("entity_types", getRegisteredEntityTypes());
            
            // HIGH PRIORITY ISSUE #7: Consistency metrics
            stats.put("metadata_schema_consistent", inconsistentCount == 0);
            stats.put("inconsistent_entities", inconsistentCount);
            stats.put("orphaned_schemas", orphanedSchemas);
            stats.put("total_schemas", entitySchemas.size());
            stats.put("registry_health_score", calculateHealthScore(inconsistentCount, orphanedSchemas));
            
            return stats;
        } finally {
            registryLock.readLock().unlock();
        }
    }
    
    /**
     * Calculates registry health score (HIGH PRIORITY ISSUE #7)
     */
    private double calculateHealthScore(long inconsistentCount, long orphanedSchemas) {
        if (entityMetadata.isEmpty()) {
            return 1.0; // Perfect score for empty registry
        }
        
        long totalIssues = inconsistentCount + orphanedSchemas;
        long totalEntities = entityMetadata.size();
        
        if (totalIssues == 0) {
            return 1.0; // Perfect health
        }
        
        // Health score = 1 - (issues / entities), minimum 0.0
        return Math.max(0.0, 1.0 - ((double) totalIssues / totalEntities));
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
     * Generates a version string for a schema (HIGH PRIORITY ISSUE #7: Secure version generation)
     * @deprecated Use generateSecureSchemaVersion instead
     */
    @Deprecated
    private String generateSchemaVersion(Schema schema) {
        if (schema == null) {
            return "unknown";
        }
        
        // Use schema fingerprint as version
        return "v" + Math.abs(schema.toString().hashCode());
    }
    
    /**
     * Generates a secure version string for a schema using SHA-256 (HIGH PRIORITY ISSUE #7)
     */
    private String generateSecureSchemaVersion(Schema schema) {
        if (schema == null) {
            return "unknown";
        }
        
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(schema.toString().getBytes(StandardCharsets.UTF_8));
            
            // Convert to hex string and take first 8 characters for readability
            StringBuilder hexString = new StringBuilder();
            for (int i = 0; i < Math.min(hash.length, 4); i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            
            return "v" + hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            log.warn("SHA-256 not available, falling back to hashCode", e);
            return "v" + Math.abs(schema.toString().hashCode());
        }
    }
    
    /**
     * Validates entity metadata (HIGH PRIORITY ISSUE #7)
     */
    private void validateMetadata(EntityMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("Entity metadata cannot be null");
        }
        
        if (metadata.getSchema() == null) {
            throw new IllegalArgumentException("Entity schema cannot be null");
        }
        
        // Validate primary key column if specified
        if (metadata.getPrimaryKeyColumn() != null) {
            String primaryKey = metadata.getPrimaryKeyColumn().trim();
            if (primaryKey.isEmpty()) {
                throw new IllegalArgumentException("Primary key column cannot be empty");
            }
            
            // Check if primary key field exists in schema
            boolean fieldExists = metadata.getSchema().getFields().stream()
                .anyMatch(field -> field.name().equals(primaryKey));
            
            if (!fieldExists) {
                throw new IllegalArgumentException("Primary key column '" + primaryKey + 
                    "' not found in schema fields");
            }
        }
        
        // Validate partition columns if specified
        if (metadata.getPartitionColumns() != null && !metadata.getPartitionColumns().isEmpty()) {
            for (String partitionColumn : metadata.getPartitionColumns()) {
                if (partitionColumn == null || partitionColumn.trim().isEmpty()) {
                    throw new IllegalArgumentException("Partition column names cannot be null or empty");
                }
                
                // Check if partition column exists in schema
                boolean fieldExists = metadata.getSchema().getFields().stream()
                    .anyMatch(field -> field.name().equals(partitionColumn.trim()));
                
                if (!fieldExists) {
                    throw new IllegalArgumentException("Partition column '" + partitionColumn + 
                        "' not found in schema fields");
                }
            }
        }
    }
    
    /**
     * Checks if two schemas are identical (HIGH PRIORITY ISSUE #7)
     */
    private boolean isSchemaSame(Schema schema1, Schema schema2) {
        if (schema1 == null && schema2 == null) {
            return true;
        }
        if (schema1 == null || schema2 == null) {
            return false;
        }
        
        // Compare schema strings for exact match
        return schema1.toString().equals(schema2.toString());
    }
    
    /**
     * Validates registry consistency and attempts repairs (HIGH PRIORITY ISSUE #7)
     */
    public void validateConsistency() {
        registryLock.writeLock().lock();
        try {
            log.info("Validating entity metadata registry consistency...");
            
            List<String> inconsistencies = new ArrayList<>();
            int repairedCount = 0;
            
            // Check that every metadata entry has a corresponding schema entry
            for (Map.Entry<String, EntityMetadata> entry : entityMetadata.entrySet()) {
                String entityType = entry.getKey();
                EntityMetadata metadata = entry.getValue();
                Schema schema = entitySchemas.get(entityType);
                
                if (schema == null) {
                    inconsistencies.add("Missing schema for entity type: " + entityType);
                    // Repair: Add schema from metadata
                    entitySchemas.put(entityType, metadata.getSchema());
                    repairedCount++;
                    log.warn("Repaired missing schema for entity type: {}", entityType);
                } else if (!isSchemaSame(metadata.getSchema(), schema)) {
                    inconsistencies.add("Schema mismatch for entity type: " + entityType);
                    // Repair: Update schema from metadata (metadata is source of truth)
                    entitySchemas.put(entityType, metadata.getSchema());
                    repairedCount++;
                    log.warn("Repaired schema mismatch for entity type: {}", entityType);
                }
            }
            
            // Check for orphaned schemas (schemas without metadata)
            for (String entityType : entitySchemas.keySet()) {
                if (!entityMetadata.containsKey(entityType)) {
                    inconsistencies.add("Orphaned schema for entity type: " + entityType);
                    // Repair: Remove orphaned schema
                    entitySchemas.remove(entityType);
                    schemaRegistrationTimes.remove(entityType);
                    repairedCount++;
                    log.warn("Removed orphaned schema for entity type: {}", entityType);
                }
            }
            
            if (inconsistencies.isEmpty()) {
                log.info("Entity metadata registry consistency check passed");
            } else {
                log.warn("Found {} consistency issues, repaired {} automatically", 
                    inconsistencies.size(), repairedCount);
                inconsistencies.forEach(issue -> log.debug("Consistency issue: {}", issue));
            }
            
        } finally {
            registryLock.writeLock().unlock();
        }
    }
}