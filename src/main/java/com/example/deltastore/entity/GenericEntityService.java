package com.example.deltastore.entity;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.schema.DeltaSchemaManager;
import com.example.deltastore.storage.DeltaTableManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Collections;

/**
 * Generic entity service that handles CRUD operations for any entity type.
 * Eliminates the need for per-entity service implementations.
 */
@Service
@Slf4j
public class GenericEntityService {
    
    private final DeltaTableManager tableManager;
    private final DeltaSchemaManager schemaManager;
    private final DeltaStoreConfiguration config;
    private final EntityMetadataRegistry metadataRegistry;
    
    public GenericEntityService(
            @Qualifier("optimized") DeltaTableManager tableManager,
            DeltaSchemaManager schemaManager,
            DeltaStoreConfiguration config,
            EntityMetadataRegistry metadataRegistry) {
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.config = config;
        this.metadataRegistry = metadataRegistry;
    }
    
    /**
     * Saves an entity (create or update)
     */
    public <T extends GenericRecord> EntityOperationResult<T> save(String entityType, T entity) {
        try {
            validateEntityType(entityType);
            Schema avroSchema = entity.getSchema();
            
            // Get table configuration
            DeltaStoreConfiguration.TableConfig tableConfig = config.getTableConfigOrDefault(entityType);
            
            // Validate schema if enabled
            if (config.getSchema().isEnableSchemaValidation()) {
                validateSchemaCompatibility(entityType, avroSchema);
            }
            
            // Convert single entity to list for batch processing
            List<GenericRecord> records = Collections.singletonList(entity);
            
            // Write to Delta table
            tableManager.write(entityType, records, avroSchema);
            
            log.info("Successfully saved entity of type: {}", entityType);
            
            return EntityOperationResult.<T>builder()
                .success(true)
                .entityType(entityType)
                .operationType(OperationType.WRITE)
                .recordCount(1)
                .message("Entity saved successfully")
                .build();
            
        } catch (Exception e) {
            log.error("Failed to save entity of type: {}", entityType, e);
            return EntityOperationResult.<T>builder()
                .success(false)
                .entityType(entityType)
                .operationType(OperationType.WRITE)
                .recordCount(0)
                .message("Failed to save entity: " + e.getMessage())
                .error(e)
                .build();
        }
    }
    
    /**
     * Saves multiple entities in batch
     */
    public <T extends GenericRecord> EntityOperationResult<T> saveAll(String entityType, List<T> entities) {
        try {
            validateEntityType(entityType);
            
            if (entities == null || entities.isEmpty()) {
                return EntityOperationResult.<T>builder()
                    .success(true)
                    .entityType(entityType)
                    .operationType(OperationType.WRITE)
                    .recordCount(0)
                    .message("No entities to save")
                    .build();
            }
            
            Schema avroSchema = entities.get(0).getSchema();
            
            // Validate schema if enabled
            if (config.getSchema().isEnableSchemaValidation()) {
                validateSchemaCompatibility(entityType, avroSchema);
            }
            
            // Convert to generic records list
            List<GenericRecord> records = entities.stream()
                .map(GenericRecord.class::cast)
                .toList();
            
            // Write to Delta table
            tableManager.write(entityType, records, avroSchema);
            
            log.info("Successfully saved {} entities of type: {}", entities.size(), entityType);
            
            return EntityOperationResult.<T>builder()
                .success(true)
                .entityType(entityType)
                .operationType(OperationType.WRITE)
                .recordCount(entities.size())
                .message("Entities saved successfully")
                .build();
            
        } catch (Exception e) {
            log.error("Failed to save entities of type: {}", entityType, e);
            return EntityOperationResult.<T>builder()
                .success(false)
                .entityType(entityType)
                .operationType(OperationType.WRITE)
                .recordCount(0)
                .message("Failed to save entities: " + e.getMessage())
                .error(e)
                .build();
        }
    }
    
    /**
     * Finds entity by primary key
     */
    public Optional<Map<String, Object>> findById(String entityType, String id) {
        try {
            validateEntityType(entityType);
            
            DeltaStoreConfiguration.TableConfig tableConfig = config.getTableConfigOrDefault(entityType);
            String primaryKey = tableConfig.getPrimaryKeyColumn();
            
            if (primaryKey == null || primaryKey.isEmpty()) {
                log.warn("No primary key configured for entity type: {}", entityType);
                return Optional.empty();
            }
            
            return tableManager.read(entityType, primaryKey, id);
            
        } catch (Exception e) {
            log.error("Failed to find entity by id for type: {}", entityType, e);
            return Optional.empty();
        }
    }
    
    /**
     * Finds entities by partition filters
     */
    public List<Map<String, Object>> findByPartition(String entityType, Map<String, String> partitionFilters) {
        try {
            validateEntityType(entityType);
            return tableManager.readByPartitions(entityType, partitionFilters);
        } catch (Exception e) {
            log.error("Failed to find entities by partition for type: {}", entityType, e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Registers a new entity type dynamically
     */
    public void registerEntityType(String entityType, EntityMetadata metadata) {
        metadataRegistry.registerEntity(entityType, metadata);
        log.info("Registered new entity type: {}", entityType);
    }
    
    /**
     * Gets entity metadata
     */
    public Optional<EntityMetadata> getEntityMetadata(String entityType) {
        return metadataRegistry.getEntityMetadata(entityType);
    }
    
    /**
     * Lists all registered entity types
     */
    public List<String> getRegisteredEntityTypes() {
        return metadataRegistry.getRegisteredEntityTypes();
    }
    
    /**
     * Validates entity type is registered or configured
     */
    private void validateEntityType(String entityType) {
        if (entityType == null || entityType.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity type cannot be null or empty");
        }
        
        // Check if entity is configured or registered
        boolean hasTableConfig = config.getTables().containsKey(entityType);
        boolean isRegistered = metadataRegistry.isEntityRegistered(entityType);
        
        if (!hasTableConfig && !isRegistered) {
            log.warn("Entity type {} not configured. Using default configuration.", entityType);
        }
    }
    
    /**
     * Validates schema compatibility for evolution
     */
    private void validateSchemaCompatibility(String entityType, Schema newSchema) {
        Optional<Schema> existingSchema = metadataRegistry.getEntitySchema(entityType);
        
        if (existingSchema.isPresent()) {
            boolean compatible = schemaManager.isSchemaCompatible(existingSchema.get(), newSchema);
            if (!compatible) {
                throw new IllegalArgumentException(
                    "Schema is not compatible with existing schema for entity type: " + entityType);
            }
        } else if (config.getSchema().isAutoRegisterSchemas()) {
            // Auto-register schema
            EntityMetadata metadata = EntityMetadata.builder()
                .entityType(entityType)
                .schema(newSchema)
                .build();
            metadataRegistry.registerEntity(entityType, metadata);
        }
    }
}