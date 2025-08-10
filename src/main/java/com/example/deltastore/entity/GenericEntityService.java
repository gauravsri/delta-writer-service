package com.example.deltastore.entity;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.storage.DeltaTableManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * Generic entity service that handles CRUD operations for any entity type.
 * Eliminates the need for per-entity service implementations.
 */
@Service
@Slf4j
public class GenericEntityService {
    
    private final DeltaTableManager tableManager;
    private final DeltaStoreConfiguration config;
    private final EntityMetadataRegistry metadataRegistry;
    
    public GenericEntityService(
            @Qualifier("optimized") DeltaTableManager tableManager,
            DeltaStoreConfiguration config,
            EntityMetadataRegistry metadataRegistry) {
        this.tableManager = tableManager;
        this.config = config;
        this.metadataRegistry = metadataRegistry;
    }
    
    /**
     * Saves an entity from Map data (for REST API)
     */
    public EntityOperationResult<?> saveFromMap(String entityType, Map<String, Object> entityData) {
        try {
            validateEntityType(entityType);
            
            // Convert Map to GenericRecord using schema inference or registry
            GenericRecord record = convertMapToGenericRecord(entityType, entityData);
            
            return saveRecord(entityType, record);
            
        } catch (IllegalArgumentException e) {
            // Re-throw validation exceptions for proper error handling
            throw e;
        } catch (Exception e) {
            log.error("Failed to save entity from map data for type: {}", entityType, e);
            return EntityOperationResult.builder()
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
     * Saves an entity (create or update) - for typed GenericRecord entities
     */
    public <T extends GenericRecord> EntityOperationResult<T> save(String entityType, T entity) {
        return saveRecord(entityType, entity);
    }
    
    /**
     * Saves an entity (create or update) - internal implementation
     */
    private <T extends GenericRecord> EntityOperationResult<T> saveRecord(String entityType, T entity) {
        try {
            validateEntityType(entityType);
            Schema avroSchema = entity.getSchema();
            
            
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
     * Registers a new entity type dynamically
     */
    public void registerEntityType(String entityType, EntityMetadata metadata) {
        metadataRegistry.registerEntity(entityType, metadata);
        log.info("Registered new entity type: {}", entityType);
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
     * Converts Map data to GenericRecord for the specified entity type
     */
    private GenericRecord convertMapToGenericRecord(String entityType, Map<String, Object> data) {
        // Create schema dynamically from data
        Schema schema = createSchemaFromMap(entityType, data);
        
        // Register the schema if auto-registration is enabled
        if (config.getSchema().isAutoRegisterSchemas()) {
            EntityMetadata metadata = EntityMetadata.builder()
                .entityType(entityType)
                .schema(schema)
                .build();
            metadataRegistry.registerEntity(entityType, metadata);
        }
        
        // Convert Map to GenericRecord using Avro's GenericRecordBuilder
        org.apache.avro.generic.GenericRecordBuilder builder = new org.apache.avro.generic.GenericRecordBuilder(schema);
        
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = data.get(fieldName);
            builder.set(fieldName, value);
        }
        
        return builder.build();
    }
    
    /**
     * Creates an Avro schema from Map data structure
     */
    private Schema createSchemaFromMap(String entityType, Map<String, Object> data) {
        org.apache.avro.SchemaBuilder.RecordBuilder<Schema> builder = 
            org.apache.avro.SchemaBuilder.record(entityType).namespace("com.example.deltastore.schemas");
        
        org.apache.avro.SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            
            if (value == null) {
                fields = fields.name(fieldName).type().nullable().stringType().noDefault();
            } else if (value instanceof String) {
                fields = fields.name(fieldName).type().stringType().noDefault();
            } else if (value instanceof Integer) {
                fields = fields.name(fieldName).type().intType().noDefault();
            } else if (value instanceof Long) {
                fields = fields.name(fieldName).type().longType().noDefault();
            } else if (value instanceof Double) {
                fields = fields.name(fieldName).type().doubleType().noDefault();
            } else if (value instanceof Float) {
                fields = fields.name(fieldName).type().floatType().noDefault();
            } else if (value instanceof Boolean) {
                fields = fields.name(fieldName).type().booleanType().noDefault();
            } else {
                // Default to string for unknown types
                fields = fields.name(fieldName).type().stringType().noDefault();
            }
        }
        
        return fields.endRecord();
    }
    
    /**
     * Validates schema compatibility for evolution
     */
    private void validateSchemaCompatibility(String entityType, Schema newSchema) {
        // For write-only operations, just auto-register schemas if enabled
        if (config.getSchema().isAutoRegisterSchemas()) {
            EntityMetadata metadata = EntityMetadata.builder()
                .entityType(entityType)
                .schema(newSchema)
                .build();
            metadataRegistry.registerEntity(entityType, metadata);
        }
    }
}