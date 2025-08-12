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
import java.util.Objects;
import java.util.regex.Pattern;

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
     * Converts Map data to GenericRecord for the specified entity type with comprehensive validation
     */
    private GenericRecord convertMapToGenericRecord(String entityType, Map<String, Object> data) {
        // CRITICAL FIX: Validate inputs before processing
        if (data == null) {
            throw new IllegalArgumentException("Cannot convert null data to GenericRecord");
        }
        
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Cannot convert empty data map to GenericRecord");
        }
        
        try {
            // Create schema dynamically from data (includes comprehensive validation)
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
            org.apache.avro.generic.GenericRecordBuilder builder = 
                new org.apache.avro.generic.GenericRecordBuilder(schema);
            
            // CRITICAL FIX: Safe field mapping with type conversion
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                Object value = data.get(fieldName);
                
                try {
                    // Convert value to match schema type if necessary
                    Object convertedValue = convertValueForSchema(value, field);
                    builder.set(fieldName, convertedValue);
                    
                } catch (Exception e) {
                    log.error("Failed to set field '{}' with value '{}' for entity type '{}'", 
                        fieldName, value, entityType, e);
                    throw new IllegalArgumentException(
                        "Failed to convert field '" + fieldName + "' for entity type: " + entityType, e);
                }
            }
            
            GenericRecord record = builder.build();
            log.debug("Successfully converted Map to GenericRecord for entity '{}' with {} fields", 
                entityType, schema.getFields().size());
            
            return record;
            
        } catch (Exception e) {
            log.error("Failed to convert Map to GenericRecord for entity type '{}': {}", 
                entityType, e.getMessage(), e);
            throw new IllegalArgumentException(
                "Failed to convert data to GenericRecord for entity type: " + entityType, e);
        }
    }
    
    /**
     * Converts a value to match the expected schema type
     */
    private Object convertValueForSchema(Object value, Schema.Field field) {
        if (value == null) {
            // Check if field allows null
            if (isNullableField(field)) {
                return null;
            } else {
                throw new IllegalArgumentException("Field '" + field.name() + "' does not allow null values");
            }
        }
        
        Schema fieldSchema = field.schema();
        Schema.Type schemaType = fieldSchema.getType();
        
        // Handle union types (typically nullable fields)
        if (schemaType == Schema.Type.UNION) {
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() != Schema.Type.NULL) {
                    schemaType = unionType.getType();
                    break;
                }
            }
        }
        
        // Convert value based on expected type
        try {
            switch (schemaType) {
                case STRING:
                    return value.toString();
                    
                case INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else {
                        return Integer.parseInt(value.toString());
                    }
                    
                case LONG:
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    } else {
                        return Long.parseLong(value.toString());
                    }
                    
                case FLOAT:
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    } else {
                        return Float.parseFloat(value.toString());
                    }
                    
                case DOUBLE:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else {
                        return Double.parseDouble(value.toString());
                    }
                    
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    } else {
                        return Boolean.parseBoolean(value.toString());
                    }
                    
                case BYTES:
                    if (value instanceof byte[]) {
                        return value;
                    } else {
                        return value.toString().getBytes();
                    }
                    
                default:
                    // For unknown types, convert to string
                    return value.toString();
            }
            
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Cannot convert value '" + value + "' to " + schemaType + " for field '" + field.name() + "'", e);
        }
    }
    
    /**
     * Checks if a field allows null values (is nullable)
     */
    private boolean isNullableField(Schema.Field field) {
        Schema fieldSchema = field.schema();
        if (fieldSchema.getType() == Schema.Type.UNION) {
            return fieldSchema.getTypes().stream()
                .anyMatch(s -> s.getType() == Schema.Type.NULL);
        }
        return false;
    }
    
    /**
     * Creates an Avro schema from Map data structure with comprehensive validation
     */
    private Schema createSchemaFromMap(String entityType, Map<String, Object> data) {
        // CRITICAL FIX: Comprehensive input validation
        if (data == null) {
            throw new IllegalArgumentException("Cannot create schema from null data");
        }
        
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Cannot create schema from empty data map");
        }
        
        // Validate entity type
        if (entityType == null || entityType.trim().isEmpty()) {
            throw new IllegalArgumentException("Entity type cannot be null or empty");
        }
        
        if (!isValidAvroName(entityType)) {
            throw new IllegalArgumentException("Invalid entity type name: " + entityType + 
                ". Must start with letter or underscore and contain only alphanumeric characters and underscores");
        }
        
        // Validate all field names before processing
        for (String fieldName : data.keySet()) {
            if (fieldName == null || fieldName.trim().isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be null or empty");
            }
            
            if (!isValidAvroName(fieldName)) {
                throw new IllegalArgumentException("Invalid field name: " + fieldName + 
                    ". Must start with letter or underscore and contain only alphanumeric characters and underscores");
            }
        }
        
        // Ensure at least one non-null value for type inference
        boolean hasNonNullValue = data.values().stream().anyMatch(Objects::nonNull);
        if (!hasNonNullValue) {
            throw new IllegalArgumentException("Cannot infer schema types - all values are null. " +
                "At least one field must have a non-null value for type inference");
        }
        
        // Validate data size to prevent memory issues
        if (data.size() > 1000) {
            throw new IllegalArgumentException("Too many fields in entity data: " + data.size() + 
                ". Maximum allowed is 1000 fields");
        }
        
        try {
            org.apache.avro.SchemaBuilder.RecordBuilder<Schema> builder = 
                org.apache.avro.SchemaBuilder.record(entityType).namespace("com.example.deltastore.schemas");
            
            org.apache.avro.SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
            
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                
                // CRITICAL FIX: Robust type inference with null handling
                if (value == null) {
                    // All null fields are nullable strings by default
                    fields = fields.name(fieldName).type().nullable().stringType().noDefault();
                } else {
                    // Infer type from non-null value
                    Class<?> valueClass = value.getClass();
                    
                    if (value instanceof String) {
                        // Validate string length to prevent memory issues
                        String strValue = (String) value;
                        if (strValue.length() > 10000) {
                            log.warn("String field '{}' has length {} which may cause performance issues", 
                                fieldName, strValue.length());
                        }
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
                        
                    } else if (value instanceof byte[]) {
                        // Handle byte arrays
                        fields = fields.name(fieldName).type().bytesType().noDefault();
                        
                    } else if (valueClass.isPrimitive()) {
                        // Handle other primitive wrapper types
                        if (valueClass == Byte.class || valueClass == Short.class) {
                            fields = fields.name(fieldName).type().intType().noDefault();
                        } else {
                            // Default to string for unknown primitive types
                            log.warn("Unknown primitive type {} for field '{}', defaulting to string", 
                                valueClass.getSimpleName(), fieldName);
                            fields = fields.name(fieldName).type().stringType().noDefault();
                        }
                        
                    } else {
                        // Default to string for complex types, with warning
                        log.warn("Complex type {} for field '{}' will be converted to string. " +
                            "Consider flattening complex objects before schema generation", 
                            valueClass.getSimpleName(), fieldName);
                        fields = fields.name(fieldName).type().stringType().noDefault();
                    }
                }
            }
            
            Schema schema = fields.endRecord();
            log.debug("Successfully created schema for entity '{}' with {} fields", entityType, data.size());
            return schema;
            
        } catch (Exception e) {
            log.error("Failed to create schema for entity type '{}': {}", entityType, e.getMessage(), e);
            throw new IllegalArgumentException("Failed to create valid Avro schema for entity type: " + entityType, e);
        }
    }
    
    /**
     * Validates if a name is valid for Avro schema (record or field names)
     */
    private boolean isValidAvroName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        
        // Avro names must start with [A-Za-z_] and contain only [A-Za-z0-9_]
        Pattern avroNamePattern = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
        return avroNamePattern.matcher(name).matches() && name.length() <= 255; // Reasonable length limit
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