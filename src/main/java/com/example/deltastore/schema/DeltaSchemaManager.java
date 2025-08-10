package com.example.deltastore.schema;

import io.delta.kernel.types.*;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dynamic schema management system that converts Avro schemas to Delta Lake schemas automatically.
 * Replaces hard-coded schema mapping with intelligent dynamic conversion.
 */
@Component
@Slf4j
public class DeltaSchemaManager {
    
    private final Map<String, StructType> schemaCache = new ConcurrentHashMap<>();
    private final AvroToDeltaSchemaConverter schemaConverter;
    private final SchemaCompatibilityChecker compatibilityChecker;
    
    public DeltaSchemaManager() {
        this.schemaConverter = new AvroToDeltaSchemaConverter();
        this.compatibilityChecker = new SchemaCompatibilityChecker();
    }
    
    /**
     * Gets or creates a Delta schema from an Avro schema with caching
     */
    public StructType getOrCreateDeltaSchema(Schema avroSchema) {
        String schemaKey = generateSchemaKey(avroSchema);
        
        return schemaCache.computeIfAbsent(schemaKey, k -> {
            log.info("Converting Avro schema to Delta schema for: {}", avroSchema.getFullName());
            StructType deltaSchema = schemaConverter.convertSchema(avroSchema);
            log.debug("Successfully converted schema with {} fields", deltaSchema.fields().size());
            return deltaSchema;
        });
    }
    
    /**
     * Validates schema compatibility for evolution
     */
    public boolean isSchemaCompatible(Schema oldSchema, Schema newSchema) {
        return compatibilityChecker.isCompatible(oldSchema, newSchema);
    }
    
    /**
     * Invalidates cached schema to force refresh
     */
    public void invalidateSchema(Schema avroSchema) {
        String schemaKey = generateSchemaKey(avroSchema);
        schemaCache.remove(schemaKey);
        log.debug("Invalidated cached schema for: {}", avroSchema.getFullName());
    }
    
    /**
     * Gets current cache statistics
     */
    public Map<String, Object> getCacheStats() {
        return Map.of(
            "cached_schemas", schemaCache.size(),
            "schema_names", schemaCache.keySet()
        );
    }
    
    private String generateSchemaKey(Schema avroSchema) {
        // Use full name + fingerprint for robust caching
        return avroSchema.getFullName() + "_" + Math.abs(avroSchema.toString().hashCode());
    }
}