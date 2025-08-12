package com.example.deltastore.schema;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

/**
 * Checks schema compatibility for evolution scenarios.
 * Implements backward compatibility rules for Avro schemas.
 */
@Component
@Slf4j
public class SchemaCompatibilityChecker {
    
    /**
     * Checks if new schema is backward compatible with old schema
     */
    public boolean isCompatible(Schema oldSchema, Schema newSchema) {
        try {
            // CRITICAL FIX: Add input validation
            if (oldSchema == null && newSchema == null) {
                log.debug("Both schemas are null, treating as compatible");
                return true;
            }
            
            if (oldSchema == null) {
                log.debug("No old schema provided, new schema is acceptable");
                return true;
            }
            
            if (newSchema == null) {
                log.warn("Attempting to replace existing schema with null schema");
                return false;
            }
            
            return checkBackwardCompatibility(oldSchema, newSchema);
        } catch (Exception e) {
            log.error("Error checking schema compatibility", e);
            return false;
        }
    }
    
    /**
     * Performs backward compatibility check
     */
    private boolean checkBackwardCompatibility(Schema oldSchema, Schema newSchema) {
        // CRITICAL FIX: Handle null schemas
        if (oldSchema == null && newSchema == null) {
            return true; // Both null = compatible
        }
        
        if (oldSchema == null) {
            log.info("No old schema to compare against, treating new schema as compatible");
            return true; // No old schema = new schema is acceptable
        }
        
        if (newSchema == null) {
            log.warn("New schema is null but old schema exists, not compatible");
            return false; // Can't replace existing schema with null
        }
        
        // Basic type compatibility
        if (oldSchema.getType() != newSchema.getType()) {
            log.warn("Schema type changed from {} to {}", oldSchema.getType(), newSchema.getType());
            return false;
        }
        
        if (oldSchema.getType() == Schema.Type.RECORD) {
            return checkRecordCompatibility(oldSchema, newSchema);
        }
        
        // For primitive types, schemas should be identical
        return oldSchema.equals(newSchema);
    }
    
    /**
     * Checks record schema compatibility
     */
    private boolean checkRecordCompatibility(Schema oldSchema, Schema newSchema) {
        Set<String> oldFields = oldSchema.getFields().stream()
            .map(Schema.Field::name)
            .collect(Collectors.toSet());
        
        Set<String> newFields = newSchema.getFields().stream()
            .map(Schema.Field::name)
            .collect(Collectors.toSet());
        
        // Check for removed fields (not allowed in backward compatibility)
        Set<String> removedFields = new HashSet<>(oldFields);
        removedFields.removeAll(newFields);
        
        if (!removedFields.isEmpty()) {
            log.warn("Fields removed in new schema: {}", removedFields);
            return false;
        }
        
        // Check field type compatibility for existing fields
        for (Schema.Field oldField : oldSchema.getFields()) {
            Schema.Field newField = newSchema.getField(oldField.name());
            if (newField != null) {
                if (!areFieldTypesCompatible(oldField.schema(), newField.schema())) {
                    log.warn("Field type changed for field: {}", oldField.name());
                    return false;
                }
            }
        }
        
        // Check that new fields have default values
        for (Schema.Field newField : newSchema.getFields()) {
            if (!oldFields.contains(newField.name()) && !newField.hasDefaultValue()) {
                log.warn("New field {} added without default value", newField.name());
                return false;
            }
        }
        
        log.info("Schema compatibility check passed");
        return true;
    }
    
    /**
     * Checks if two field types are compatible
     */
    private boolean areFieldTypesCompatible(Schema oldFieldSchema, Schema newFieldSchema) {
        // Handle union types (nullable fields)
        if (oldFieldSchema.getType() == Schema.Type.UNION) {
            oldFieldSchema = getNonNullType(oldFieldSchema);
        }
        if (newFieldSchema.getType() == Schema.Type.UNION) {
            newFieldSchema = getNonNullType(newFieldSchema);
        }
        
        // Basic type equality check
        if (oldFieldSchema.getType() != newFieldSchema.getType()) {
            return false;
        }
        
        // For primitive types
        if (isPrimitiveType(oldFieldSchema.getType())) {
            return true;
        }
        
        // For complex types, recursive check would be needed
        // For simplicity, we'll consider them compatible if types match
        return oldFieldSchema.getType() == newFieldSchema.getType();
    }
    
    /**
     * Gets non-null type from union (for nullable fields)
     */
    private Schema getNonNullType(Schema unionSchema) {
        if (unionSchema.getType() != Schema.Type.UNION) {
            return unionSchema;
        }
        
        return unionSchema.getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .orElse(unionSchema);
    }
    
    /**
     * Checks if a type is primitive
     */
    private boolean isPrimitiveType(Schema.Type type) {
        return type == Schema.Type.STRING ||
               type == Schema.Type.INT ||
               type == Schema.Type.LONG ||
               type == Schema.Type.FLOAT ||
               type == Schema.Type.DOUBLE ||
               type == Schema.Type.BOOLEAN ||
               type == Schema.Type.BYTES;
    }
}