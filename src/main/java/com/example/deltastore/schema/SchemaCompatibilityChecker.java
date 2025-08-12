package com.example.deltastore.schema;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Checks schema compatibility for evolution scenarios.
 * Implements comprehensive compatibility rules for Avro schemas (HIGH PRIORITY ISSUE #8: Enhanced).
 */
@Component
@Slf4j
public class SchemaCompatibilityChecker {
    
    // HIGH PRIORITY ISSUE #8: Add compatibility metrics and configurable policies
    private final AtomicLong compatibilityChecksTotal = new AtomicLong();
    private final AtomicLong compatibilityChecksPassed = new AtomicLong();
    private final AtomicLong compatibilityChecksFailed = new AtomicLong();
    private final AtomicLong backwardCompatibilityChecks = new AtomicLong();
    private final AtomicLong forwardCompatibilityChecks = new AtomicLong();
    private final AtomicLong fullCompatibilityChecks = new AtomicLong();
    
    /**
     * Schema compatibility types (HIGH PRIORITY ISSUE #8)
     */
    public enum CompatibilityType {
        BACKWARD,    // New schema can read data written with old schema
        FORWARD,     // Old schema can read data written with new schema  
        FULL,        // Both backward and forward compatible
        NONE         // No compatibility checking
    }
    
    /**
     * Checks if new schema is backward compatible with old schema (HIGH PRIORITY ISSUE #8: Enhanced)
     */
    public boolean isCompatible(Schema oldSchema, Schema newSchema) {
        return isCompatible(oldSchema, newSchema, CompatibilityType.BACKWARD);
    }
    
    /**
     * Checks schema compatibility with configurable compatibility type (HIGH PRIORITY ISSUE #8)
     */
    public boolean isCompatible(Schema oldSchema, Schema newSchema, CompatibilityType compatibilityType) {
        compatibilityChecksTotal.incrementAndGet();
        
        try {
            // CRITICAL FIX: Add input validation
            if (oldSchema == null && newSchema == null) {
                log.debug("Both schemas are null, treating as compatible");
                compatibilityChecksPassed.incrementAndGet();
                return true;
            }
            
            if (oldSchema == null) {
                log.debug("No old schema provided, new schema is acceptable");
                compatibilityChecksPassed.incrementAndGet();
                return true;
            }
            
            if (newSchema == null) {
                log.warn("Attempting to replace existing schema with null schema");
                compatibilityChecksFailed.incrementAndGet();
                return false;
            }
            
            boolean compatible = performCompatibilityCheck(oldSchema, newSchema, compatibilityType);
            
            if (compatible) {
                compatibilityChecksPassed.incrementAndGet();
            } else {
                compatibilityChecksFailed.incrementAndGet();
            }
            
            return compatible;
            
        } catch (Exception e) {
            log.error("Error checking schema compatibility", e);
            compatibilityChecksFailed.incrementAndGet();
            return false;
        }
    }
    
    /**
     * Detailed compatibility result with reasons (HIGH PRIORITY ISSUE #8)
     */
    public static class CompatibilityResult {
        private final boolean compatible;
        private final List<String> issues;
        private final List<String> warnings;
        private final CompatibilityType checkedType;
        
        public CompatibilityResult(boolean compatible, List<String> issues, List<String> warnings, CompatibilityType checkedType) {
            this.compatible = compatible;
            this.issues = new ArrayList<>(issues);
            this.warnings = new ArrayList<>(warnings);
            this.checkedType = checkedType;
        }
        
        public boolean isCompatible() { return compatible; }
        public List<String> getIssues() { return new ArrayList<>(issues); }
        public List<String> getWarnings() { return new ArrayList<>(warnings); }
        public CompatibilityType getCheckedType() { return checkedType; }
    }
    
    /**
     * Detailed compatibility check with full analysis (HIGH PRIORITY ISSUE #8)
     */
    public CompatibilityResult checkCompatibilityDetailed(Schema oldSchema, Schema newSchema, CompatibilityType compatibilityType) {
        compatibilityChecksTotal.incrementAndGet();
        
        List<String> issues = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        
        try {
            if (oldSchema == null && newSchema == null) {
                compatibilityChecksPassed.incrementAndGet();
                return new CompatibilityResult(true, issues, warnings, compatibilityType);
            }
            
            if (oldSchema == null) {
                warnings.add("No old schema provided, accepting new schema");
                compatibilityChecksPassed.incrementAndGet();
                return new CompatibilityResult(true, issues, warnings, compatibilityType);
            }
            
            if (newSchema == null) {
                issues.add("Cannot replace existing schema with null schema");
                compatibilityChecksFailed.incrementAndGet();
                return new CompatibilityResult(false, issues, warnings, compatibilityType);
            }
            
            boolean compatible = performDetailedCompatibilityCheck(oldSchema, newSchema, compatibilityType, issues, warnings);
            
            if (compatible) {
                compatibilityChecksPassed.incrementAndGet();
            } else {
                compatibilityChecksFailed.incrementAndGet();
            }
            
            return new CompatibilityResult(compatible, issues, warnings, compatibilityType);
            
        } catch (Exception e) {
            issues.add("Error during compatibility check: " + e.getMessage());
            log.error("Error checking detailed schema compatibility", e);
            compatibilityChecksFailed.incrementAndGet();
            return new CompatibilityResult(false, issues, warnings, compatibilityType);
        }
    }
    
    /**
     * Performs compatibility check based on type (HIGH PRIORITY ISSUE #8)
     */
    private boolean performCompatibilityCheck(Schema oldSchema, Schema newSchema, CompatibilityType compatibilityType) {
        switch (compatibilityType) {
            case BACKWARD:
                backwardCompatibilityChecks.incrementAndGet();
                return checkBackwardCompatibility(oldSchema, newSchema);
            case FORWARD:
                forwardCompatibilityChecks.incrementAndGet();
                return checkForwardCompatibility(oldSchema, newSchema);
            case FULL:
                fullCompatibilityChecks.incrementAndGet();
                return checkBackwardCompatibility(oldSchema, newSchema) && 
                       checkForwardCompatibility(oldSchema, newSchema);
            case NONE:
                return true; // No compatibility checking
            default:
                log.warn("Unknown compatibility type: {}, defaulting to BACKWARD", compatibilityType);
                return checkBackwardCompatibility(oldSchema, newSchema);
        }
    }
    
    /**
     * Performs detailed compatibility check with issues tracking (HIGH PRIORITY ISSUE #8)
     */
    private boolean performDetailedCompatibilityCheck(Schema oldSchema, Schema newSchema, 
            CompatibilityType compatibilityType, List<String> issues, List<String> warnings) {
        switch (compatibilityType) {
            case BACKWARD:
                backwardCompatibilityChecks.incrementAndGet();
                return checkBackwardCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
            case FORWARD:
                forwardCompatibilityChecks.incrementAndGet();
                return checkForwardCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
            case FULL:
                fullCompatibilityChecks.incrementAndGet();
                boolean backwardCompatible = checkBackwardCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
                boolean forwardCompatible = checkForwardCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
                return backwardCompatible && forwardCompatible;
            case NONE:
                warnings.add("No compatibility checking performed");
                return true;
            default:
                issues.add("Unknown compatibility type: " + compatibilityType);
                return false;
        }
    }
    
    /**
     * Performs backward compatibility check (LEGACY - HIGH PRIORITY ISSUE #8: Enhanced)
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
     * Forward compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkForwardCompatibility(Schema oldSchema, Schema newSchema) {
        // Forward compatibility: old schema can read data written with new schema
        // This is essentially backward compatibility checked in reverse
        return checkBackwardCompatibility(newSchema, oldSchema);
    }
    
    /**
     * Detailed backward compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkBackwardCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        if (oldSchema == null && newSchema == null) {
            return true;
        }
        
        if (oldSchema == null) {
            warnings.add("No old schema to compare against, accepting new schema");
            return true;
        }
        
        if (newSchema == null) {
            issues.add("Cannot replace existing schema with null");
            return false;
        }
        
        // Basic type compatibility
        if (oldSchema.getType() != newSchema.getType()) {
            issues.add("Schema type changed from " + oldSchema.getType() + " to " + newSchema.getType());
            return false;
        }
        
        if (oldSchema.getType() == Schema.Type.RECORD) {
            return checkRecordCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
        } else if (oldSchema.getType() == Schema.Type.ARRAY) {
            return checkArrayCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
        } else if (oldSchema.getType() == Schema.Type.MAP) {
            return checkMapCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
        } else if (oldSchema.getType() == Schema.Type.UNION) {
            return checkUnionCompatibilityDetailed(oldSchema, newSchema, issues, warnings);
        }
        
        // For primitive types, schemas should be identical
        boolean compatible = oldSchema.equals(newSchema);
        if (!compatible) {
            issues.add("Primitive schemas are not identical");
        }
        return compatible;
    }
    
    /**
     * Detailed forward compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkForwardCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        List<String> tempIssues = new ArrayList<>();
        List<String> tempWarnings = new ArrayList<>();
        
        // Forward compatibility is backward compatibility in reverse
        boolean compatible = checkBackwardCompatibilityDetailed(newSchema, oldSchema, tempIssues, tempWarnings);
        
        // Relabel issues for forward compatibility context
        for (String issue : tempIssues) {
            issues.add("Forward compatibility: " + issue);
        }
        for (String warning : tempWarnings) {
            warnings.add("Forward compatibility: " + warning);
        }
        
        return compatible;
    }
    
    /**
     * Enhanced record compatibility check with detailed reporting (HIGH PRIORITY ISSUE #8)
     */
    private boolean checkRecordCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        Set<String> oldFields = oldSchema.getFields().stream()
            .map(Schema.Field::name)
            .collect(Collectors.toSet());
        
        Set<String> newFields = newSchema.getFields().stream()
            .map(Schema.Field::name)
            .collect(Collectors.toSet());
        
        boolean compatible = true;
        
        // Check for removed fields (not allowed in backward compatibility)
        Set<String> removedFields = new HashSet<>(oldFields);
        removedFields.removeAll(newFields);
        
        if (!removedFields.isEmpty()) {
            issues.add("Fields removed in new schema: " + removedFields);
            compatible = false;
        }
        
        // Check field type compatibility for existing fields
        for (Schema.Field oldField : oldSchema.getFields()) {
            Schema.Field newField = newSchema.getField(oldField.name());
            if (newField != null) {
                List<String> fieldIssues = new ArrayList<>();
                List<String> fieldWarnings = new ArrayList<>();
                
                if (!areFieldTypesCompatibleDetailed(oldField.schema(), newField.schema(), fieldIssues, fieldWarnings)) {
                    issues.add("Field '" + oldField.name() + "': " + String.join(", ", fieldIssues));
                    compatible = false;
                }
                
                if (!fieldWarnings.isEmpty()) {
                    warnings.add("Field '" + oldField.name() + "': " + String.join(", ", fieldWarnings));
                }
            }
        }
        
        // Check that new fields have default values
        Set<String> newFieldsOnly = new HashSet<>(newFields);
        newFieldsOnly.removeAll(oldFields);
        
        for (String newFieldName : newFieldsOnly) {
            Schema.Field newField = newSchema.getField(newFieldName);
            if (!newField.hasDefaultValue()) {
                issues.add("New field '" + newFieldName + "' added without default value");
                compatible = false;
            } else {
                warnings.add("New field '" + newFieldName + "' added with default value");
            }
        }
        
        if (compatible) {
            log.debug("Record schema compatibility check passed for {} fields", oldFields.size());
        }
        
        return compatible;
    }
    
    /**
     * Array compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkArrayCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        Schema oldElement = oldSchema.getElementType();
        Schema newElement = newSchema.getElementType();
        
        List<String> elementIssues = new ArrayList<>();
        List<String> elementWarnings = new ArrayList<>();
        
        if (!areFieldTypesCompatibleDetailed(oldElement, newElement, elementIssues, elementWarnings)) {
            issues.add("Array element types incompatible: " + String.join(", ", elementIssues));
            return false;
        }
        
        if (!elementWarnings.isEmpty()) {
            warnings.add("Array element types: " + String.join(", ", elementWarnings));
        }
        
        return true;
    }
    
    /**
     * Map compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkMapCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        Schema oldValue = oldSchema.getValueType();
        Schema newValue = newSchema.getValueType();
        
        List<String> valueIssues = new ArrayList<>();
        List<String> valueWarnings = new ArrayList<>();
        
        if (!areFieldTypesCompatibleDetailed(oldValue, newValue, valueIssues, valueWarnings)) {
            issues.add("Map value types incompatible: " + String.join(", ", valueIssues));
            return false;
        }
        
        if (!valueWarnings.isEmpty()) {
            warnings.add("Map value types: " + String.join(", ", valueWarnings));
        }
        
        return true;
    }
    
    /**
     * Union compatibility check (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean checkUnionCompatibilityDetailed(Schema oldSchema, Schema newSchema, 
            List<String> issues, List<String> warnings) {
        
        List<Schema> oldTypes = oldSchema.getTypes();
        List<Schema> newTypes = newSchema.getTypes();
        
        // For backward compatibility, all old union types must be present in new union
        Set<String> oldTypeNames = oldTypes.stream()
            .map(s -> s.getType().getName())
            .collect(Collectors.toSet());
        
        Set<String> newTypeNames = newTypes.stream()
            .map(s -> s.getType().getName())
            .collect(Collectors.toSet());
        
        Set<String> removedTypes = new HashSet<>(oldTypeNames);
        removedTypes.removeAll(newTypeNames);
        
        if (!removedTypes.isEmpty()) {
            issues.add("Union types removed: " + removedTypes);
            return false;
        }
        
        Set<String> addedTypes = new HashSet<>(newTypeNames);
        addedTypes.removeAll(oldTypeNames);
        
        if (!addedTypes.isEmpty()) {
            warnings.add("Union types added: " + addedTypes);
        }
        
        return true;
    }
    
    /**
     * Enhanced field type compatibility check (HIGH PRIORITY ISSUE #8)
     */
    private boolean areFieldTypesCompatibleDetailed(Schema oldFieldSchema, Schema newFieldSchema, 
            List<String> issues, List<String> warnings) {
        
        // Handle union types (nullable fields)
        Schema oldNonNull = oldFieldSchema;
        Schema newNonNull = newFieldSchema;
        boolean oldNullable = false;
        boolean newNullable = false;
        
        if (oldFieldSchema.getType() == Schema.Type.UNION) {
            oldNonNull = getNonNullType(oldFieldSchema);
            oldNullable = isNullable(oldFieldSchema);
        }
        if (newFieldSchema.getType() == Schema.Type.UNION) {
            newNonNull = getNonNullType(newFieldSchema);
            newNullable = isNullable(newFieldSchema);
        }
        
        // Check nullability changes
        if (oldNullable && !newNullable) {
            warnings.add("Field changed from nullable to non-nullable");
        } else if (!oldNullable && newNullable) {
            warnings.add("Field changed from non-nullable to nullable");
        }
        
        // Basic type equality check
        if (oldNonNull.getType() != newNonNull.getType()) {
            issues.add("Type changed from " + oldNonNull.getType() + " to " + newNonNull.getType());
            return false;
        }
        
        // For primitive types
        if (isPrimitiveType(oldNonNull.getType())) {
            return true;
        }
        
        // For complex types, check recursively
        if (oldNonNull.getType() == Schema.Type.RECORD) {
            return checkRecordCompatibilityDetailed(oldNonNull, newNonNull, issues, warnings);
        } else if (oldNonNull.getType() == Schema.Type.ARRAY) {
            return checkArrayCompatibilityDetailed(oldNonNull, newNonNull, issues, warnings);
        } else if (oldNonNull.getType() == Schema.Type.MAP) {
            return checkMapCompatibilityDetailed(oldNonNull, newNonNull, issues, warnings);
        }
        
        return oldNonNull.getType() == newNonNull.getType();
    }
    
    /**
     * Checks if union type is nullable (HIGH PRIORITY ISSUE #8: NEW)
     */
    private boolean isNullable(Schema unionSchema) {
        if (unionSchema.getType() != Schema.Type.UNION) {
            return false;
        }
        
        return unionSchema.getTypes().stream()
            .anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
    
    /**
     * Gets compatibility metrics (HIGH PRIORITY ISSUE #8: NEW)
     */
    public Map<String, Object> getCompatibilityMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        long total = compatibilityChecksTotal.get();
        long passed = compatibilityChecksPassed.get();
        long failed = compatibilityChecksFailed.get();
        
        metrics.put("total_compatibility_checks", total);
        metrics.put("compatibility_checks_passed", passed);
        metrics.put("compatibility_checks_failed", failed);
        metrics.put("compatibility_success_rate", total > 0 ? (double) passed / total : 0.0);
        
        metrics.put("backward_compatibility_checks", backwardCompatibilityChecks.get());
        metrics.put("forward_compatibility_checks", forwardCompatibilityChecks.get());
        metrics.put("full_compatibility_checks", fullCompatibilityChecks.get());
        
        return metrics;
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
               type == Schema.Type.BYTES ||
               type == Schema.Type.FIXED;
    }
}