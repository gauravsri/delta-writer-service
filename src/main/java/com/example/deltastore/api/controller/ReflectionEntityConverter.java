package com.example.deltastore.api.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Generic entity converter that uses reflection to convert Map data to any Avro GenericRecord.
 * Eliminates the need for entity-specific converter classes.
 */
@Component
@Slf4j
public class ReflectionEntityConverter {

    /**
     * Converts Map data to GenericRecord using reflection on the target class.
     * Works with any Avro-generated class without requiring custom converter code.
     */
    public GenericRecord convertFromMap(Map<String, Object> entityData, Class<?> avroClass) {
        try {
            // Get the Avro schema from the class
            Schema schema = getSchemaFromClass(avroClass);
            
            // Create a new GenericRecord instance
            GenericRecord record = new GenericData.Record(schema);
            
            // Populate fields using schema information
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();
                Object value = entityData.get(fieldName);
                
                if (value != null) {
                    // Handle type conversion based on schema
                    Object convertedValue = convertValue(value, field.schema());
                    record.put(fieldName, convertedValue);
                }
            }
            
            return record;
            
        } catch (Exception e) {
            log.error("Failed to convert map to {} using reflection", avroClass.getSimpleName(), e);
            throw new RuntimeException("Entity conversion failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Alternative method that creates specific Avro objects if needed
     */
    public GenericRecord convertFromMapToSpecific(Map<String, Object> entityData, Class<?> avroClass) {
        try {
            // Try to use the builder pattern if available
            Method builderMethod = avroClass.getMethod("newBuilder");
            Object builder = builderMethod.invoke(null);
            
            // Get all setter methods from the builder
            Class<?> builderClass = builder.getClass();
            
            for (Map.Entry<String, Object> entry : entityData.entrySet()) {
                String fieldName = entry.getKey();
                Object value = entry.getValue();
                
                if (value != null) {
                    try {
                        // Convert camelCase to setter method name
                        String setterName = "set" + Character.toUpperCase(fieldName.charAt(0)) + 
                                          fieldName.substring(1);
                        
                        Method setter = findSetter(builderClass, setterName, value.getClass());
                        if (setter != null) {
                            setter.invoke(builder, value);
                        } else {
                            log.warn("No setter found for field {} in {}", fieldName, avroClass.getSimpleName());
                        }
                    } catch (Exception e) {
                        log.warn("Failed to set field {} in {}: {}", fieldName, avroClass.getSimpleName(), e.getMessage());
                    }
                }
            }
            
            // Build the final object
            Method buildMethod = builderClass.getMethod("build");
            return (GenericRecord) buildMethod.invoke(builder);
            
        } catch (Exception e) {
            log.error("Failed to convert map to {} using builder pattern", avroClass.getSimpleName(), e);
            // Fallback to generic approach
            return convertFromMap(entityData, avroClass);
        }
    }
    
    private Schema getSchemaFromClass(Class<?> avroClass) throws Exception {
        // Try to get schema from the class - Avro generated classes have getClassSchema method
        if (SpecificRecordBase.class.isAssignableFrom(avroClass)) {
            Method getSchemaMethod = avroClass.getMethod("getClassSchema");
            return (Schema) getSchemaMethod.invoke(null);
        } else {
            // Fallback: try to get schema from SpecificData
            return SpecificData.get().getSchema(avroClass);
        }
    }
    
    private Object convertValue(Object value, Schema fieldSchema) {
        Schema.Type type = fieldSchema.getType();
        
        // Handle union types (nullable fields)
        if (type == Schema.Type.UNION) {
            for (Schema unionSchema : fieldSchema.getTypes()) {
                if (unionSchema.getType() != Schema.Type.NULL) {
                    return convertValue(value, unionSchema);
                }
            }
        }
        
        // Convert based on schema type
        switch (type) {
            case STRING:
                return value.toString();
            case INT:
                return value instanceof Integer ? value : Integer.parseInt(value.toString());
            case LONG:
                return value instanceof Long ? value : Long.parseLong(value.toString());
            case FLOAT:
                return value instanceof Float ? value : Float.parseFloat(value.toString());
            case DOUBLE:
                return value instanceof Double ? value : Double.parseDouble(value.toString());
            case BOOLEAN:
                return value instanceof Boolean ? value : Boolean.parseBoolean(value.toString());
            default:
                return value;
        }
    }
    
    private Method findSetter(Class<?> builderClass, String setterName, Class<?> paramType) {
        try {
            // Try exact type match first
            return builderClass.getMethod(setterName, paramType);
        } catch (NoSuchMethodException e) {
            // Try common type conversions
            for (Method method : builderClass.getMethods()) {
                if (method.getName().equals(setterName) && method.getParameterCount() == 1) {
                    Class<?> expectedType = method.getParameterTypes()[0];
                    if (expectedType.isAssignableFrom(paramType) || 
                        isCompatibleType(paramType, expectedType)) {
                        return method;
                    }
                }
            }
        }
        return null;
    }
    
    private boolean isCompatibleType(Class<?> provided, Class<?> expected) {
        // Handle primitive and wrapper type conversions
        if (provided == String.class) {
            return expected == String.class || expected == CharSequence.class;
        }
        if (provided == Integer.class || provided == int.class) {
            return expected == Integer.class || expected == int.class;
        }
        if (provided == Long.class || provided == long.class) {
            return expected == Long.class || expected == long.class;
        }
        if (provided == Double.class || provided == double.class) {
            return expected == Double.class || expected == double.class;
        }
        if (provided == Float.class || provided == float.class) {
            return expected == Float.class || expected == float.class;
        }
        if (provided == Boolean.class || provided == boolean.class) {
            return expected == Boolean.class || expected == boolean.class;
        }
        
        return false;
    }

    /**
     * Entity-specific converter wrapper that uses the generic reflection converter
     * for a specific entity type. This eliminates the need for custom converter classes.
     */
    public static class EntitySpecificConverter implements EntityConverter {
        private final ReflectionEntityConverter reflectionConverter;
        private final Class<?> avroClass;
        private final String entityType;

        public EntitySpecificConverter(ReflectionEntityConverter reflectionConverter, 
                                     Class<?> avroClass) {
            this.reflectionConverter = reflectionConverter;
            this.avroClass = avroClass;
            this.entityType = avroClass.getSimpleName().toLowerCase() + "s";
        }

        @Override
        public GenericRecord convertFromMap(Map<String, Object> entityData) {
            return reflectionConverter.convertFromMapToSpecific(entityData, avroClass);
        }

        @Override
        public String getEntityType() {
            return entityType;
        }
    }
}