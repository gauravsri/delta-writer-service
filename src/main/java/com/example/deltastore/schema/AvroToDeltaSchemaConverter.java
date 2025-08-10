package com.example.deltastore.schema;

import io.delta.kernel.types.*;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Converts Avro schemas to Delta Lake schemas dynamically.
 * Supports all common Avro types and handles nullable fields correctly.
 */
@Component
@Slf4j
public class AvroToDeltaSchemaConverter {
    
    private static final Map<Schema.Type, DataType> TYPE_MAPPING = new HashMap<>();
    
    static {
        TYPE_MAPPING.put(Schema.Type.STRING, StringType.STRING);
        TYPE_MAPPING.put(Schema.Type.INT, IntegerType.INTEGER);
        TYPE_MAPPING.put(Schema.Type.LONG, LongType.LONG);
        TYPE_MAPPING.put(Schema.Type.FLOAT, FloatType.FLOAT);
        TYPE_MAPPING.put(Schema.Type.DOUBLE, DoubleType.DOUBLE);
        TYPE_MAPPING.put(Schema.Type.BOOLEAN, BooleanType.BOOLEAN);
        TYPE_MAPPING.put(Schema.Type.BYTES, BinaryType.BINARY);
    }
    
    /**
     * Converts an Avro schema to a Delta StructType
     */
    public StructType convertSchema(Schema avroSchema) {
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Can only convert RECORD type schemas");
        }
        
        StructType struct = new StructType();
        
        for (Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            DataType deltaType = convertFieldType(field.schema());
            boolean nullable = isNullable(field.schema());
            
            struct = struct.add(fieldName, deltaType, nullable);
            log.debug("Converted field: {} -> {} (nullable: {})", fieldName, deltaType, nullable);
        }
        
        return struct;
    }
    
    /**
     * Converts an Avro field type to Delta DataType
     */
    private DataType convertFieldType(Schema fieldSchema) {
        Schema.Type type = fieldSchema.getType();
        
        switch (type) {
            case UNION:
                return handleUnionType(fieldSchema);
            case RECORD:
                return handleNestedRecord(fieldSchema);
            case ARRAY:
                return handleArrayType(fieldSchema);
            case MAP:
                return handleMapType(fieldSchema);
            case ENUM:
                return StringType.STRING; // Enums as strings
            default:
                return TYPE_MAPPING.getOrDefault(type, StringType.STRING);
        }
    }
    
    /**
     * Handles nullable union types (e.g., ["null", "string"])
     */
    private DataType handleUnionType(Schema unionSchema) {
        List<Schema> types = unionSchema.getTypes();
        
        // Find non-null type in union
        for (Schema type : types) {
            if (type.getType() != Schema.Type.NULL) {
                return convertFieldType(type);
            }
        }
        
        // If only null, default to string
        return StringType.STRING;
    }
    
    /**
     * Handles nested record types
     */
    private DataType handleNestedRecord(Schema recordSchema) {
        // For now, convert nested records to JSON strings
        // In a full implementation, this would be recursive StructType
        log.debug("Converting nested record {} to string (JSON)", recordSchema.getName());
        return StringType.STRING;
    }
    
    /**
     * Handles array types
     */
    private DataType handleArrayType(Schema arraySchema) {
        DataType elementType = convertFieldType(arraySchema.getElementType());
        boolean containsNull = isNullable(arraySchema.getElementType());
        return new ArrayType(elementType, containsNull);
    }
    
    /**
     * Handles map types
     */
    private DataType handleMapType(Schema mapSchema) {
        DataType valueType = convertFieldType(mapSchema.getValueType());
        boolean valueContainsNull = isNullable(mapSchema.getValueType());
        return new MapType(StringType.STRING, valueType, valueContainsNull);
    }
    
    /**
     * Determines if a schema field is nullable
     */
    private boolean isNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                .anyMatch(s -> s.getType() == Schema.Type.NULL);
        }
        return false;
    }
}