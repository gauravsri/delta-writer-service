package com.example.deltastore.schema;

import io.delta.kernel.types.*;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class AvroToDeltaSchemaConverterTest {

    private AvroToDeltaSchemaConverter converter;

    @BeforeEach
    void setUp() {
        converter = new AvroToDeltaSchemaConverter();
    }

    @Test
    void testConvertSimpleRecordSchema() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "SimpleRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean"}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertNotNull(deltaSchema);
        assertEquals(3, deltaSchema.fields().size());
        
        // Verify field conversions
        assertEquals("id", deltaSchema.fields().get(0).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(0).getDataType());
        assertFalse(deltaSchema.fields().get(0).isNullable());
        
        assertEquals("age", deltaSchema.fields().get(1).getName());
        assertEquals(IntegerType.INTEGER, deltaSchema.fields().get(1).getDataType());
        assertFalse(deltaSchema.fields().get(1).isNullable());
        
        assertEquals("active", deltaSchema.fields().get(2).getName());
        assertEquals(BooleanType.BOOLEAN, deltaSchema.fields().get(2).getDataType());
        assertFalse(deltaSchema.fields().get(2).isNullable());
    }

    @Test
    void testConvertWithNullableFields() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "NullableRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "email", "type": ["null", "string"], "default": null},
                    {"name": "score", "type": ["null", "double"], "default": null}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(3, deltaSchema.fields().size());
        
        // Non-nullable field
        assertEquals("id", deltaSchema.fields().get(0).getName());
        assertFalse(deltaSchema.fields().get(0).isNullable());
        
        // Nullable fields
        assertEquals("email", deltaSchema.fields().get(1).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(1).getDataType());
        assertTrue(deltaSchema.fields().get(1).isNullable());
        
        assertEquals("score", deltaSchema.fields().get(2).getName());
        assertEquals(DoubleType.DOUBLE, deltaSchema.fields().get(2).getDataType());
        assertTrue(deltaSchema.fields().get(2).isNullable());
    }

    @Test
    void testConvertWithAllDataTypes() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "AllTypesRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "str_field", "type": "string"},
                    {"name": "int_field", "type": "int"},
                    {"name": "long_field", "type": "long"},
                    {"name": "float_field", "type": "float"},
                    {"name": "double_field", "type": "double"},
                    {"name": "boolean_field", "type": "boolean"},
                    {"name": "bytes_field", "type": "bytes"}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(7, deltaSchema.fields().size());
        
        assertEquals(StringType.STRING, deltaSchema.fields().get(0).getDataType());
        assertEquals(IntegerType.INTEGER, deltaSchema.fields().get(1).getDataType());
        assertEquals(LongType.LONG, deltaSchema.fields().get(2).getDataType());
        assertEquals(FloatType.FLOAT, deltaSchema.fields().get(3).getDataType());
        assertEquals(DoubleType.DOUBLE, deltaSchema.fields().get(4).getDataType());
        assertEquals(BooleanType.BOOLEAN, deltaSchema.fields().get(5).getDataType());
        assertEquals(BinaryType.BINARY, deltaSchema.fields().get(6).getDataType());
    }

    @Test
    void testConvertWithArrayField() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "ArrayRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(2, deltaSchema.fields().size());
        
        assertEquals("id", deltaSchema.fields().get(0).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(0).getDataType());
        
        assertEquals("tags", deltaSchema.fields().get(1).getName());
        assertTrue(deltaSchema.fields().get(1).getDataType() instanceof ArrayType);
        
        ArrayType arrayType = (ArrayType) deltaSchema.fields().get(1).getDataType();
        assertEquals(StringType.STRING, arrayType.getElementType());
        assertFalse(arrayType.containsNull()); // String elements are not nullable
    }

    @Test
    void testConvertWithMapField() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "MapRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "metadata", "type": {"type": "map", "values": "string"}}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(2, deltaSchema.fields().size());
        
        assertEquals("metadata", deltaSchema.fields().get(1).getName());
        assertTrue(deltaSchema.fields().get(1).getDataType() instanceof MapType);
        
        MapType mapType = (MapType) deltaSchema.fields().get(1).getDataType();
        assertEquals(StringType.STRING, mapType.getKeyType()); // Maps always have string keys
        assertEquals(StringType.STRING, mapType.getValueType());
        assertFalse(mapType.isValueContainsNull()); // String values are not nullable
    }

    @Test
    void testConvertWithEnumField() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "EnumRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE", "PENDING"]}}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(2, deltaSchema.fields().size());
        
        assertEquals("status", deltaSchema.fields().get(1).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(1).getDataType()); // Enums become strings
    }

    @Test
    void testConvertWithNestedRecord() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "NestedRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "address", "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"}
                        ]
                    }}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(2, deltaSchema.fields().size());
        
        assertEquals("address", deltaSchema.fields().get(1).getName());
        // Nested records are converted to strings (JSON) for simplicity
        assertEquals(StringType.STRING, deltaSchema.fields().get(1).getDataType());
    }

    @Test
    void testConvertNonRecordSchemaThrowsException() {
        String avroSchemaJson = """
            {
                "type": "string"
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        assertThrows(IllegalArgumentException.class, () -> {
            converter.convertSchema(avroSchema);
        });
    }

    @Test
    void testConvertEmptyRecordSchema() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "EmptyRecord",
                "namespace": "com.example.test",
                "fields": []
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertNotNull(deltaSchema);
        assertEquals(0, deltaSchema.fields().size());
    }

    @Test
    void testConvertWithComplexUnion() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "ComplexUnionRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": ["null", "string", "int"]}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = converter.convertSchema(avroSchema);
        
        assertEquals(2, deltaSchema.fields().size());
        
        assertEquals("value", deltaSchema.fields().get(1).getName());
        // Complex unions default to string type with nullable=true
        assertEquals(StringType.STRING, deltaSchema.fields().get(1).getDataType());
        assertTrue(deltaSchema.fields().get(1).isNullable());
    }
}