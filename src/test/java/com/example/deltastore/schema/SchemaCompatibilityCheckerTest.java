package com.example.deltastore.schema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class SchemaCompatibilityCheckerTest {

    private SchemaCompatibilityChecker checker;

    @BeforeEach
    void setUp() {
        checker = new SchemaCompatibilityChecker();
    }

    @Test
    void testCompatibleSchemas() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": ["null", "string"], "default": null}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        assertTrue(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testIncompatibleSchemasFieldRemoved() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        assertFalse(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testIncompatibleSchemasDifferentTypes() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "age", "type": "int"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "age", "type": "string"}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        assertFalse(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testCompatibleWithFieldTypeChangeToNullable() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "email", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "email", "type": ["null", "string"], "default": null}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        // This should be compatible as we're making field nullable
        assertTrue(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testIncompatibleNewFieldWithoutDefault() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        assertFalse(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testIncompatibleSchemaTypeMismatch() {
        String recordSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        String primitiveSchemaJson = """
            {
                "type": "string"
            }
            """;
        
        Schema recordSchema = new Schema.Parser().parse(recordSchemaJson);
        Schema primitiveSchema = new Schema.Parser().parse(primitiveSchemaJson);
        
        assertFalse(checker.isCompatible(recordSchema, primitiveSchema));
    }

    @Test
    void testCompatibleIdenticalSchemas() {
        String schemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": ["null", "string"], "default": null}
                ]
            }
            """;
        
        Schema schema1 = new Schema.Parser().parse(schemaJson);
        Schema schema2 = new Schema.Parser().parse(schemaJson);
        
        assertTrue(checker.isCompatible(schema1, schema2));
    }

    @Test
    void testPrimitiveSchemaCompatibility() {
        Schema stringSchema1 = new Schema.Parser().parse("\"string\"");
        Schema stringSchema2 = new Schema.Parser().parse("\"string\"");
        
        assertTrue(checker.isCompatible(stringSchema1, stringSchema2));
        
        Schema intSchema = new Schema.Parser().parse("\"int\"");
        assertFalse(checker.isCompatible(stringSchema1, intSchema));
    }

    @Test
    void testCompatibilityWithMultipleNewFields() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": ["null", "string"], "default": null},
                    {"name": "email", "type": ["null", "string"], "default": null},
                    {"name": "age", "type": ["null", "int"], "default": null}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        assertTrue(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testIncompatibilityWithMixedNewFields() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        String newSchemaJson = """
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": ["null", "string"], "default": null},
                    {"name": "required_field", "type": "string"}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        // Should be incompatible because required_field has no default
        assertFalse(checker.isCompatible(oldSchema, newSchema));
    }

    @Test
    void testHandleExceptionInCompatibilityCheck() {
        // Test null schema handling - both null should be compatible
        assertTrue(checker.isCompatible(null, null));
        
        // Test null old schema with new schema - should be compatible
        String schemaJson = """
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [{"name": "id", "type": "string"}]
            }
            """;
        Schema newSchema = new Schema.Parser().parse(schemaJson);
        assertTrue(checker.isCompatible(null, newSchema));
        
        // Test existing schema with null new schema - should be incompatible
        assertFalse(checker.isCompatible(newSchema, null));
    }
}