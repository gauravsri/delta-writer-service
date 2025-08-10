package com.example.deltastore.schema;

import io.delta.kernel.types.*;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class DeltaSchemaManagerTest {

    private DeltaSchemaManager schemaManager;

    @BeforeEach
    void setUp() {
        schemaManager = new DeltaSchemaManager();
    }

    @Test
    void testGetOrCreateDeltaSchemaFromAvroSchema() {
        // Create a simple Avro schema
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "TestRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "email", "type": ["null", "string"], "default": null}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        StructType deltaSchema = schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        assertNotNull(deltaSchema);
        assertEquals(4, deltaSchema.fields().size());
        
        // Verify field types
        assertEquals("id", deltaSchema.fields().get(0).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(0).getDataType());
        assertFalse(deltaSchema.fields().get(0).isNullable());
        
        assertEquals("name", deltaSchema.fields().get(1).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(1).getDataType());
        
        assertEquals("age", deltaSchema.fields().get(2).getName());
        assertEquals(IntegerType.INTEGER, deltaSchema.fields().get(2).getDataType());
        
        assertEquals("email", deltaSchema.fields().get(3).getName());
        assertEquals(StringType.STRING, deltaSchema.fields().get(3).getDataType());
        assertTrue(deltaSchema.fields().get(3).isNullable()); // Should be nullable due to union with null
    }

    @Test
    void testSchemaCaching() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "CacheTestRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        // First call
        StructType deltaSchema1 = schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        // Second call should return cached version
        StructType deltaSchema2 = schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        // Should be the same instance due to caching
        assertSame(deltaSchema1, deltaSchema2);
    }

    @Test
    void testInvalidateSchema() {
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "InvalidateTestRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        // Get schema (should be cached)
        StructType deltaSchema1 = schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        // Invalidate cache
        schemaManager.invalidateSchema(avroSchema);
        
        // Get schema again (should create new instance)
        StructType deltaSchema2 = schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        // Should be different instances after invalidation
        assertNotSame(deltaSchema1, deltaSchema2);
        
        // But should have same structure
        assertEquals(deltaSchema1.fields().size(), deltaSchema2.fields().size());
        assertEquals(deltaSchema1.fields().get(0).getName(), deltaSchema2.fields().get(0).getName());
    }

    @Test
    void testGetCacheStats() {
        // Initially should have 0 cached schemas
        var stats = schemaManager.getCacheStats();
        assertEquals(0, stats.get("cached_schemas"));
        assertTrue(((java.util.Set<?>) stats.get("schema_names")).isEmpty());
        
        // Add a schema
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "StatsTestRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        schemaManager.getOrCreateDeltaSchema(avroSchema);
        
        // Should now have 1 cached schema
        stats = schemaManager.getCacheStats();
        assertEquals(1, stats.get("cached_schemas"));
        assertFalse(((java.util.Set<?>) stats.get("schema_names")).isEmpty());
    }

    @Test
    void testSchemaCompatibilityCheck() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "CompatibilityTestRecord",
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
                "name": "CompatibilityTestRecord",
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
        
        boolean compatible = schemaManager.isSchemaCompatible(oldSchema, newSchema);
        assertTrue(compatible); // Adding optional field should be compatible
    }

    @Test
    void testSchemaCompatibilityCheckWithIncompatibleChange() {
        String oldSchemaJson = """
            {
                "type": "record",
                "name": "IncompatibilityTestRecord",
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
                "name": "IncompatibilityTestRecord",
                "namespace": "com.example.test",
                "fields": [
                    {"name": "id", "type": "string"}
                ]
            }
            """;
        
        Schema oldSchema = new Schema.Parser().parse(oldSchemaJson);
        Schema newSchema = new Schema.Parser().parse(newSchemaJson);
        
        boolean compatible = schemaManager.isSchemaCompatible(oldSchema, newSchema);
        assertFalse(compatible); // Removing field should be incompatible
    }
}