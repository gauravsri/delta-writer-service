package com.example.deltastore.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class TableSchemaTest {

    private TableSchema schema;

    @BeforeEach 
    void setUp() {
        schema = new TableSchema();
        schema.setName("test_table");
        schema.setSchemaPath("/path/to/schema.avsc");
        schema.setPrimaryKey("user_id");
        schema.setPartitionBy(List.of("country", "signup_date"));
    }

    @Test
    void testDefaultConstructor() {
        TableSchema newSchema = new TableSchema();
        assertNotNull(newSchema);
        assertNull(newSchema.getName());
        assertNull(newSchema.getSchemaPath());
        assertNull(newSchema.getPrimaryKey());
        assertNull(newSchema.getPartitionBy());
    }

    @Test
    void testConstructorWithName() {
        TableSchema namedSchema = new TableSchema();
        namedSchema.setName("users");
        assertEquals("users", namedSchema.getName());
        assertNull(namedSchema.getSchemaPath());
        assertNull(namedSchema.getPrimaryKey());
        assertNull(namedSchema.getPartitionBy());
    }

    @Test
    void testGettersAndSetters() {
        assertEquals("test_table", schema.getName());
        assertEquals("/path/to/schema.avsc", schema.getSchemaPath());
        assertEquals("user_id", schema.getPrimaryKey());
        assertEquals(List.of("country", "signup_date"), schema.getPartitionBy());
    }

    @Test
    void testSetName() {
        schema.setName("new_table");
        assertEquals("new_table", schema.getName());
    }

    @Test
    void testSetSchemaPath() {
        schema.setSchemaPath("/new/path/schema.avsc");
        assertEquals("/new/path/schema.avsc", schema.getSchemaPath());
    }

    @Test
    void testSetPrimaryKey() {
        schema.setPrimaryKey("id");
        assertEquals("id", schema.getPrimaryKey());
    }

    @Test
    void testSetPartitionBy() {
        List<String> newPartitions = List.of("region", "year");
        schema.setPartitionBy(newPartitions);
        assertEquals(newPartitions, schema.getPartitionBy());
    }

    @Test
    void testSetPartitionByEmpty() {
        schema.setPartitionBy(List.of());
        assertNotNull(schema.getPartitionBy());
        assertTrue(schema.getPartitionBy().isEmpty());
    }

    @Test
    void testSetPartitionByNull() {
        schema.setPartitionBy(null);
        assertNull(schema.getPartitionBy());
    }

    @Test
    void testEqualsAndHashCode() {
        TableSchema schema1 = new TableSchema();
        schema1.setName("test");
        schema1.setSchemaPath("/path");
        schema1.setPrimaryKey("id");
        schema1.setPartitionBy(List.of("col1"));
        
        TableSchema schema2 = new TableSchema();
        schema2.setName("test");
        schema2.setSchemaPath("/path");
        schema2.setPrimaryKey("id");
        schema2.setPartitionBy(List.of("col1"));
        
        TableSchema schema3 = new TableSchema();
        schema3.setName("different");
        
        assertEquals(schema1, schema2);
        assertEquals(schema1.hashCode(), schema2.hashCode());
        assertNotEquals(schema1, schema3);
    }

    @Test
    void testToString() {
        String toString = schema.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("TableSchema"));
        assertTrue(toString.contains("test_table"));
    }

    @Test
    void testCanEqual() {
        assertTrue(schema.canEqual(new TableSchema()));
        assertFalse(schema.canEqual(new Object()));
        assertFalse(schema.canEqual(null));
    }

    @Test
    void testIsPartitioned() {
        // Has partition columns
        assertTrue(schema.getPartitionBy() != null && !schema.getPartitionBy().isEmpty());
        
        // No partition columns
        schema.setPartitionBy(null);
        assertFalse(schema.getPartitionBy() != null && !schema.getPartitionBy().isEmpty());
        
        schema.setPartitionBy(List.of());
        assertFalse(schema.getPartitionBy() != null && !schema.getPartitionBy().isEmpty());
    }

    @Test
    void testHasPrimaryKey() {
        assertTrue(schema.getPrimaryKey() != null && !schema.getPrimaryKey().trim().isEmpty());
        
        schema.setPrimaryKey(null);
        assertFalse(schema.getPrimaryKey() != null && !schema.getPrimaryKey().trim().isEmpty());
        
        schema.setPrimaryKey("");
        assertFalse(schema.getPrimaryKey() != null && !schema.getPrimaryKey().trim().isEmpty());
        
        schema.setPrimaryKey("   ");
        assertFalse(schema.getPrimaryKey() != null && !schema.getPrimaryKey().trim().isEmpty());
    }

    @Test
    void testSinglePartitionColumn() {
        schema.setPartitionBy(List.of("country"));
        assertEquals(1, schema.getPartitionBy().size());
        assertEquals("country", schema.getPartitionBy().get(0));
    }

    @Test
    void testMultiplePartitionColumns() {
        List<String> partitions = List.of("country", "year", "month");
        schema.setPartitionBy(partitions);
        assertEquals(3, schema.getPartitionBy().size());
        assertEquals("country", schema.getPartitionBy().get(0));
        assertEquals("year", schema.getPartitionBy().get(1));
        assertEquals("month", schema.getPartitionBy().get(2));
    }

    @Test
    void testCompleteTableSchema() {
        TableSchema completeSchema = new TableSchema();
        completeSchema.setName("user_events");
        completeSchema.setSchemaPath("/schemas/user_events.avsc");
        completeSchema.setPrimaryKey("event_id");
        completeSchema.setPartitionBy(List.of("event_date", "user_country"));

        assertEquals("user_events", completeSchema.getName());
        assertEquals("/schemas/user_events.avsc", completeSchema.getSchemaPath());
        assertEquals("event_id", completeSchema.getPrimaryKey());
        assertEquals(2, completeSchema.getPartitionBy().size());
        assertTrue(completeSchema.getPartitionBy().contains("event_date"));
        assertTrue(completeSchema.getPartitionBy().contains("user_country"));
    }

    @Test
    void testTableSchemaWithNullValues() {
        TableSchema nullSchema = new TableSchema();
        nullSchema.setName(null);
        nullSchema.setSchemaPath(null);
        nullSchema.setPrimaryKey(null);
        nullSchema.setPartitionBy(null);

        assertNull(nullSchema.getName());
        assertNull(nullSchema.getSchemaPath());
        assertNull(nullSchema.getPrimaryKey());
        assertNull(nullSchema.getPartitionBy());
    }

    @Test
    void testSchemaPathValidation() {
        schema.setSchemaPath("/valid/path/schema.avsc");
        assertEquals("/valid/path/schema.avsc", schema.getSchemaPath());
        
        schema.setSchemaPath("");
        assertEquals("", schema.getSchemaPath());
        
        schema.setSchemaPath(null);
        assertNull(schema.getSchemaPath());
    }
}