package com.example.deltastore.entity;

import com.example.deltastore.schemas.User;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EntityMetadataTest {

    @Test
    void testBuilderWithAllFields() {
        Schema schema = User.getClassSchema();
        List<String> partitionColumns = List.of("country", "signup_date");
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .primaryKeyColumn("user_id")
            .partitionColumns(partitionColumns)
            .active(true)
            .build();

        assertEquals("users", metadata.getEntityType());
        assertEquals(schema, metadata.getSchema());
        assertEquals("user_id", metadata.getPrimaryKeyColumn());
        assertEquals(partitionColumns, metadata.getPartitionColumns());
        assertTrue(metadata.isActive());
        // Note: Lombok builder does not auto-populate timestamps or generate schema versions
        // These would be set by the service layer when creating metadata
    }

    @Test
    void testBuilderWithMinimalFields() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .build();

        assertEquals("users", metadata.getEntityType());
        assertEquals(schema, metadata.getSchema());
        assertNull(metadata.getPrimaryKeyColumn());
        assertNull(metadata.getPartitionColumns()); // Lombok doesn't initialize collections
        assertFalse(metadata.isActive()); // Default boolean is false
    }

    @Test
    void testBuilderWithNullEntityType() {
        Schema schema = User.getClassSchema();
        
        // Lombok builder allows null values, so this should not throw
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType(null)
            .schema(schema)
            .build();
            
        assertNull(metadata.getEntityType());
    }

    @Test
    void testBuilderWithEmptyEntityType() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("")
            .schema(schema)
            .build();
            
        assertEquals("", metadata.getEntityType());
    }

    @Test
    void testBuilderWithNullSchema() {
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(null)
            .build();
            
        assertNull(metadata.getSchema());
    }

    @Test
    void testSchemaVersionField() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .schemaVersion("v1.0.0")
            .build();

        assertEquals("v1.0.0", metadata.getSchemaVersion());
    }

    @Test
    void testTimestampFields() {
        Schema schema = User.getClassSchema();
        LocalDateTime registeredAt = LocalDateTime.now();
        LocalDateTime lastUpdated = LocalDateTime.now().plusMinutes(1);
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .registeredAt(registeredAt)
            .lastUpdated(lastUpdated)
            .build();

        assertEquals(registeredAt, metadata.getRegisteredAt());
        assertEquals(lastUpdated, metadata.getLastUpdated());
    }

    @Test
    void testActiveField() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata activeMetadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .active(true)
            .build();
            
        assertTrue(activeMetadata.isActive());
        
        EntityMetadata inactiveMetadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .active(false)
            .build();
            
        assertFalse(inactiveMetadata.isActive());
    }

    @Test
    void testPartitionColumns() {
        Schema schema = User.getClassSchema();
        List<String> partitionColumns = List.of("country", "signup_date");
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .partitionColumns(partitionColumns)
            .build();
            
        List<String> retrievedPartitions = metadata.getPartitionColumns();
        
        assertEquals(partitionColumns, retrievedPartitions);
        assertEquals(2, retrievedPartitions.size());
        assertTrue(retrievedPartitions.contains("country"));
        assertTrue(retrievedPartitions.contains("signup_date"));
    }

    @Test
    void testToString() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country"))
            .build();
            
        String toString = metadata.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("users"));
        assertTrue(toString.contains("user_id"));
        assertTrue(toString.contains("country"));
    }

    @Test
    void testEqualsAndHashCode() {
        Schema schema = User.getClassSchema();
        
        EntityMetadata metadata1 = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .primaryKeyColumn("user_id")
            .build();
            
        EntityMetadata metadata2 = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .primaryKeyColumn("user_id")
            .build();
            
        EntityMetadata metadata3 = EntityMetadata.builder()
            .entityType("orders")
            .schema(schema)
            .primaryKeyColumn("order_id")
            .build();

        // Same entity type and schema should be equal
        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
        
        // Different entity type should not be equal
        assertNotEquals(metadata1, metadata3);
        assertNotEquals(metadata1.hashCode(), metadata3.hashCode());
        
        // Null check
        assertNotEquals(metadata1, null);
        
        // Self check
        assertEquals(metadata1, metadata1);
    }
}