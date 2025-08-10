package com.example.deltastore.entity;

import com.example.deltastore.schemas.User;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EntityMetadataRegistryTest {

    private EntityMetadataRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new EntityMetadataRegistry();
    }

    @Test
    void testRegisterEntity() {
        Schema schema = User.getClassSchema();
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country", "signup_date"))
            .build();

        registry.registerEntity("users", metadata);

        assertTrue(registry.isEntityRegistered("users"));
        Optional<EntityMetadata> retrieved = registry.getEntityMetadata("users");
        assertTrue(retrieved.isPresent());
        assertEquals("users", retrieved.get().getEntityType());
        assertEquals("user_id", retrieved.get().getPrimaryKeyColumn());
        assertEquals(schema, retrieved.get().getSchema());
        assertTrue(retrieved.get().isActive());
        assertNotNull(retrieved.get().getRegisteredAt());
        assertNotNull(retrieved.get().getLastUpdated());
        assertNotNull(retrieved.get().getSchemaVersion());
    }

    @Test
    void testRegisterEntityWithNullEntityType() {
        EntityMetadata metadata = EntityMetadata.builder()
            .schema(User.getClassSchema())
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            registry.registerEntity(null, metadata);
        });
    }

    @Test
    void testRegisterEntityWithEmptyEntityType() {
        EntityMetadata metadata = EntityMetadata.builder()
            .schema(User.getClassSchema())
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            registry.registerEntity("", metadata);
        });
    }

    @Test
    void testRegisterEntityWithInvalidEntityTypeName() {
        EntityMetadata metadata = EntityMetadata.builder()
            .schema(User.getClassSchema())
            .build();

        // Entity type starting with number
        assertThrows(IllegalArgumentException.class, () -> {
            registry.registerEntity("123users", metadata);
        });

        // Entity type with special characters
        assertThrows(IllegalArgumentException.class, () -> {
            registry.registerEntity("user-type", metadata);
        });

        // Valid entity type with underscore should work
        assertDoesNotThrow(() -> {
            registry.registerEntity("user_type", metadata);
        });
    }

    @Test
    void testGetNonExistentEntity() {
        Optional<EntityMetadata> result = registry.getEntityMetadata("nonexistent");
        assertTrue(result.isEmpty());

        assertFalse(registry.isEntityRegistered("nonexistent"));

        Optional<Schema> schema = registry.getEntitySchema("nonexistent");
        assertTrue(schema.isEmpty());
    }

    @Test
    void testUpdateEntityMetadata() {
        // Register initial entity
        Schema originalSchema = User.getClassSchema();
        EntityMetadata originalMetadata = EntityMetadata.builder()
            .entityType("users")
            .schema(originalSchema)
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country"))
            .active(true)
            .build();

        registry.registerEntity("users", originalMetadata);
        
        Optional<EntityMetadata> initial = registry.getEntityMetadata("users");
        assertTrue(initial.isPresent());
        LocalDateTime originalRegisteredAt = initial.get().getRegisteredAt();

        // Update metadata
        EntityMetadata updatedMetadata = EntityMetadata.builder()
            .entityType("users")
            .schema(originalSchema)
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country", "signup_date"))
            .active(true)
            .build();

        registry.updateEntityMetadata("users", updatedMetadata);

        Optional<EntityMetadata> updated = registry.getEntityMetadata("users");
        assertTrue(updated.isPresent());
        
        // Should have same registered time but updated last updated time
        assertEquals(originalRegisteredAt, updated.get().getRegisteredAt());
        assertTrue(updated.get().getLastUpdated().isAfter(originalRegisteredAt));
        
        // Should have updated partition columns
        assertEquals(2, updated.get().getPartitionColumns().size());
        assertTrue(updated.get().getPartitionColumns().contains("signup_date"));
    }

    @Test
    void testUpdateNonExistentEntity() {
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("nonexistent")
            .schema(User.getClassSchema())
            .build();

        assertThrows(IllegalArgumentException.class, () -> {
            registry.updateEntityMetadata("nonexistent", metadata);
        });
    }

    @Test
    void testDeactivateEntity() {
        // Register entity
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .primaryKeyColumn("user_id")
            .active(true)
            .build();

        registry.registerEntity("users", metadata);
        
        Optional<EntityMetadata> active = registry.getEntityMetadata("users");
        assertTrue(active.isPresent());
        assertTrue(active.get().isActive());

        // Deactivate entity
        registry.deactivateEntity("users");

        Optional<EntityMetadata> deactivated = registry.getEntityMetadata("users");
        assertTrue(deactivated.isPresent());
        assertFalse(deactivated.get().isActive());
        
        // Should still be registered but not active
        assertTrue(registry.isEntityRegistered("users"));
    }

    @Test
    void testDeactivateNonExistentEntity() {
        // Should not throw exception
        assertDoesNotThrow(() -> {
            registry.deactivateEntity("nonexistent");
        });
    }

    @Test
    void testGetRegisteredEntityTypes() {
        assertTrue(registry.getRegisteredEntityTypes().isEmpty());

        // Register multiple entities
        EntityMetadata users = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .build();
        
        EntityMetadata orders = EntityMetadata.builder()
            .entityType("orders")
            .schema(User.getClassSchema()) // Using same schema for simplicity
            .build();

        registry.registerEntity("users", users);
        registry.registerEntity("orders", orders);

        List<String> registeredTypes = registry.getRegisteredEntityTypes();
        assertEquals(2, registeredTypes.size());
        assertTrue(registeredTypes.contains("users"));
        assertTrue(registeredTypes.contains("orders"));
    }

    @Test
    void testGetActiveEntityTypes() {
        // Register entities
        EntityMetadata users = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .build();
        
        EntityMetadata orders = EntityMetadata.builder()
            .entityType("orders")
            .schema(User.getClassSchema())
            .build();

        registry.registerEntity("users", users);
        registry.registerEntity("orders", orders);

        // Initially both should be active
        List<String> activeTypes = registry.getActiveEntityTypes();
        assertEquals(2, activeTypes.size());
        assertTrue(activeTypes.contains("users"));
        assertTrue(activeTypes.contains("orders"));

        // Deactivate one
        registry.deactivateEntity("orders");

        activeTypes = registry.getActiveEntityTypes();
        assertEquals(1, activeTypes.size());
        assertTrue(activeTypes.contains("users"));
        assertFalse(activeTypes.contains("orders"));
    }

    @Test
    void testGetRegistryStats() {
        Map<String, Object> initialStats = registry.getRegistryStats();
        assertEquals(0, initialStats.get("total_registered"));
        assertEquals(0L, initialStats.get("active_entities"));
        assertEquals(0, initialStats.get("inactive_entities"));

        // Register entities
        EntityMetadata users = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .build();
        
        EntityMetadata orders = EntityMetadata.builder()
            .entityType("orders")
            .schema(User.getClassSchema())
            .build();

        registry.registerEntity("users", users);
        registry.registerEntity("orders", orders);

        Map<String, Object> stats = registry.getRegistryStats();
        assertEquals(2, stats.get("total_registered"));
        assertEquals(2L, stats.get("active_entities"));
        assertEquals(0, stats.get("inactive_entities"));

        // Deactivate one
        registry.deactivateEntity("orders");

        stats = registry.getRegistryStats();
        assertEquals(2, stats.get("total_registered"));
        assertEquals(1L, stats.get("active_entities"));
        assertEquals(1, stats.get("inactive_entities"));

        @SuppressWarnings("unchecked")
        List<String> entityTypes = (List<String>) stats.get("entity_types");
        assertEquals(2, entityTypes.size());
    }

    @Test
    void testClearAll() {
        // Register entities
        EntityMetadata users = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .build();

        registry.registerEntity("users", users);
        
        assertTrue(registry.isEntityRegistered("users"));

        // Clear all
        registry.clearAll();

        assertFalse(registry.isEntityRegistered("users"));
        assertTrue(registry.getRegisteredEntityTypes().isEmpty());
        
        Map<String, Object> stats = registry.getRegistryStats();
        assertEquals(0, stats.get("total_registered"));
    }

    @Test
    void testGetEntitySchema() {
        Schema schema = User.getClassSchema();
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .build();

        registry.registerEntity("users", metadata);

        Optional<Schema> retrieved = registry.getEntitySchema("users");
        assertTrue(retrieved.isPresent());
        assertEquals(schema, retrieved.get());
    }

    @Test
    void testSchemaVersionGeneration() {
        Schema schema = User.getClassSchema();
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .schema(schema)
            .build();

        registry.registerEntity("users", metadata);

        Optional<EntityMetadata> retrieved = registry.getEntityMetadata("users");
        assertTrue(retrieved.isPresent());
        
        String schemaVersion = retrieved.get().getSchemaVersion();
        assertNotNull(schemaVersion);
        assertTrue(schemaVersion.startsWith("v"));
    }
}