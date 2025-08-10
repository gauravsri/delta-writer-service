package com.example.deltastore.entity;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.schema.DeltaSchemaManager;
import com.example.deltastore.storage.DeltaTableManager;
import com.example.deltastore.schemas.User;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GenericEntityServiceTest {

    @Mock
    private DeltaTableManager tableManager;
    
    @Mock
    private DeltaSchemaManager schemaManager;
    
    @Mock
    private DeltaStoreConfiguration config;
    
    @Mock
    private EntityMetadataRegistry metadataRegistry;

    private GenericEntityService entityService;

    @BeforeEach
    void setUp() {
        entityService = new GenericEntityService(tableManager, schemaManager, config, metadataRegistry);
        
        // Setup default configuration
        DeltaStoreConfiguration.SchemaConfig schemaConfig = new DeltaStoreConfiguration.SchemaConfig();
        schemaConfig.setEnableSchemaValidation(false); // Disable for simpler tests
        when(config.getSchema()).thenReturn(schemaConfig);
        
        DeltaStoreConfiguration.TableConfig defaultTableConfig = new DeltaStoreConfiguration.TableConfig();
        defaultTableConfig.setPrimaryKeyColumn("user_id");
        when(config.getTableConfigOrDefault(anyString())).thenReturn(defaultTableConfig);
        when(config.getTables()).thenReturn(Map.of("users", defaultTableConfig));
    }

    @Test
    void testSaveSingleEntity() {
        // Create test user
        User user = createTestUser("test001", "testuser", "test@example.com", "US", "2024-08-10");
        
        // Mock table manager behavior
        doNothing().when(tableManager).write(anyString(), anyList(), any());
        
        // Execute
        EntityOperationResult<User> result = entityService.save("users", user);
        
        // Verify
        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        assertEquals("Entity saved successfully", result.getMessage());
        
        // Verify table manager was called
        verify(tableManager).write(eq("users"), anyList(), any());
    }

    @Test
    void testSaveMultipleEntities() {
        // Create test users
        List<User> users = List.of(
            createTestUser("test001", "user1", "user1@example.com", "US", "2024-08-10"),
            createTestUser("test002", "user2", "user2@example.com", "CA", "2024-08-10"),
            createTestUser("test003", "user3", "user3@example.com", "UK", "2024-08-10")
        );
        
        // Mock table manager behavior
        doNothing().when(tableManager).write(anyString(), anyList(), any());
        
        // Execute
        EntityOperationResult<User> result = entityService.saveAll("users", users);
        
        // Verify
        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(3, result.getRecordCount());
        assertEquals("Entities saved successfully", result.getMessage());
        
        // Verify table manager was called with correct number of records
        verify(tableManager).write(eq("users"), argThat(list -> list.size() == 3), any());
    }

    @Test
    void testSaveWithEmptyList() {
        List<User> emptyUsers = Collections.emptyList();
        
        EntityOperationResult<User> result = entityService.saveAll("users", emptyUsers);
        
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRecordCount());
        assertEquals("No entities to save", result.getMessage());
        
        // Verify table manager was not called
        verify(tableManager, never()).write(anyString(), anyList(), any());
    }

    @Test
    void testSaveWithTableManagerException() {
        User user = createTestUser("test001", "testuser", "test@example.com", "US", "2024-08-10");
        
        // Mock table manager to throw exception
        doThrow(new RuntimeException("Table write failed")).when(tableManager).write(anyString(), anyList(), any());
        
        EntityOperationResult<User> result = entityService.save("users", user);
        
        assertFalse(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(0, result.getRecordCount());
        assertTrue(result.getMessage().contains("Failed to save entity"));
        assertNotNull(result.getError());
    }

    @Test
    void testFindById() {
        String entityType = "users";
        String userId = "test001";
        
        Map<String, Object> expectedUser = Map.of(
            "user_id", "test001",
            "username", "testuser",
            "email", "test@example.com",
            "country", "US",
            "signup_date", "2024-08-10"
        );
        
        when(tableManager.read(eq(entityType), eq("user_id"), eq(userId)))
            .thenReturn(Optional.of(expectedUser));
        
        Optional<Map<String, Object>> result = entityService.findById(entityType, userId);
        
        assertTrue(result.isPresent());
        assertEquals(expectedUser, result.get());
        
        verify(tableManager).read(eq(entityType), eq("user_id"), eq(userId));
    }

    @Test
    void testFindByIdNotFound() {
        String entityType = "users";
        String userId = "nonexistent";
        
        when(tableManager.read(eq(entityType), eq("user_id"), eq(userId)))
            .thenReturn(Optional.empty());
        
        Optional<Map<String, Object>> result = entityService.findById(entityType, userId);
        
        assertTrue(result.isEmpty());
        verify(tableManager).read(eq(entityType), eq("user_id"), eq(userId));
    }

    @Test
    void testFindByPartition() {
        String entityType = "users";
        Map<String, String> partitionFilters = Map.of("country", "US", "signup_date", "2024-08-10");
        
        List<Map<String, Object>> expectedUsers = List.of(
            Map.of("user_id", "test001", "username", "user1", "country", "US"),
            Map.of("user_id", "test002", "username", "user2", "country", "US")
        );
        
        when(tableManager.readByPartitions(eq(entityType), eq(partitionFilters)))
            .thenReturn(expectedUsers);
        
        List<Map<String, Object>> result = entityService.findByPartition(entityType, partitionFilters);
        
        assertEquals(2, result.size());
        assertEquals(expectedUsers, result);
        
        verify(tableManager).readByPartitions(eq(entityType), eq(partitionFilters));
    }

    @Test
    void testRegisterEntityType() {
        String entityType = "products";
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType(entityType)
            .primaryKeyColumn("product_id")
            .partitionColumns(List.of("category"))
            .build();
        
        doNothing().when(metadataRegistry).registerEntity(eq(entityType), eq(metadata));
        
        entityService.registerEntityType(entityType, metadata);
        
        verify(metadataRegistry).registerEntity(eq(entityType), eq(metadata));
    }

    @Test
    void testGetEntityMetadata() {
        String entityType = "users";
        EntityMetadata expectedMetadata = EntityMetadata.builder()
            .entityType(entityType)
            .primaryKeyColumn("user_id")
            .build();
        
        when(metadataRegistry.getEntityMetadata(eq(entityType)))
            .thenReturn(Optional.of(expectedMetadata));
        
        Optional<EntityMetadata> result = entityService.getEntityMetadata(entityType);
        
        assertTrue(result.isPresent());
        assertEquals(expectedMetadata, result.get());
        verify(metadataRegistry).getEntityMetadata(eq(entityType));
    }

    @Test
    void testGetRegisteredEntityTypes() {
        List<String> expectedTypes = List.of("users", "products", "orders");
        
        when(metadataRegistry.getRegisteredEntityTypes()).thenReturn(expectedTypes);
        
        List<String> result = entityService.getRegisteredEntityTypes();
        
        assertEquals(expectedTypes, result);
        verify(metadataRegistry).getRegisteredEntityTypes();
    }

    private User createTestUser(String userId, String username, String email, String country, String signupDate) {
        User user = new User();
        user.setUserId(userId);
        user.setUsername(username);
        user.setEmail(email);
        user.setCountry(country);
        user.setSignupDate(signupDate);
        return user;
    }
}