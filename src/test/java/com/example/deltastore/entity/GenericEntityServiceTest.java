package com.example.deltastore.entity;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.storage.DeltaTableManager;
import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class GenericEntityServiceTest {

    @Mock
    private DeltaTableManager tableManager;
    
    @Mock
    private DeltaStoreConfiguration config;
    
    @Mock
    private EntityMetadataRegistry metadataRegistry;

    private GenericEntityService entityService;

    @BeforeEach
    void setUp() {
        entityService = new GenericEntityService(tableManager, config, metadataRegistry);
        
        // Setup default configuration with lenient stubbing to avoid unnecessary stubbing errors
        DeltaStoreConfiguration.SchemaConfig schemaConfig = new DeltaStoreConfiguration.SchemaConfig();
        schemaConfig.setEnableSchemaValidation(false); // Disable for simpler tests
        schemaConfig.setAutoRegisterSchemas(true);
        lenient().when(config.getSchema()).thenReturn(schemaConfig);
        
        DeltaStoreConfiguration.TableConfig defaultTableConfig = new DeltaStoreConfiguration.TableConfig();
        defaultTableConfig.setPrimaryKeyColumn("user_id");
        lenient().when(config.getTableConfigOrDefault(anyString())).thenReturn(defaultTableConfig);
        lenient().when(config.getTables()).thenReturn(Map.of("users", defaultTableConfig));
    }

    @Test
    void testSaveSingleEntity() {
        // Create test user
        User user = createTestUser("test001", "testuser", "test@example.com", "US", "2024-08-10");
        
        // Mock table manager behavior
        doNothing().when(tableManager).write(anyString(), anyList(), any());
        
        EntityOperationResult<User> result = entityService.save("users", user);
        
        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        
        verify(tableManager).write(eq("users"), anyList(), eq(user.getSchema()));
    }

    @Test
    void testSaveMultipleEntities() {
        // Create test users
        List<User> users = List.of(
            createTestUser("test001", "user1", "user1@example.com", "US", "2024-08-10"),
            createTestUser("test002", "user2", "user2@example.com", "CA", "2024-08-10"),
            createTestUser("test003", "user3", "user3@example.com", "UK", "2024-08-10")
        );
        
        doNothing().when(tableManager).write(anyString(), anyList(), any());
        
        EntityOperationResult<User> result = entityService.saveAll("users", users);
        
        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(3, result.getRecordCount());
        
        verify(tableManager).write(eq("users"), anyList(), eq(users.get(0).getSchema()));
    }

    @Test
    void testSaveWithEmptyList() {
        List<User> emptyUsers = Collections.emptyList();
        
        EntityOperationResult<User> result = entityService.saveAll("users", emptyUsers);
        
        assertTrue(result.isSuccess());
        assertEquals(0, result.getRecordCount());
        assertEquals("No entities to save", result.getMessage());
        
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
        
        verify(tableManager).write(anyString(), anyList(), any());
    }

    @Test
    void testSaveFromMap() {
        Map<String, Object> userData = Map.of(
            "user_id", "test001",
            "username", "testuser",
            "email", "test@example.com",
            "country", "US",
            "signup_date", "2024-08-10"
        );
        
        doNothing().when(tableManager).write(anyString(), anyList(), any());
        
        EntityOperationResult<?> result = entityService.saveFromMap("users", userData);
        
        assertTrue(result.isSuccess());
        assertEquals("users", result.getEntityType());
        assertEquals(OperationType.WRITE, result.getOperationType());
        assertEquals(1, result.getRecordCount());
        
        verify(tableManager).write(eq("users"), anyList(), any());
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
    void testGetRegisteredEntityTypes() {
        List<String> expectedTypes = List.of("users", "products", "orders");
        
        when(metadataRegistry.getRegisteredEntityTypes()).thenReturn(expectedTypes);
        
        List<String> result = entityService.getRegisteredEntityTypes();
        
        assertEquals(3, result.size());
        assertEquals(expectedTypes, result);
        verify(metadataRegistry).getRegisteredEntityTypes();
    }

    @Test
    void testValidateEntityTypeWithNullName() {
        assertThrows(IllegalArgumentException.class, () -> {
            entityService.saveFromMap(null, Map.of("key", "value"));
        });
    }

    @Test
    void testValidateEntityTypeWithEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> {
            entityService.saveFromMap("", Map.of("key", "value"));
        });
    }

    // Helper method to create test users
    private User createTestUser(String userId, String username, String email, String country, String signupDate) {
        return User.newBuilder()
            .setUserId(userId)
            .setUsername(username)
            .setEmail(email)
            .setCountry(country)
            .setSignupDate(signupDate)
            .build();
    }
}