package com.example.deltastore.entity;

import com.example.deltastore.testdata.Order;
import com.example.deltastore.testdata.Product;
import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the Generic Entity Framework using multiple entity types.
 * Tests the framework's ability to handle different entities (users, orders, products)
 * through the same generic service interface.
 */
@ExtendWith(MockitoExtension.class)
class GenericEntityFrameworkIntegrationTest {

    @Mock
    private GenericEntityService entityService;

    private User testUser;
    private Order testOrder;
    private Product testProduct;

    @BeforeEach
    void setUp() {
        // Create test entities
        testUser = new User();
        testUser.setUserId("user-123");
        testUser.setUsername("testuser");
        testUser.setEmail("test@example.com");
        testUser.setCountry("US");
        testUser.setSignupDate("2024-08-10");

        testOrder = new Order(
            "order-456",
            "user-123",
            "product-789",
            2,
            99.99,
            "2024-08-10",
            "US",
            "CONFIRMED"
        );

        testProduct = new Product(
            "product-789",
            "Test Product",
            "Electronics",
            199.99,
            50,
            "2024-08-10"
        );
        testProduct.setBrand("TestBrand");
        testProduct.setDescription("A test product for demonstration");
    }

    @Test
    void testSaveMultipleEntityTypes() {
        // Setup mocks for different entity types
        when(entityService.save(eq("users"), any(User.class)))
            .thenReturn(EntityOperationResult.<User>builder()
                .success(true)
                .entityType("users")
                .operationType(OperationType.WRITE)
                .recordCount(1)
                .build());

        when(entityService.save(eq("orders"), any(Order.class)))
            .thenReturn(EntityOperationResult.<Order>builder()
                .success(true)
                .entityType("orders")
                .operationType(OperationType.WRITE)
                .recordCount(1)
                .build());

        when(entityService.save(eq("products"), any(Product.class)))
            .thenReturn(EntityOperationResult.<Product>builder()
                .success(true)
                .entityType("products")
                .operationType(OperationType.WRITE)
                .recordCount(1)
                .build());

        // Test saving different entity types
        EntityOperationResult<?> userResult = entityService.save("users", testUser);
        EntityOperationResult<?> orderResult = entityService.save("orders", testOrder);
        EntityOperationResult<?> productResult = entityService.save("products", testProduct);

        // Verify all operations succeeded
        assertTrue(userResult.isSuccess());
        assertEquals("users", userResult.getEntityType());
        
        assertTrue(orderResult.isSuccess());
        assertEquals("orders", orderResult.getEntityType());
        
        assertTrue(productResult.isSuccess());
        assertEquals("products", productResult.getEntityType());

        // Verify service calls
        verify(entityService).save("users", testUser);
        verify(entityService).save("orders", testOrder);
        verify(entityService).save("products", testProduct);
    }

    @Test
    void testBatchSaveMultipleEntityTypes() {
        // Create multiple entities for batch operations
        List<User> users = List.of(testUser);
        List<Order> orders = List.of(testOrder);
        List<Product> products = List.of(testProduct);

        // Setup mocks for batch operations
        when(entityService.saveAll(eq("users"), eq(users)))
            .thenReturn(EntityOperationResult.<User>builder()
                .success(true)
                .entityType("users")
                .operationType(OperationType.BATCH_WRITE)
                .recordCount(1)
                .build());

        when(entityService.saveAll(eq("orders"), eq(orders)))
            .thenReturn(EntityOperationResult.<Order>builder()
                .success(true)
                .entityType("orders")
                .operationType(OperationType.BATCH_WRITE)
                .recordCount(1)
                .build());

        when(entityService.saveAll(eq("products"), eq(products)))
            .thenReturn(EntityOperationResult.<Product>builder()
                .success(true)
                .entityType("products")
                .operationType(OperationType.BATCH_WRITE)
                .recordCount(1)
                .build());

        // Execute batch operations
        EntityOperationResult<?> userBatchResult = entityService.saveAll("users", users);
        EntityOperationResult<?> orderBatchResult = entityService.saveAll("orders", orders);
        EntityOperationResult<?> productBatchResult = entityService.saveAll("products", products);

        // Verify all batch operations succeeded
        assertTrue(userBatchResult.isSuccess());
        assertEquals(OperationType.BATCH_WRITE, userBatchResult.getOperationType());
        
        assertTrue(orderBatchResult.isSuccess());
        assertEquals(OperationType.BATCH_WRITE, orderBatchResult.getOperationType());
        
        assertTrue(productBatchResult.isSuccess());
        assertEquals(OperationType.BATCH_WRITE, productBatchResult.getOperationType());
    }

    @Test
    void testFindByIdMultipleEntityTypes() {
        // Setup mock data for different entity types
        Map<String, Object> userData = Map.of(
            "user_id", "user-123",
            "username", "testuser",
            "email", "test@example.com"
        );
        
        Map<String, Object> orderData = Map.of(
            "order_id", "order-456",
            "customer_id", "user-123",
            "total_amount", 199.98
        );
        
        Map<String, Object> productData = Map.of(
            "product_id", "product-789",
            "name", "Test Product",
            "price", 199.99
        );

        when(entityService.findById("users", "user-123"))
            .thenReturn(Optional.of(userData));
        when(entityService.findById("orders", "order-456"))
            .thenReturn(Optional.of(orderData));
        when(entityService.findById("products", "product-789"))
            .thenReturn(Optional.of(productData));

        // Test finding entities of different types
        Optional<Map<String, Object>> userResult = entityService.findById("users", "user-123");
        Optional<Map<String, Object>> orderResult = entityService.findById("orders", "order-456");
        Optional<Map<String, Object>> productResult = entityService.findById("products", "product-789");

        // Verify all entities were found
        assertTrue(userResult.isPresent());
        assertEquals("user-123", userResult.get().get("user_id"));
        
        assertTrue(orderResult.isPresent());
        assertEquals("order-456", orderResult.get().get("order_id"));
        
        assertTrue(productResult.isPresent());
        assertEquals("product-789", productResult.get().get("product_id"));
    }

    @Test
    void testFindByPartitionMultipleEntityTypes() {
        // Setup partition filters for different entity types
        Map<String, String> userFilters = Map.of("country", "US", "signup_date", "2024-08-10");
        Map<String, String> orderFilters = Map.of("order_date", "2024-08-10", "region", "US");
        Map<String, String> productFilters = Map.of("category", "Electronics");

        // Setup mock return data
        List<Map<String, Object>> userResults = List.of(
            Map.of("user_id", "user-123", "country", "US")
        );
        List<Map<String, Object>> orderResults = List.of(
            Map.of("order_id", "order-456", "region", "US")
        );
        List<Map<String, Object>> productResults = List.of(
            Map.of("product_id", "product-789", "category", "Electronics")
        );

        when(entityService.findByPartition("users", userFilters))
            .thenReturn(userResults);
        when(entityService.findByPartition("orders", orderFilters))
            .thenReturn(orderResults);
        when(entityService.findByPartition("products", productFilters))
            .thenReturn(productResults);

        // Test partition searches
        List<Map<String, Object>> foundUsers = entityService.findByPartition("users", userFilters);
        List<Map<String, Object>> foundOrders = entityService.findByPartition("orders", orderFilters);
        List<Map<String, Object>> foundProducts = entityService.findByPartition("products", productFilters);

        // Verify partition searches work for all entity types
        assertEquals(1, foundUsers.size());
        assertEquals("US", foundUsers.get(0).get("country"));
        
        assertEquals(1, foundOrders.size());
        assertEquals("US", foundOrders.get(0).get("region"));
        
        assertEquals(1, foundProducts.size());
        assertEquals("Electronics", foundProducts.get(0).get("category"));
    }

    @Test
    void testEntityMetadataForAllTypes() {
        // Setup metadata for different entity types
        EntityMetadata userMetadata = EntityMetadata.builder()
            .entityType("users")
            .schema(User.getClassSchema())
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country", "signup_date"))
            .active(true)
            .build();

        EntityMetadata orderMetadata = EntityMetadata.builder()
            .entityType("orders")
            .schema(Order.getClassSchema())
            .primaryKeyColumn("order_id")
            .partitionColumns(List.of("order_date", "region"))
            .active(true)
            .build();

        EntityMetadata productMetadata = EntityMetadata.builder()
            .entityType("products")
            .schema(Product.getClassSchema())
            .primaryKeyColumn("product_id")
            .partitionColumns(List.of("category"))
            .active(true)
            .build();

        when(entityService.getEntityMetadata("users")).thenReturn(Optional.of(userMetadata));
        when(entityService.getEntityMetadata("orders")).thenReturn(Optional.of(orderMetadata));
        when(entityService.getEntityMetadata("products")).thenReturn(Optional.of(productMetadata));

        // Test metadata retrieval for all entity types
        Optional<EntityMetadata> userMeta = entityService.getEntityMetadata("users");
        Optional<EntityMetadata> orderMeta = entityService.getEntityMetadata("orders");
        Optional<EntityMetadata> productMeta = entityService.getEntityMetadata("products");

        // Verify metadata for all entity types
        assertTrue(userMeta.isPresent());
        assertEquals("user_id", userMeta.get().getPrimaryKeyColumn());
        assertEquals(2, userMeta.get().getPartitionColumns().size());

        assertTrue(orderMeta.isPresent());
        assertEquals("order_id", orderMeta.get().getPrimaryKeyColumn());
        assertTrue(orderMeta.get().getPartitionColumns().contains("region"));

        assertTrue(productMeta.isPresent());
        assertEquals("product_id", productMeta.get().getPrimaryKeyColumn());
        assertTrue(productMeta.get().getPartitionColumns().contains("category"));
    }

    @Test
    void testGetRegisteredEntityTypes() {
        List<String> entityTypes = List.of("users", "orders", "products");
        
        when(entityService.getRegisteredEntityTypes()).thenReturn(entityTypes);

        List<String> result = entityService.getRegisteredEntityTypes();

        assertEquals(3, result.size());
        assertTrue(result.contains("users"));
        assertTrue(result.contains("orders"));
        assertTrue(result.contains("products"));
    }

    @Test
    void testRegisterNewEntityType() {
        // Test registering a new entity type dynamically
        EntityMetadata newEntityMetadata = EntityMetadata.builder()
            .entityType("customers")
            .schema(User.getClassSchema()) // Reusing schema for test
            .primaryKeyColumn("customer_id")
            .partitionColumns(List.of("region"))
            .active(true)
            .build();

        doNothing().when(entityService).registerEntityType("customers", newEntityMetadata);

        // Register new entity type
        assertDoesNotThrow(() -> {
            entityService.registerEntityType("customers", newEntityMetadata);
        });

        verify(entityService).registerEntityType("customers", newEntityMetadata);
    }

    @Test
    void testEntityTypeValidation() {
        // Test that the framework handles invalid entity types gracefully
        when(entityService.findById("invalid_entity", "test-id"))
            .thenReturn(Optional.empty());

        Optional<Map<String, Object>> result = entityService.findById("invalid_entity", "test-id");
        
        assertTrue(result.isEmpty());
        verify(entityService).findById("invalid_entity", "test-id");
    }

    @Test
    void testSchemaCompatibilityAcrossEntityTypes() {
        // Verify that different entity types have different schemas
        assertNotEquals(User.getClassSchema(), Order.getClassSchema());
        assertNotEquals(User.getClassSchema(), Product.getClassSchema());
        assertNotEquals(Order.getClassSchema(), Product.getClassSchema());

        // Verify schema names are unique
        assertEquals("User", User.getClassSchema().getName());
        assertEquals("Order", Order.getClassSchema().getName());
        assertEquals("Product", Product.getClassSchema().getName());
    }

    @Test
    void testEntityDataIntegrity() {
        // Test that entity data maintains integrity across operations
        assertEquals("user-123", testUser.getUserId());
        assertEquals("order-456", testOrder.getOrderId());
        assertEquals("product-789", testProduct.getProductId());

        // Verify calculated fields in order
        assertEquals(199.98, testOrder.getTotalAmount(), 0.01);

        // Verify default values in product
        assertEquals("USD", testProduct.getCurrency());
        assertTrue(testProduct.isActive());
    }
}