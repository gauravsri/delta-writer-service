package com.example.deltastore.entity;

import com.example.deltastore.testdata.Order;
import com.example.deltastore.testdata.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for GenericEntityController using test entities (orders, products).
 * Validates that the controller can handle multiple entity types through the same endpoints.
 */
@ExtendWith(MockitoExtension.class)
class GenericEntityControllerIntegrationTest {

    @Mock
    private GenericEntityService entityService;

    private GenericEntityController controller;
    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        controller = new GenericEntityController(entityService);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testCreateOrder() throws Exception {
        // Setup successful order creation
        EntityOperationResult<Order> successResult = EntityOperationResult.<Order>builder()
            .success(true)
            .entityType("orders")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Order saved successfully")
            .build();

        when(entityService.save(eq("orders"), any())).thenReturn((EntityOperationResult) successResult);

        String orderJson = """
            {
                "order_id": "order-123",
                "customer_id": "customer-456",
                "product_id": "product-789",
                "quantity": 2,
                "unit_price": 99.99,
                "total_amount": 199.98,
                "order_date": "2024-08-10",
                "region": "US",
                "status": "CONFIRMED"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(orderJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.entityType").value("orders"))
            .andExpect(jsonPath("$.operationType").value("WRITE"))
            .andExpect(jsonPath("$.recordCount").value(1));

        verify(entityService).save(eq("orders"), any());
    }

    @Test
    void testCreateProduct() throws Exception {
        // Setup successful product creation
        EntityOperationResult<Product> successResult = EntityOperationResult.<Product>builder()
            .success(true)
            .entityType("products")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Product saved successfully")
            .build();

        when(entityService.save(eq("products"), any())).thenReturn((EntityOperationResult) successResult);

        String productJson = """
            {
                "product_id": "product-123",
                "name": "Test Product",
                "description": "A test product",
                "category": "Electronics",
                "subcategory": "Smartphones",
                "brand": "TestBrand",
                "price": 299.99,
                "currency": "USD",
                "stock_quantity": 100,
                "weight_kg": 0.2,
                "dimensions": "15x7x0.8cm",
                "is_active": true,
                "created_date": "2024-08-10"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/products")
                .contentType(MediaType.APPLICATION_JSON)
                .content(productJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.entityType").value("products"))
            .andExpect(jsonPath("$.operationType").value("WRITE"));

        verify(entityService).save(eq("products"), any());
    }

    @Test
    void testBatchCreateOrders() throws Exception {
        EntityOperationResult<Order> batchResult = EntityOperationResult.<Order>builder()
            .success(true)
            .entityType("orders")
            .operationType(OperationType.BATCH_WRITE)
            .recordCount(2)
            .message("Batch orders saved successfully")
            .build();

        when(entityService.saveAll(eq("orders"), anyList())).thenReturn((EntityOperationResult) batchResult);

        String batchOrdersJson = """
            [
                {
                    "order_id": "order-001",
                    "customer_id": "customer-123",
                    "product_id": "product-456",
                    "quantity": 1,
                    "unit_price": 49.99,
                    "total_amount": 49.99,
                    "order_date": "2024-08-10",
                    "region": "US",
                    "status": "PENDING"
                },
                {
                    "order_id": "order-002",
                    "customer_id": "customer-124",
                    "product_id": "product-457",
                    "quantity": 3,
                    "unit_price": 29.99,
                    "total_amount": 89.97,
                    "order_date": "2024-08-10",
                    "region": "EU",
                    "status": "CONFIRMED"
                }
            ]
            """;

        mockMvc.perform(post("/api/v1/entities/orders/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchOrdersJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.operationType").value("BATCH_WRITE"))
            .andExpect(jsonPath("$.recordCount").value(2));

        verify(entityService).saveAll(eq("orders"), anyList());
    }

    @Test
    void testBatchCreateProducts() throws Exception {
        EntityOperationResult<Product> batchResult = EntityOperationResult.<Product>builder()
            .success(true)
            .entityType("products")
            .operationType(OperationType.BATCH_WRITE)
            .recordCount(3)
            .message("Batch products saved successfully")
            .build();

        when(entityService.saveAll(eq("products"), anyList())).thenReturn((EntityOperationResult) batchResult);

        String batchProductsJson = """
            [
                {
                    "product_id": "product-001",
                    "name": "Smartphone",
                    "category": "Electronics",
                    "price": 699.99,
                    "stock_quantity": 50,
                    "created_date": "2024-08-10"
                },
                {
                    "product_id": "product-002",
                    "name": "Laptop",
                    "category": "Electronics",
                    "price": 1299.99,
                    "stock_quantity": 25,
                    "created_date": "2024-08-10"
                },
                {
                    "product_id": "product-003",
                    "name": "Headphones",
                    "category": "Electronics",
                    "price": 199.99,
                    "stock_quantity": 100,
                    "created_date": "2024-08-10"
                }
            ]
            """;

        mockMvc.perform(post("/api/v1/entities/products/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchProductsJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.operationType").value("BATCH_WRITE"))
            .andExpect(jsonPath("$.recordCount").value(3));

        verify(entityService).saveAll(eq("products"), anyList());
    }

    @Test
    void testGetOrderById() throws Exception {
        Map<String, Object> orderData = Map.of(
            "order_id", "order-123",
            "customer_id", "customer-456",
            "product_id", "product-789",
            "quantity", 2,
            "total_amount", 199.98,
            "region", "US",
            "status", "CONFIRMED"
        );

        when(entityService.findById("orders", "order-123"))
            .thenReturn(Optional.of(orderData));

        mockMvc.perform(get("/api/v1/entities/orders/order-123"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.order_id").value("order-123"))
            .andExpect(jsonPath("$.customer_id").value("customer-456"))
            .andExpect(jsonPath("$.total_amount").value(199.98))
            .andExpect(jsonPath("$.status").value("CONFIRMED"));

        verify(entityService).findById("orders", "order-123");
    }

    @Test
    void testGetProductById() throws Exception {
        Map<String, Object> productData = Map.of(
            "product_id", "product-789",
            "name", "Test Product",
            "category", "Electronics",
            "price", 299.99,
            "stock_quantity", 50,
            "is_active", true
        );

        when(entityService.findById("products", "product-789"))
            .thenReturn(Optional.of(productData));

        mockMvc.perform(get("/api/v1/entities/products/product-789"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.product_id").value("product-789"))
            .andExpect(jsonPath("$.name").value("Test Product"))
            .andExpect(jsonPath("$.category").value("Electronics"))
            .andExpect(jsonPath("$.price").value(299.99));

        verify(entityService).findById("products", "product-789");
    }

    @Test
    void testSearchOrdersByPartition() throws Exception {
        List<Map<String, Object>> orderResults = List.of(
            Map.of("order_id", "order-001", "region", "US", "order_date", "2024-08-10"),
            Map.of("order_id", "order-002", "region", "US", "order_date", "2024-08-10")
        );

        when(entityService.findByPartition(eq("orders"), anyMap()))
            .thenReturn(orderResults);

        String searchJson = """
            {
                "region": "US",
                "order_date": "2024-08-10"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/orders/search")
                .contentType(MediaType.APPLICATION_JSON)
                .content(searchJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$.length()").value(2))
            .andExpect(jsonPath("$[0].region").value("US"))
            .andExpect(jsonPath("$[1].region").value("US"));

        verify(entityService).findByPartition(eq("orders"), anyMap());
    }

    @Test
    void testSearchProductsByPartition() throws Exception {
        List<Map<String, Object>> productResults = List.of(
            Map.of("product_id", "product-001", "category", "Electronics", "name", "Smartphone"),
            Map.of("product_id", "product-002", "category", "Electronics", "name", "Laptop")
        );

        when(entityService.findByPartition(eq("products"), anyMap()))
            .thenReturn(productResults);

        String searchJson = """
            {
                "category": "Electronics"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/products/search")
                .contentType(MediaType.APPLICATION_JSON)
                .content(searchJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$.length()").value(2))
            .andExpect(jsonPath("$[0].category").value("Electronics"))
            .andExpect(jsonPath("$[1].category").value("Electronics"));

        verify(entityService).findByPartition(eq("products"), anyMap());
    }

    @Test
    void testGetAllEntityTypes() throws Exception {
        List<String> entityTypes = List.of("users", "orders", "products");

        when(entityService.getRegisteredEntityTypes()).thenReturn(entityTypes);

        mockMvc.perform(get("/api/v1/entities/types"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$.length()").value(3))
            .andExpect(jsonPath("$[0]").value("users"))
            .andExpect(jsonPath("$[1]").value("orders"))
            .andExpect(jsonPath("$[2]").value("products"));

        verify(entityService).getRegisteredEntityTypes();
    }

    @Test
    void testGetOrderMetadata() throws Exception {
        EntityMetadata orderMetadata = EntityMetadata.builder()
            .entityType("orders")
            .schema(Order.getClassSchema())
            .primaryKeyColumn("order_id")
            .partitionColumns(List.of("order_date", "region"))
            .active(true)
            .build();

        when(entityService.getEntityMetadata("orders"))
            .thenReturn(Optional.of(orderMetadata));

        mockMvc.perform(get("/api/v1/entities/orders/metadata"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.entityType").value("orders"))
            .andExpect(jsonPath("$.primaryKeyColumn").value("order_id"))
            .andExpect(jsonPath("$.active").value(true));

        verify(entityService).getEntityMetadata("orders");
    }

    @Test
    void testGetProductMetadata() throws Exception {
        EntityMetadata productMetadata = EntityMetadata.builder()
            .entityType("products")
            .schema(Product.getClassSchema())
            .primaryKeyColumn("product_id")
            .partitionColumns(List.of("category"))
            .active(true)
            .build();

        when(entityService.getEntityMetadata("products"))
            .thenReturn(Optional.of(productMetadata));

        mockMvc.perform(get("/api/v1/entities/products/metadata"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.entityType").value("products"))
            .andExpect(jsonPath("$.primaryKeyColumn").value("product_id"))
            .andExpect(jsonPath("$.active").value(true));

        verify(entityService).getEntityMetadata("products");
    }

    @Test
    void testRegisterNewEntityType() throws Exception {
        doNothing().when(entityService).registerEntityType(eq("inventory"), any(EntityMetadata.class));

        String metadataJson = """
            {
                "entityType": "inventory",
                "primaryKeyColumn": "inventory_id",
                "partitionColumns": ["warehouse", "date"],
                "active": true
            }
            """;

        mockMvc.perform(post("/api/v1/entities/inventory/register")
                .contentType(MediaType.APPLICATION_JSON)
                .content(metadataJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("success"))
            .andExpect(jsonPath("$.message").value("Entity type registered successfully"))
            .andExpect(jsonPath("$.entityType").value("inventory"));

        verify(entityService).registerEntityType(eq("inventory"), any(EntityMetadata.class));
    }
}