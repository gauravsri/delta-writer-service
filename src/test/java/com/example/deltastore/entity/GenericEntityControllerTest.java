package com.example.deltastore.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
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

@ExtendWith(MockitoExtension.class)
class GenericEntityControllerTest {

    @Mock
    private GenericEntityService entityService;

    @Mock
    private GenericRecord mockEntity;

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
    void testCreateEntitySuccess() throws Exception {
        EntityOperationResult<GenericRecord> successResult = EntityOperationResult.<GenericRecord>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(1)
            .message("Entity saved successfully")
            .build();

        when(entityService.save(eq("users"), any(GenericRecord.class)))
            .thenReturn(successResult);

        String entityJson = """
            {
                "user_id": "user123",
                "username": "johndoe",
                "email": "john@example.com"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(entityJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.entityType").value("users"))
            .andExpect(jsonPath("$.operationType").value("WRITE"))
            .andExpect(jsonPath("$.recordCount").value(1));

        verify(entityService).save(eq("users"), any(GenericRecord.class));
    }

    @Test
    void testCreateEntityFailure() throws Exception {
        EntityOperationResult<GenericRecord> failureResult = EntityOperationResult.<GenericRecord>builder()
            .success(false)
            .entityType("users")
            .operationType(OperationType.WRITE)
            .recordCount(0)
            .message("Failed to save entity")
            .error(new RuntimeException("Validation error"))
            .build();

        when(entityService.save(eq("users"), any(GenericRecord.class)))
            .thenReturn(failureResult);

        String entityJson = """
            {
                "user_id": "",
                "username": "johndoe"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(entityJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.message").value("Failed to save entity"));

        verify(entityService).save(eq("users"), any(GenericRecord.class));
    }

    @Test
    void testCreateEntitiesBatchSuccess() throws Exception {
        EntityOperationResult<GenericRecord> batchResult = EntityOperationResult.<GenericRecord>builder()
            .success(true)
            .entityType("users")
            .operationType(OperationType.BATCH_WRITE)
            .recordCount(3)
            .message("Batch operation completed")
            .build();

        when(entityService.saveAll(eq("users"), anyList()))
            .thenReturn(batchResult);

        String batchJson = """
            [
                {
                    "user_id": "user1",
                    "username": "user1"
                },
                {
                    "user_id": "user2", 
                    "username": "user2"
                },
                {
                    "user_id": "user3",
                    "username": "user3"
                }
            ]
            """;

        mockMvc.perform(post("/api/v1/entities/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.success").value(true))
            .andExpect(jsonPath("$.operationType").value("BATCH_WRITE"))
            .andExpect(jsonPath("$.recordCount").value(3));

        verify(entityService).saveAll(eq("users"), anyList());
    }

    @Test
    void testCreateEntitiesBatchFailure() throws Exception {
        EntityOperationResult<GenericRecord> failureResult = EntityOperationResult.<GenericRecord>builder()
            .success(false)
            .entityType("users")
            .operationType(OperationType.BATCH_WRITE)
            .recordCount(0)
            .message("Batch operation failed")
            .build();

        when(entityService.saveAll(eq("users"), anyList()))
            .thenReturn(failureResult);

        String batchJson = """
            [
                {
                    "user_id": "",
                    "username": "invalid"
                }
            ]
            """;

        mockMvc.perform(post("/api/v1/entities/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.success").value(false))
            .andExpect(jsonPath("$.message").value("Batch operation failed"));

        verify(entityService).saveAll(eq("users"), anyList());
    }

    @Test
    void testGetEntityFound() throws Exception {
        Map<String, Object> entityData = Map.of(
            "user_id", "user123",
            "username", "johndoe",
            "email", "john@example.com"
        );

        when(entityService.findById("users", "user123"))
            .thenReturn(Optional.of(entityData));

        mockMvc.perform(get("/api/v1/entities/users/user123"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.user_id").value("user123"))
            .andExpect(jsonPath("$.username").value("johndoe"))
            .andExpect(jsonPath("$.email").value("john@example.com"));

        verify(entityService).findById("users", "user123");
    }

    @Test
    void testGetEntityNotFound() throws Exception {
        when(entityService.findById("users", "nonexistent"))
            .thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/entities/users/nonexistent"))
            .andExpect(status().isNotFound());

        verify(entityService).findById("users", "nonexistent");
    }

    @Test
    void testSearchEntities() throws Exception {
        List<Map<String, Object>> searchResults = List.of(
            Map.of("user_id", "user1", "country", "US", "signup_date", "2024-08-10"),
            Map.of("user_id", "user2", "country", "US", "signup_date", "2024-08-10")
        );

        when(entityService.findByPartition(eq("users"), anyMap()))
            .thenReturn(searchResults);

        String searchJson = """
            {
                "country": "US",
                "signup_date": "2024-08-10"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/users/search")
                .contentType(MediaType.APPLICATION_JSON)
                .content(searchJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$.length()").value(2))
            .andExpect(jsonPath("$[0].user_id").value("user1"))
            .andExpect(jsonPath("$[1].user_id").value("user2"));

        verify(entityService).findByPartition(eq("users"), anyMap());
    }

    @Test
    void testGetEntityTypes() throws Exception {
        List<String> entityTypes = List.of("users", "orders", "products");

        when(entityService.getRegisteredEntityTypes())
            .thenReturn(entityTypes);

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
    void testGetEntityMetadataFound() throws Exception {
        EntityMetadata metadata = EntityMetadata.builder()
            .entityType("users")
            .primaryKeyColumn("user_id")
            .partitionColumns(List.of("country", "signup_date"))
            .active(true)
            .build();

        when(entityService.getEntityMetadata("users"))
            .thenReturn(Optional.of(metadata));

        mockMvc.perform(get("/api/v1/entities/users/metadata"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.entityType").value("users"))
            .andExpect(jsonPath("$.primaryKeyColumn").value("user_id"))
            .andExpect(jsonPath("$.active").value(true));

        verify(entityService).getEntityMetadata("users");
    }

    @Test
    void testGetEntityMetadataNotFound() throws Exception {
        when(entityService.getEntityMetadata("nonexistent"))
            .thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/entities/nonexistent/metadata"))
            .andExpect(status().isNotFound());

        verify(entityService).getEntityMetadata("nonexistent");
    }

    @Test
    void testRegisterEntityTypeSuccess() throws Exception {
        doNothing().when(entityService).registerEntityType(eq("customers"), any(EntityMetadata.class));

        String metadataJson = """
            {
                "entityType": "customers",
                "primaryKeyColumn": "customer_id",
                "partitionColumns": ["region", "signup_date"],
                "active": true
            }
            """;

        mockMvc.perform(post("/api/v1/entities/customers/register")
                .contentType(MediaType.APPLICATION_JSON)
                .content(metadataJson))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("success"))
            .andExpect(jsonPath("$.message").value("Entity type registered successfully"))
            .andExpect(jsonPath("$.entityType").value("customers"));

        verify(entityService).registerEntityType(eq("customers"), any(EntityMetadata.class));
    }

    @Test
    void testRegisterEntityTypeFailure() throws Exception {
        doThrow(new RuntimeException("Invalid schema"))
            .when(entityService).registerEntityType(eq("customers"), any(EntityMetadata.class));

        String metadataJson = """
            {
                "entityType": "customers",
                "primaryKeyColumn": "customer_id"
            }
            """;

        mockMvc.perform(post("/api/v1/entities/customers/register")
                .contentType(MediaType.APPLICATION_JSON)
                .content(metadataJson))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("Failed to register entity type: Invalid schema"))
            .andExpect(jsonPath("$.entityType").value("customers"));

        verify(entityService).registerEntityType(eq("customers"), any(EntityMetadata.class));
    }

    @Test
    void testAllEndpointsWithDifferentEntityTypes() throws Exception {
        String[] entityTypes = {"users", "orders", "products", "events"};

        for (String entityType : entityTypes) {
            when(entityService.getRegisteredEntityTypes())
                .thenReturn(List.of(entityType));

            mockMvc.perform(get("/api/v1/entities/types"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0]").value(entityType));
        }

        verify(entityService, times(4)).getRegisteredEntityTypes();
    }

    @Test
    void testControllerLogging() throws Exception {
        EntityOperationResult<GenericRecord> result = EntityOperationResult.<GenericRecord>builder()
            .success(true)
            .build();

        when(entityService.save(any(), any())).thenReturn(result);
        when(entityService.findById(any(), any())).thenReturn(Optional.empty());

        String entityJson = "{\"id\": \"test\"}";

        mockMvc.perform(post("/api/v1/entities/test")
                .contentType(MediaType.APPLICATION_JSON)
                .content(entityJson));

        mockMvc.perform(get("/api/v1/entities/test/123"));

        verify(entityService).save(eq("test"), any());
        verify(entityService).findById("test", "123");
    }
}