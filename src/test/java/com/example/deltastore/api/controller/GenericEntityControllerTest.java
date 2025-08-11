package com.example.deltastore.api.controller;

import com.example.deltastore.api.dto.GenericBatchCreateRequest;
import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.service.EntityService;
import com.example.deltastore.validation.EntityValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@ActiveProfiles("local")
class GenericEntityControllerTest {

    @Mock
    private EntityControllerRegistry controllerRegistry;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private EntityControllerConfig entityControllerConfig;

    @Mock
    private EntityService<GenericRecord> entityService;

    @Mock
    private EntityValidator<GenericRecord> entityValidator;

    @Mock
    private EntityConverter entityConverter;

    @Mock
    private GenericRecord genericRecord;

    private GenericEntityController controller;

    @BeforeEach
    void setUp() {
        controller = new GenericEntityController(controllerRegistry, objectMapper);
    }

    @Test
    void testCreateEntity_Success() {
        // Given
        String entityType = "users";
        Map<String, Object> entityData = Map.of("id", "123", "name", "John");
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityConverter()).thenReturn(entityConverter);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityControllerConfig.getEntityService()).thenReturn(entityService);
        when(entityConverter.convertFromMap(entityData)).thenReturn(genericRecord);
        when(entityValidator.validate(entityType, genericRecord)).thenReturn(Collections.emptyList());

        // When
        ResponseEntity<?> response = controller.createEntity(entityType, entityData);

        // Then
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        verify(entityService).save(genericRecord);
    }

    @Test
    void testCreateEntity_UnsupportedEntityType() {
        // Given
        String entityType = "unsupported";
        Map<String, Object> entityData = Map.of("id", "123");
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(null);

        // When
        ResponseEntity<?> response = controller.createEntity(entityType, entityData);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Entity type 'unsupported' is not supported"));
        verify(entityService, never()).save(any());
    }

    @Test
    void testCreateEntity_ValidationFailure() {
        // Given
        String entityType = "users";
        Map<String, Object> entityData = Map.of("id", "123");
        List<String> validationErrors = Arrays.asList("Name is required", "Email is invalid");
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityConverter()).thenReturn(entityConverter);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityConverter.convertFromMap(entityData)).thenReturn(genericRecord);
        when(entityValidator.validate(entityType, genericRecord)).thenReturn(validationErrors);

        // When
        ResponseEntity<?> response = controller.createEntity(entityType, entityData);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Name is required"));
        verify(entityService, never()).save(any());
    }

    @Test
    void testCreateEntity_ServiceException() {
        // Given
        String entityType = "users";
        Map<String, Object> entityData = Map.of("id", "123", "name", "John");
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityConverter()).thenReturn(entityConverter);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityControllerConfig.getEntityService()).thenReturn(entityService);
        when(entityConverter.convertFromMap(entityData)).thenReturn(genericRecord);
        when(entityValidator.validate(entityType, genericRecord)).thenReturn(Collections.emptyList());
        doThrow(new RuntimeException("Database error")).when(entityService).save(genericRecord);

        // When
        ResponseEntity<?> response = controller.createEntity(entityType, entityData);

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Failed to create users entity"));
    }

    @Test
    void testCreateEntitiesBatch_Success() {
        // Given
        String entityType = "users";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        request.setEntities(Arrays.asList(genericRecord, genericRecord));
        
        BatchCreateResponse batchResponse = BatchCreateResponse.builder()
                .successCount(2)
                .failureCount(0)
                .build();
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityControllerConfig.getEntityService()).thenReturn(entityService);
        when(entityValidator.validate(eq(entityType), any(GenericRecord.class))).thenReturn(Collections.emptyList());
        when(entityService.saveBatch(any())).thenReturn(batchResponse);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(batchResponse, response.getBody());
        verify(entityService).saveBatch(any());
    }

    @Test
    void testCreateEntitiesBatch_PartialSuccess() {
        // Given
        String entityType = "users";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        request.setEntities(Arrays.asList(genericRecord, genericRecord));
        
        BatchCreateResponse batchResponse = BatchCreateResponse.builder()
                .successCount(1)
                .failureCount(1)
                .build();
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityControllerConfig.getEntityService()).thenReturn(entityService);
        when(entityValidator.validate(eq(entityType), any(GenericRecord.class))).thenReturn(Collections.emptyList());
        when(entityService.saveBatch(any())).thenReturn(batchResponse);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.PARTIAL_CONTENT, response.getStatusCode());
        assertEquals(batchResponse, response.getBody());
    }

    @Test
    void testCreateEntitiesBatch_EmptyEntities() {
        // Given
        String entityType = "users";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        request.setEntities(Collections.emptyList());
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Entities list cannot be empty"));
        verify(entityService, never()).saveBatch(any());
    }

    @Test
    void testCreateEntitiesBatch_NullRequest() {
        // Given
        String entityType = "users";
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, null);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Entities list cannot be empty"));
        verify(entityService, never()).saveBatch(any());
    }

    @Test
    void testCreateEntitiesBatch_TooManyEntities() {
        // Given
        String entityType = "users";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        List<GenericRecord> largeList = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            largeList.add(genericRecord);
        }
        request.setEntities(largeList);
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Batch size cannot exceed 1000 entities"));
        verify(entityService, never()).saveBatch(any());
    }

    @Test
    void testCreateEntitiesBatch_ValidationErrors() {
        // Given
        String entityType = "users";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        request.setEntities(Arrays.asList(genericRecord, genericRecord));
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(entityControllerConfig);
        when(entityControllerConfig.getEntityValidator()).thenReturn(entityValidator);
        when(entityValidator.validate(eq(entityType), any(GenericRecord.class)))
            .thenReturn(Arrays.asList("Validation error"));

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Validation error"));
        verify(entityService, never()).saveBatch(any());
    }

    @Test
    void testCreateEntitiesBatch_UnsupportedEntityType() {
        // Given
        String entityType = "unsupported";
        GenericBatchCreateRequest<GenericRecord> request = new GenericBatchCreateRequest<>();
        request.setEntities(Arrays.asList(genericRecord));
        
        when(controllerRegistry.getConfig(entityType)).thenReturn(null);

        // When
        ResponseEntity<?> response = controller.createEntitiesBatch(entityType, request);

        // Then
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Entity type 'unsupported' is not supported"));
        verify(entityService, never()).saveBatch(any());
    }

    @Test
    void testGetSupportedEntityTypes_Success() {
        // Given
        List<String> supportedTypes = Arrays.asList("users", "orders", "products");
        when(controllerRegistry.getSupportedEntityTypes()).thenReturn(supportedTypes);

        // When
        ResponseEntity<?> response = controller.getSupportedEntityTypes();

        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("users"));
        assertTrue(response.getBody().toString().contains("orders"));
        assertTrue(response.getBody().toString().contains("products"));
    }

    @Test
    void testGetSupportedEntityTypes_Exception() {
        // Given
        when(controllerRegistry.getSupportedEntityTypes()).thenThrow(new RuntimeException("Registry error"));

        // When
        ResponseEntity<?> response = controller.getSupportedEntityTypes();

        // Then
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertTrue(response.getBody().toString().contains("Failed to retrieve supported entity types"));
    }
}