package com.example.deltastore.config;

import com.example.deltastore.api.controller.EntityControllerRegistry;
import com.example.deltastore.api.controller.ReflectionEntityConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AutoEntityDiscoveryTest {

    @Mock
    private EntityControllerRegistry registry;

    @Mock
    private ReflectionEntityConverter reflectionConverter;

    @Mock
    private ApplicationContext applicationContext;

    @Mock
    private Resource mockResource;

    private AutoEntityDiscovery autoEntityDiscovery;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        autoEntityDiscovery = new AutoEntityDiscovery(registry, reflectionConverter, applicationContext);
    }

    @Test
    @DisplayName("Should discover and register entity from valid Avro schema")
    void testDiscoverFromValidAvroSchema() throws Exception {
        // Given
        String userSchema = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example.deltastore.schemas",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "username", "type": "string"},
                {"name": "email", "type": "string"}
              ]
            }
            """;

        InputStream schemaInputStream = new ByteArrayInputStream(userSchema.getBytes());
        Resource[] resources = {mockResource};

        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(resources);
        when(mockResource.getInputStream()).thenReturn(schemaInputStream);
        when(mockResource.getFilename()).thenReturn("user.avsc");
        
        // Mock the application context beans
        when(applicationContext.getBean("optimized", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("deltaStoreMetrics", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("genericEntityService", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("entityValidator")).thenReturn(mock(Object.class));
        
        when(registry.isSupported("users")).thenReturn(false); // Not already registered
        when(registry.getRegisteredCount()).thenReturn(1);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, atLeastOnce()).registerEntityType(any());
        verify(registry).getRegisteredCount();
    }

    @Test
    @DisplayName("Should skip already registered entity types")
    void testSkipAlreadyRegisteredEntity() throws Exception {
        // Given
        String userSchema = """
            {
              "type": "record",
              "name": "User", 
              "namespace": "com.example.deltastore.schemas",
              "fields": [
                {"name": "userId", "type": "string"}
              ]
            }
            """;

        InputStream schemaInputStream = new ByteArrayInputStream(userSchema.getBytes());
        Resource[] resources = {mockResource};

        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(resources);
        when(mockResource.getInputStream()).thenReturn(schemaInputStream);
        when(mockResource.getFilename()).thenReturn("user.avsc");
        
        when(registry.isSupported("users")).thenReturn(true); // Already registered
        when(registry.getRegisteredCount()).thenReturn(1);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, never()).registerEntityType(any()); // Should not register again
        verify(registry).getRegisteredCount();
    }

    @Test
    @DisplayName("Should handle invalid Avro schema gracefully")
    void testHandleInvalidAvroSchema() throws Exception {
        // Given
        String invalidSchema = """
            {
              "type": "invalid",
              "malformed": true
            }
            """;

        InputStream schemaInputStream = new ByteArrayInputStream(invalidSchema.getBytes());
        Resource[] resources = {mockResource};

        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(resources);
        when(mockResource.getInputStream()).thenReturn(schemaInputStream);
        when(mockResource.getFilename()).thenReturn("invalid.avsc");
        when(registry.getRegisteredCount()).thenReturn(0);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, never()).registerEntityType(any()); // Should not register invalid schema
        verify(registry).getRegisteredCount();
    }

    @Test
    @DisplayName("Should handle no schema files gracefully")
    void testHandleNoSchemaFiles() throws Exception {
        // Given
        Resource[] emptyResources = {};
        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(emptyResources);
        when(registry.getRegisteredCount()).thenReturn(0);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, never()).registerEntityType(any());
        verify(registry).getRegisteredCount();
    }

    @Test
    @DisplayName("Should handle resource reading exceptions")
    void testHandleResourceReadingException() throws Exception {
        // Given
        Resource[] resources = {mockResource};
        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(resources);
        when(mockResource.getInputStream()).thenThrow(new RuntimeException("File not found"));
        when(mockResource.getFilename()).thenReturn("error.avsc");
        when(registry.getRegisteredCount()).thenReturn(0);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, never()).registerEntityType(any()); // Should not register on error
        verify(registry).getRegisteredCount();
    }

    @Test
    @DisplayName("Should handle multiple schema files")
    void testHandleMultipleSchemaFiles() throws Exception {
        // Given
        String userSchema = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example.deltastore.schemas",
              "fields": [{"name": "userId", "type": "string"}]
            }
            """;
        
        String productSchema = """
            {
              "type": "record", 
              "name": "Product",
              "namespace": "com.example.deltastore.schemas",
              "fields": [{"name": "productId", "type": "string"}]
            }
            """;

        Resource mockUserResource = mock(Resource.class);
        Resource mockProductResource = mock(Resource.class);
        Resource[] resources = {mockUserResource, mockProductResource};

        when(applicationContext.getResources("classpath:avro/*.avsc")).thenReturn(resources);
        
        when(mockUserResource.getInputStream()).thenReturn(new ByteArrayInputStream(userSchema.getBytes()));
        when(mockUserResource.getFilename()).thenReturn("user.avsc");
        
        when(mockProductResource.getInputStream()).thenReturn(new ByteArrayInputStream(productSchema.getBytes()));
        when(mockProductResource.getFilename()).thenReturn("product.avsc");
        
        // Mock beans for both entities
        when(applicationContext.getBean("optimized", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("deltaStoreMetrics", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("genericEntityService", Object.class)).thenReturn(mock(Object.class));
        when(applicationContext.getBean("entityValidator")).thenReturn(mock(Object.class));

        when(registry.isSupported(anyString())).thenReturn(false); // Neither registered
        when(registry.getRegisteredCount()).thenReturn(2);

        // When
        autoEntityDiscovery.discoverAndRegisterEntities();

        // Then
        verify(registry, times(2)).registerEntityType(any()); // Should register both
        verify(registry).getRegisteredCount();
    }
}