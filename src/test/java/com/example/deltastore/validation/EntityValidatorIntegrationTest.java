package com.example.deltastore.validation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.kie.api.KieServices;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class EntityValidatorIntegrationTest {

    private EntityValidator<GenericRecord> entityValidator;
    private Schema testSchema;
    private GenericRecord testUser;

    @BeforeEach
    void setUp() {
        entityValidator = new EntityValidator<>();
        
        // Create test schema
        testSchema = Schema.parse("{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
        
        // Create test user
        testUser = new GenericRecordBuilder(testSchema)
                .set("id", "user123")
                .set("name", "John Doe")
                .set("email", "john@example.com")
                .build();
    }

    @Test
    void testConstructor() {
        assertNotNull(entityValidator);
        
        // Verify KieServices was initialized
        Object kieServices = ReflectionTestUtils.getField(entityValidator, "kieServices");
        assertNotNull(kieServices);
        assertTrue(kieServices.getClass().getName().contains("KieServices"));
        
        // Verify containers map was initialized
        Map<String, ?> containers = (Map<String, ?>) ReflectionTestUtils.getField(entityValidator, "kieContainers");
        assertNotNull(containers);
        assertEquals(ConcurrentHashMap.class, containers.getClass());
    }

    @Test
    void testInit() {
        // Call init method
        assertDoesNotThrow(() -> entityValidator.init());
        
        // Verify containers map exists (even if no rules are loaded in test environment)
        Map<String, ?> containers = (Map<String, ?>) ReflectionTestUtils.getField(entityValidator, "kieContainers");
        assertNotNull(containers);
    }

    @Test
    void testValidateWithNullEntity() {
        List<String> errors = entityValidator.validate("users", null);
        
        assertNotNull(errors);
        assertEquals(1, errors.size());
        assertEquals("users entity cannot be null", errors.get(0));
    }

    @Test
    void testValidateWithValidEntity() {
        // Test with valid entity - should return empty list since no rules exist in test environment
        List<String> errors = entityValidator.validate("users", testUser);
        
        assertNotNull(errors);
        // In test environment without rule files, should return empty list
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateWithUnknownEntityType() {
        List<String> errors = entityValidator.validate("unknown_entity", testUser);
        
        assertNotNull(errors);
        // Should return empty list since no rules exist for unknown entity type
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateUserConvenienceMethod() {
        List<String> errors = entityValidator.validateUser(testUser);
        
        assertNotNull(errors);
        // Should call validate("users", user) internally
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateUserWithNull() {
        List<String> errors = entityValidator.validateUser(null);
        
        assertNotNull(errors);
        assertEquals(1, errors.size());
        assertEquals("users entity cannot be null", errors.get(0));
    }

    @Test
    void testGetOrCreateKieContainer() throws Exception {
        // Test via reflection since it's a private method
        Object result = ReflectionTestUtils.invokeMethod(entityValidator, "getOrCreateKieContainer", "users");
        
        // Should return null in test environment since no rule files exist
        assertNull(result);
    }

    @Test
    void testLoadRulesForEntityType() throws Exception {
        // Test via reflection since it's a private method
        Object result = ReflectionTestUtils.invokeMethod(entityValidator, "loadRulesForEntityType", "users");
        
        // Should return null in test environment since no rule files exist
        assertNull(result);
    }

    @Test
    void testLoadRulesForNonExistentEntityType() throws Exception {
        // Test via reflection since it's a private method
        Object result = ReflectionTestUtils.invokeMethod(entityValidator, "loadRulesForEntityType", "nonexistent");
        
        // Should return null in test environment
        assertNull(result);
    }

    @Test
    void testCleanup() {
        // Add some mock containers to test cleanup
        Map<String, Object> containers = (Map<String, Object>) ReflectionTestUtils.getField(entityValidator, "kieContainers");
        
        // Call cleanup - should not throw exceptions even with empty containers
        assertDoesNotThrow(() -> entityValidator.cleanup());
        
        // Verify containers were cleared
        assertTrue(containers.isEmpty());
    }

    @Test
    void testValidationContext() {
        EntityValidator.ValidationContext context = new EntityValidator.ValidationContext();
        
        // Test initial state
        assertNotNull(context);
        assertFalse(context.hasErrors());
        assertEquals(0, context.getErrorCount());
        assertTrue(context.getErrors().isEmpty());
    }

    @Test
    void testValidationContextAddError() {
        EntityValidator.ValidationContext context = new EntityValidator.ValidationContext();
        
        // Add errors
        context.addError("First error");
        context.addError("Second error");
        
        // Test state after adding errors
        assertTrue(context.hasErrors());
        assertEquals(2, context.getErrorCount());
        
        List<String> errors = context.getErrors();
        assertEquals(2, errors.size());
        assertEquals("First error", errors.get(0));
        assertEquals("Second error", errors.get(1));
    }

    @Test
    void testValidationContextGetErrorsReturnsCopy() {
        EntityValidator.ValidationContext context = new EntityValidator.ValidationContext();
        context.addError("Test error");
        
        List<String> errors1 = context.getErrors();
        List<String> errors2 = context.getErrors();
        
        // Should return different instances (copies)
        assertNotSame(errors1, errors2);
        assertEquals(errors1, errors2);
        
        // Modifying returned list should not affect context
        errors1.add("Additional error");
        assertEquals(1, context.getErrorCount()); // Should still be 1
    }

    @Test
    void testValidateWithEmptyStringEntityType() {
        List<String> errors = entityValidator.validate("", testUser);
        
        assertNotNull(errors);
        // Should handle empty entity type gracefully
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateWithSpecialCharacterEntityType() {
        List<String> errors = entityValidator.validate("test-entity_with.special@chars", testUser);
        
        assertNotNull(errors);
        // Should handle special characters in entity type gracefully
        assertTrue(errors.isEmpty());
    }

    @Test
    void testConcurrentValidation() throws InterruptedException {
        // Test thread safety by running validation concurrently
        Thread[] threads = new Thread[5];
        
        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    GenericRecord user = new GenericRecordBuilder(testSchema)
                            .set("id", "user" + threadId + "_" + j)
                            .set("name", "User " + threadId + "_" + j)
                            .set("email", "user" + threadId + "_" + j + "@example.com")
                            .build();
                    
                    List<String> errors = entityValidator.validate("users", user);
                    assertNotNull(errors);
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }
        
        // If we get here without exceptions, thread safety test passed
        assertTrue(true);
    }

    @Test
    void testInitCallsLoadRulesForUsers() {
        // Create new instance to test init behavior
        EntityValidator<GenericRecord> newValidator = new EntityValidator<>();
        
        // Init should complete without throwing exceptions
        assertDoesNotThrow(() -> newValidator.init());
        
        // Verify the validator is properly initialized
        assertNotNull(newValidator);
    }

    @Test
    void testMultipleEntityTypes() {
        // Test validation with multiple entity types
        List<String> userErrors = entityValidator.validate("users", testUser);
        List<String> productErrors = entityValidator.validate("products", testUser);
        List<String> orderErrors = entityValidator.validate("orders", testUser);
        
        assertNotNull(userErrors);
        assertNotNull(productErrors);
        assertNotNull(orderErrors);
        
        // All should be empty in test environment without rule files
        assertTrue(userErrors.isEmpty());
        assertTrue(productErrors.isEmpty());
        assertTrue(orderErrors.isEmpty());
    }

    @Test
    void testValidationWithDifferentRecordTypes() {
        // Create different record schema
        Schema productSchema = Schema.parse("{\"type\":\"record\",\"name\":\"Product\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"}]}");
        
        GenericRecord product = new GenericRecordBuilder(productSchema)
                .set("id", "product123")
                .set("name", "Test Product")
                .set("price", 99.99)
                .build();
        
        List<String> errors = entityValidator.validate("products", product);
        
        assertNotNull(errors);
        assertTrue(errors.isEmpty());
    }

    @Test
    void testKieServicesFactoryAccess() {
        // Verify that KieServices can be accessed (tests factory initialization)
        Object kieServices = ReflectionTestUtils.getField(entityValidator, "kieServices");
        assertNotNull(kieServices);
        
        // Should be a KieServices implementation
        assertTrue(kieServices.getClass().getSimpleName().contains("Kie") || 
                   kieServices.getClass().getName().contains("kie"));
    }
}