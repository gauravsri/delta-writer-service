package com.example.deltastore.api.dto;

import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class GenericBatchCreateRequestTest {

    private List<User> testUsers;
    private GenericBatchCreateRequest.BatchCreateOptions testOptions;

    @BeforeEach
    void setUp() {
        User user1 = new User("test1", "user1", "user1@test.com", "US", "2024-01-01");
        User user2 = new User("test2", "user2", "user2@test.com", "CA", "2024-01-02");
        testUsers = List.of(user1, user2);
        
        testOptions = new GenericBatchCreateRequest.BatchCreateOptions();
        testOptions.setFailFast(true);
        testOptions.setBatchSize(50);
    }

    @Test
    void testDefaultConstructor() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        assertNull(request.getEntities());
        assertNull(request.getOptions());
    }

    @Test
    void testSettersAndGetters() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        
        request.setEntities(testUsers);
        request.setOptions(testOptions);
        
        assertEquals(testUsers, request.getEntities());
        assertEquals(testOptions, request.getOptions());
    }

    @Test
    void testToString() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        request.setEntities(testUsers);
        request.setOptions(null);
        
        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("GenericBatchCreateRequest"));
    }

    @Test
    void testEqualsAndHashCode() {
        GenericBatchCreateRequest<User> request1 = new GenericBatchCreateRequest<>();
        request1.setEntities(testUsers);
        request1.setOptions(testOptions);
        
        GenericBatchCreateRequest<User> request2 = new GenericBatchCreateRequest<>();
        request2.setEntities(testUsers);
        request2.setOptions(testOptions);
        
        GenericBatchCreateRequest<User> request3 = new GenericBatchCreateRequest<>();
        request3.setEntities(List.of());
        request3.setOptions(testOptions);
        
        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
        assertNotEquals(request1, request3);
    }

    @Test
    void testCanEqual() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        assertTrue(request.canEqual(new GenericBatchCreateRequest<User>()));
        assertFalse(request.canEqual(new Object()));
        assertFalse(request.canEqual(null));
    }

    @Test
    void testWithEmptyEntities() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        request.setEntities(List.of());
        request.setOptions(null);
        
        assertNotNull(request.getEntities());
        assertTrue(request.getEntities().isEmpty());
    }

    @Test
    void testWithNullValues() {
        GenericBatchCreateRequest<User> request = new GenericBatchCreateRequest<>();
        request.setEntities(null);
        request.setOptions(null);
        
        assertNull(request.getEntities());
        assertNull(request.getOptions());
    }

    @Test
    void testBatchCreateOptions() {
        GenericBatchCreateRequest.BatchCreateOptions options = new GenericBatchCreateRequest.BatchCreateOptions();
        
        // Test defaults
        assertFalse(options.isFailFast());
        assertEquals(100, options.getBatchSize());
        assertTrue(options.isContinueOnFailure());
        assertTrue(options.isValidateDuplicates());
        
        // Test setters
        options.setFailFast(true);
        options.setBatchSize(50);
        options.setContinueOnFailure(false);
        options.setValidateDuplicates(false);
        
        assertTrue(options.isFailFast());
        assertEquals(50, options.getBatchSize());
        assertFalse(options.isContinueOnFailure());
        assertFalse(options.isValidateDuplicates());
    }

    @Test
    void testGenericTypeSupport() {
        // Test that we can create requests for different entity types
        GenericBatchCreateRequest<User> userRequest = new GenericBatchCreateRequest<>();
        userRequest.setEntities(testUsers);
        
        assertNotNull(userRequest.getEntities());
        assertEquals(2, userRequest.getEntities().size());
        assertTrue(userRequest.getEntities().get(0) instanceof User);
    }
}