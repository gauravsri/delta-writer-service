package com.example.deltastore.api.dto;

import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BatchCreateRequestTest {

    private List<User> testUsers;
    private BatchCreateRequest.BatchCreateOptions testOptions;

    @BeforeEach
    void setUp() {
        User user1 = new User("test1", "user1", "user1@test.com", "US", "2024-01-01");
        User user2 = new User("test2", "user2", "user2@test.com", "CA", "2024-01-02");
        testUsers = List.of(user1, user2);
        
        testOptions = new BatchCreateRequest.BatchCreateOptions();
        testOptions.setFailFast(true);
        testOptions.setBatchSize(50);
    }

    @Test
    void testDefaultConstructor() {
        BatchCreateRequest request = new BatchCreateRequest();
        assertNull(request.getUsers());
        assertNull(request.getOptions());
    }

    @Test
    void testSettersAndGetters() {
        BatchCreateRequest request = new BatchCreateRequest();
        
        request.setUsers(testUsers);
        request.setOptions(testOptions);
        
        assertEquals(testUsers, request.getUsers());
        assertEquals(testOptions, request.getOptions());
    }

    @Test
    void testToString() {
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(testUsers);
        request.setOptions(null);
        
        String toString = request.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("BatchCreateRequest"));
    }

    @Test
    void testEqualsAndHashCode() {
        BatchCreateRequest request1 = new BatchCreateRequest();
        request1.setUsers(testUsers);
        request1.setOptions(testOptions);
        
        BatchCreateRequest request2 = new BatchCreateRequest();
        request2.setUsers(testUsers);
        request2.setOptions(testOptions);
        
        BatchCreateRequest request3 = new BatchCreateRequest();
        request3.setUsers(List.of());
        request3.setOptions(testOptions);
        
        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
        assertNotEquals(request1, request3);
    }

    @Test
    void testCanEqual() {
        BatchCreateRequest request = new BatchCreateRequest();
        assertTrue(request.canEqual(new BatchCreateRequest()));
        assertFalse(request.canEqual(new Object()));
        assertFalse(request.canEqual(null));
    }

    @Test
    void testWithEmptyUsers() {
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(List.of());
        request.setOptions(null);
        
        assertNotNull(request.getUsers());
        assertTrue(request.getUsers().isEmpty());
    }

    @Test
    void testWithNullValues() {
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(null);
        request.setOptions(null);
        
        assertNull(request.getUsers());
        assertNull(request.getOptions());
    }

    @Test
    void testBatchCreateOptions() {
        BatchCreateRequest.BatchCreateOptions options = new BatchCreateRequest.BatchCreateOptions();
        
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
}