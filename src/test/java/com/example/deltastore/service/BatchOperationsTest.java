package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.schemas.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.http.*;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("local")
public class BatchOperationsTest {

    private final TestRestTemplate restTemplate;
    private final String BATCH_URL = "http://localhost:8080/api/v1/users/batch";
    
    public BatchOperationsTest() {
        // Configure TestRestTemplate with proper Jackson settings
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(objectMapper);
        
        this.restTemplate = new TestRestTemplate();
        this.restTemplate.getRestTemplate().getMessageConverters().add(0, converter);
    }

    @Test
    public void testSmallBatchInsert() {
        // Create a small batch of 5 users as plain maps to avoid Avro serialization issues
        List<Map<String, Object>> users = createTestUsersAsMaps(5, "small-batch");
        
        Map<String, Object> request = Map.of("users", users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<BatchCreateResponse> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, BatchCreateResponse.class);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        
        BatchCreateResponse batchResponse = response.getBody();
        assertEquals(5, batchResponse.getTotalRequested());
        assertEquals(5, batchResponse.getSuccessCount());
        assertEquals(0, batchResponse.getFailureCount());
        
        System.out.println("Small batch test completed: " + 
            batchResponse.getProcessingTimeMs() + "ms for 5 users");
    }

    @Test
    public void testMediumBatchInsert() {
        // Create a medium batch of 50 users as plain maps
        List<Map<String, Object>> users = createTestUsersAsMaps(50, "medium-batch");
        
        Map<String, Object> request = Map.of("users", users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<BatchCreateResponse> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, BatchCreateResponse.class);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        
        BatchCreateResponse batchResponse = response.getBody();
        assertEquals(50, batchResponse.getTotalRequested());
        assertEquals(50, batchResponse.getSuccessCount());
        assertEquals(0, batchResponse.getFailureCount());
        
        System.out.println("Medium batch test completed: " + 
            batchResponse.getProcessingTimeMs() + "ms for 50 users");
    }

    @Test
    public void testLargeBatchInsert() {
        // Create a large batch of 200 users (will be processed in 2 chunks of 100 each)
        List<Map<String, Object>> users = createTestUsersAsMaps(200, "large-batch");
        
        Map<String, Object> request = Map.of("users", users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<BatchCreateResponse> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, BatchCreateResponse.class);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        
        BatchCreateResponse batchResponse = response.getBody();
        assertEquals(200, batchResponse.getTotalRequested());
        assertEquals(200, batchResponse.getSuccessCount());
        assertEquals(0, batchResponse.getFailureCount());
        
        // Verify statistics
        assertNotNull(batchResponse.getStatistics());
        assertEquals(2, batchResponse.getStatistics().getTotalBatches());
        
        System.out.println("Large batch test completed: " + 
            batchResponse.getProcessingTimeMs() + "ms for 200 users in " +
            batchResponse.getStatistics().getTotalBatches() + " batches");
    }

    @Test
    public void testBatchValidationErrors() {
        List<Map<String, Object>> users = new ArrayList<>();
        
        // Add valid user
        users.add(Map.of(
            "user_id", "valid-user-001",
            "username", "validuser",
            "email", "valid@test.com",
            "country", "US",
            "signup_date", "2024-08-09"
        ));
        
        // Add invalid user (missing user_id)
        users.add(Map.of(
            "username", "invaliduser",
            "email", "invalid@test.com",
            "country", "US",
            "signup_date", "2024-08-09"
        ));
        
        Map<String, Object> request = Map.of("users", users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<Map> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, Map.class);
        
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        // The error happens at JSON parsing level for required Avro fields
        assertTrue(response.getBody().containsKey("error") || response.getBody().containsKey("errors"));
        
        System.out.println("Batch validation test completed with proper error handling");
    }

    @Test
    public void testEmptyBatch() {
        Map<String, Object> request = Map.of("users", Collections.emptyList());
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<Map> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, Map.class);
        
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("error"));
        
        System.out.println("Empty batch test completed with proper error handling");
    }

    @Test
    public void testOversizedBatch() {
        // Create a batch larger than the allowed limit (1001 users)
        List<Map<String, Object>> users = createTestUsersAsMaps(1001, "oversized-batch");
        
        Map<String, Object> request = Map.of("users", users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        ResponseEntity<Map> response = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, Map.class);
        
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("error"));
        
        System.out.println("Oversized batch test completed with proper error handling");
    }

    private List<User> createTestUsers(int count, String prefix) {
        List<User> users = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            User user = User.newBuilder()
                .setUserId(String.format("%s-%03d", prefix, i))
                .setUsername(String.format("user%d", i))
                .setEmail(String.format("user%d@test.com", i))
                .setCountry("US")
                .setSignupDate("2024-08-09")
                .build();
            users.add(user);
        }
        return users;
    }
    
    private List<Map<String, Object>> createTestUsersAsMaps(int count, String prefix) {
        List<Map<String, Object>> users = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            Map<String, Object> user = Map.of(
                "user_id", String.format("%s-%03d", prefix, i),
                "username", String.format("user%d", i),
                "email", String.format("user%d@test.com", i),
                "country", "US",
                "signup_date", "2024-08-09"
            );
            users.add(user);
        }
        return users;
    }
}