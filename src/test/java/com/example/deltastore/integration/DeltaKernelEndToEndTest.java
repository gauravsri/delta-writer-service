package com.example.deltastore.integration;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end test for complete Delta Kernel implementation
 * Tests both write and read operations to ensure data integrity
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("local")
public class DeltaKernelEndToEndTest {

    private final TestRestTemplate restTemplate;
    private final String BATCH_URL = "http://localhost:8080/api/v1/users/batch";
    private final String READ_URL = "http://localhost:8080/api/v1/users/";
    
    public DeltaKernelEndToEndTest() {
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
    public void testCompleteWriteAndReadCycle() {
        String testPrefix = "e2e-test-" + System.currentTimeMillis();
        
        // Step 1: Write data using batch insert
        List<Map<String, Object>> users = createTestUsers(3, testPrefix);
        
        Map<String, Object> request = Map.of("users", users);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(request, headers);
        
        System.out.println("=== TESTING WRITE OPERATION ===");
        ResponseEntity<BatchCreateResponse> writeResponse = restTemplate.exchange(
            BATCH_URL, HttpMethod.POST, httpEntity, BatchCreateResponse.class);
        
        // Verify write was successful
        assertEquals(HttpStatus.CREATED, writeResponse.getStatusCode());
        assertNotNull(writeResponse.getBody());
        
        BatchCreateResponse batchResponse = writeResponse.getBody();
        assertEquals(3, batchResponse.getTotalRequested());
        assertEquals(3, batchResponse.getSuccessCount());
        assertEquals(0, batchResponse.getFailureCount());
        
        System.out.println("✅ Write operation successful: " + batchResponse.getSuccessCount() + " users written");
        
        // Step 2: Read back the data to verify it's accessible
        System.out.println("=== TESTING READ OPERATIONS ===");
        
        for (int i = 1; i <= 3; i++) {
            String userId = testPrefix + "-" + String.format("%03d", i);
            
            try {
                ResponseEntity<Map<String, Object>> readResponse = restTemplate.exchange(
                    READ_URL + userId, HttpMethod.GET, null, 
                    new ParameterizedTypeReference<Map<String, Object>>() {});
                
                if (readResponse.getStatusCode() == HttpStatus.OK) {
                    Map<String, Object> userData = readResponse.getBody();
                    assertNotNull(userData);
                    
                    // Verify the data integrity
                    assertEquals(userId, userData.get("user_id"));
                    assertEquals("user" + i, userData.get("username"));
                    assertEquals("user" + i + "@test.com", userData.get("email"));
                    assertEquals("US", userData.get("country"));
                    assertEquals("2024-08-09", userData.get("signup_date"));
                    
                    System.out.println("✅ Read successful for user: " + userId);
                    System.out.println("   Data: " + userData);
                } else {
                    System.out.println("❌ Read failed for user: " + userId + " (Status: " + readResponse.getStatusCode() + ")");
                    fail("Failed to read user: " + userId);
                }
                
            } catch (Exception e) {
                System.out.println("❌ Read exception for user: " + userId + " - " + e.getMessage());
                fail("Exception while reading user: " + userId + " - " + e.getMessage());
            }
        }
        
        System.out.println("=== END-TO-END TEST COMPLETED SUCCESSFULLY ===");
    }

    private List<Map<String, Object>> createTestUsers(int count, String prefix) {
        List<Map<String, Object>> users = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            Map<String, Object> user = Map.of(
                "user_id", prefix + "-" + String.format("%03d", i),
                "username", "user" + i,
                "email", "user" + i + "@test.com",
                "country", "US",
                "signup_date", "2024-08-09"
            );
            users.add(user);
        }
        return users;
    }
}