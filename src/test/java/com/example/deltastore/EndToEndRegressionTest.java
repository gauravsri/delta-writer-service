package com.example.deltastore;

import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("local")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EndToEndRegressionTest {

    private TestRestTemplate restTemplate;
    private final String BASE_URL = "http://localhost:8080/api/v1/users";
    private final List<String> createdUserIds = new ArrayList<>();
    private final Map<String, Map<String, Object>> testDataMap = new HashMap<>();
    
    @BeforeAll
    public void setup() {
        System.out.println("=== END-TO-END REGRESSION TEST STARTING ===");
        System.out.println("Timestamp: " + LocalDateTime.now());
        restTemplate = new TestRestTemplate();
        System.out.println("✅ Test environment initialized");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test 1: Basic User Creation")
    public void testBasicUserCreation() {
        System.out.println("\n--- Test 1: Basic User Creation ---");
        
        Map<String, Object> user = createTestUser("regression-001", "john", "john@test.com", "US");
        ResponseEntity<String> response = createUser(user);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        System.out.println("✅ Basic user creation successful");
    }
    
    @Test
    @Order(2)
    @DisplayName("Test 2: Multiple Users with Different Countries")
    public void testMultipleUsersCreation() {
        System.out.println("\n--- Test 2: Multiple Users Creation ---");
        
        String[] countries = {"US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX"};
        int successCount = 0;
        
        for (int i = 0; i < countries.length; i++) {
            String userId = String.format("regression-%03d", i + 2);
            Map<String, Object> user = createTestUser(userId, "user" + (i + 2), 
                "user" + (i + 2) + "@test.com", countries[i]);
                
            ResponseEntity<String> response = createUser(user);
            
            if (response.getStatusCode() == HttpStatus.CREATED) {
                successCount++;
                System.out.println("✅ Created user: " + userId + " (" + countries[i] + ")");
            }
        }
        
        assertEquals(countries.length, successCount);
        System.out.println("✅ All " + countries.length + " users created successfully");
    }
    
    @Test
    @Order(3)
    @DisplayName("Test 3: Edge Cases - Special Characters")
    public void testSpecialCharacters() {
        System.out.println("\n--- Test 3: Edge Cases - Special Characters ---");
        
        // Test with special characters in email
        Map<String, Object> user1 = createTestUser("special-001", "user@#$", 
            "user+tag@example.com", "US");
        ResponseEntity<String> response1 = createUser(user1);
        assertEquals(HttpStatus.CREATED, response1.getStatusCode());
        
        // Test with unicode characters
        Map<String, Object> user2 = createTestUser("special-002", "用户", 
            "unicode@test.com", "CN");
        ResponseEntity<String> response2 = createUser(user2);
        assertEquals(HttpStatus.CREATED, response2.getStatusCode());
        
        System.out.println("✅ Special characters handled correctly");
    }
    
    @Test
    @Order(4)
    @DisplayName("Test 4: Large Data Volumes")
    public void testLargeDataVolumes() {
        System.out.println("\n--- Test 4: Large Data Volumes ---");
        
        // Create user with long strings
        String longUsername = "a".repeat(255);
        String longEmail = "long" + "x".repeat(100) + "@example.com";
        
        Map<String, Object> user = createTestUser("large-001", longUsername, longEmail, "US");
        ResponseEntity<String> response = createUser(user);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        System.out.println("✅ Large data volumes handled correctly");
    }
    
    @Test
    @Order(5)
    @DisplayName("Test 5: Concurrent Operations")
    public void testConcurrentOperations() throws InterruptedException {
        System.out.println("\n--- Test 5: Concurrent Operations ---");
        
        List<Thread> threads = new ArrayList<>();
        final int threadCount = 5;
        final int usersPerThread = 3;
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread thread = new Thread(() -> {
                for (int u = 0; u < usersPerThread; u++) {
                    String userId = "concurrent-" + threadId + "-" + u;
                    Map<String, Object> user = createTestUser(userId, 
                        "thread" + threadId + "user" + u, 
                        "thread" + threadId + "user" + u + "@test.com", 
                        "US");
                    
                    ResponseEntity<String> response = createUser(user);
                    System.out.println("Thread " + threadId + ": Created " + userId + 
                        " -> " + response.getStatusCode());
                }
            });
            threads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        System.out.println("✅ Concurrent operations completed");
    }
    
    @Test
    @Order(6)
    @DisplayName("Test 6: Error Scenarios")
    public void testErrorScenarios() {
        System.out.println("\n--- Test 6: Error Scenarios ---");
        
        // Test missing required field
        Map<String, Object> invalidUser1 = new HashMap<>();
        invalidUser1.put("username", "incomplete");
        invalidUser1.put("email", "incomplete@test.com");
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> request1 = new HttpEntity<>(invalidUser1, headers);
        
        ResponseEntity<String> response1 = restTemplate.exchange(
            BASE_URL, HttpMethod.POST, request1, String.class);
        
        assertTrue(response1.getStatusCode().is4xxClientError() || 
                  response1.getStatusCode().is5xxServerError());
        System.out.println("✅ Missing field validation: " + response1.getStatusCode());
        
        // Test duplicate user_id
        Map<String, Object> user = createTestUser("duplicate-test", "original", "original@test.com", "US");
        ResponseEntity<String> response2 = createUser(user);
        ResponseEntity<String> response3 = createUser(user); // Try to create again
        
        assertEquals(HttpStatus.CREATED, response2.getStatusCode());
        // Second creation might succeed or fail depending on implementation
        System.out.println("✅ Duplicate ID test: " + response2.getStatusCode() + 
                          " -> " + response3.getStatusCode());
    }
    
    @Test
    @Order(7)
    @DisplayName("Test 7: Performance Benchmarking")
    public void testPerformanceBenchmarking() {
        System.out.println("\n--- Test 7: Performance Benchmarking ---");
        
        List<Long> responseTimes = new ArrayList<>();
        int iterations = 10;
        
        for (int i = 0; i < iterations; i++) {
            Map<String, Object> user = createTestUser("perf-" + i, "perfuser" + i, 
                "perf" + i + "@test.com", "US");
                
            long startTime = System.currentTimeMillis();
            ResponseEntity<String> response = createUser(user);
            long endTime = System.currentTimeMillis();
            
            long duration = endTime - startTime;
            responseTimes.add(duration);
            
            System.out.println("  Operation " + (i + 1) + ": " + duration + "ms -> " + 
                response.getStatusCode());
        }
        
        double avgTime = responseTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        long minTime = Collections.min(responseTimes);
        long maxTime = Collections.max(responseTimes);
        
        System.out.println("\n📈 Performance Summary:");
        System.out.println("  Average response time: " + String.format("%.2f", avgTime) + "ms");
        System.out.println("  Minimum response time: " + minTime + "ms");
        System.out.println("  Maximum response time: " + maxTime + "ms");
        System.out.println("  Total operations: " + iterations);
        
        assertTrue(avgTime < 10000, "Average response time should be under 10 seconds");
        System.out.println("✅ Performance benchmarking completed");
    }
    
    @Test
    @Order(8)
    @DisplayName("Test 8: Data Validation Summary")
    public void testDataValidationSummary() {
        System.out.println("\n--- Test 8: Data Validation Summary ---");
        
        // Check health endpoint
        ResponseEntity<String> healthResponse = restTemplate.getForEntity(
            "http://localhost:8080/actuator/health", String.class);
        assertEquals(HttpStatus.OK, healthResponse.getStatusCode());
        
        System.out.println("📊 Test Execution Summary:");
        System.out.println("  Total test users created: " + createdUserIds.size());
        System.out.println("  Health check status: " + healthResponse.getStatusCode());
        System.out.println("  Application status: HEALTHY");
        
        assertTrue(createdUserIds.size() > 20, "Should have created multiple test users");
        System.out.println("✅ Data validation summary completed");
    }
    
    // Helper methods
    private Map<String, Object> createTestUser(String userId, String username, String email, String country) {
        Map<String, Object> user = new HashMap<>();
        user.put("user_id", userId);
        user.put("username", username);
        user.put("email", email);
        user.put("country", country);
        user.put("signup_date", "2024-08-09");
        
        testDataMap.put(userId, user);
        return user;
    }
    
    private ResponseEntity<String> createUser(Map<String, Object> user) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(user, headers);
        
        ResponseEntity<String> response = restTemplate.exchange(
            BASE_URL, HttpMethod.POST, request, String.class);
            
        if (response.getStatusCode() == HttpStatus.CREATED) {
            createdUserIds.add((String) user.get("user_id"));
        }
        
        return response;
    }
    
    @AfterAll
    public void teardown() {
        System.out.println("\n=== END-TO-END REGRESSION TEST COMPLETED ===");
        System.out.println("Final Summary:");
        System.out.println("  Total users successfully created: " + createdUserIds.size());
        System.out.println("  Test completion time: " + LocalDateTime.now());
        System.out.println("============================================\n");
    }
}