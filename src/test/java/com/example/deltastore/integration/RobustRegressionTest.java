package com.example.deltastore.integration;

import com.example.deltastore.api.dto.BatchCreateRequest;
import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.schemas.User;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ActiveProfiles;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
@Slf4j
public class RobustRegressionTest {

    @Autowired
    private TestRestTemplate restTemplate;

    private final String testSessionId = "robust-test-" + System.currentTimeMillis();

    @BeforeEach
    public void setup() {
        log.info("=== ROBUST REGRESSION TEST SESSION: {} ===", testSessionId);
    }

    @Test
    public void testRobustDataScenarios() {
        log.info("=== TESTING ROBUST DATA SCENARIOS ===");
        
        // Scenario 1: Small batch (5 users)
        testSmallBatchScenario();
        
        // Scenario 2: Medium batch (20 users)
        testMediumBatchScenario();
        
        // Scenario 3: Large batch (50 users)
        testLargeBatchScenario();
        
        // Scenario 4: Data integrity validation
        testDataIntegrityScenario();
        
        log.info("=== ALL ROBUST SCENARIOS COMPLETED SUCCESSFULLY ===");
    }

    private void testSmallBatchScenario() {
        log.info("--- Testing Small Batch Scenario (5 users) ---");
        
        List<User> users = createSimpleTestUsers(5, "small");
        BatchCreateResponse response = executeBatchWrite(users, "small batch");
        
        assertEquals(5, response.getTotalRequested());
        assertEquals(5, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        
        // Validate all users can be read back
        for (User user : users) {
            validateUserReadback(user);
        }
        
        log.info("✅ Small batch scenario completed successfully");
    }

    private void testMediumBatchScenario() {
        log.info("--- Testing Medium Batch Scenario (20 users) ---");
        
        List<User> users = createSimpleTestUsers(20, "medium");
        BatchCreateResponse response = executeBatchWrite(users, "medium batch");
        
        assertEquals(20, response.getTotalRequested());
        assertEquals(20, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        
        // Sample validation - check first, middle, and last user
        validateUserReadback(users.get(0));
        validateUserReadback(users.get(10));
        validateUserReadback(users.get(19));
        
        log.info("✅ Medium batch scenario completed successfully");
    }

    private void testLargeBatchScenario() {
        log.info("--- Testing Large Batch Scenario (50 users) ---");
        
        List<User> users = createSimpleTestUsers(50, "large");
        BatchCreateResponse response = executeBatchWrite(users, "large batch");
        
        assertEquals(50, response.getTotalRequested());
        assertEquals(50, response.getSuccessCount());
        assertEquals(0, response.getFailureCount());
        
        // Sample validation - check multiple users
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            int randomIndex = random.nextInt(50);
            validateUserReadback(users.get(randomIndex));
        }
        
        log.info("✅ Large batch scenario completed successfully");
    }

    private void testDataIntegrityScenario() {
        log.info("--- Testing Data Integrity Scenario ---");
        
        // Create users with varied but safe data
        List<User> integrityUsers = new ArrayList<>();
        String[] countries = {"US", "UK", "CA", "AU", "DE"};
        String[] domains = {"gmail.com", "yahoo.com", "test.com"};
        
        for (int i = 1; i <= 15; i++) {
            String userId = testSessionId + "-integrity-" + String.format("%03d", i);
            String country = countries[(i - 1) % countries.length];
            String domain = domains[(i - 1) % domains.length];
            
            User user = new User();
            user.setUserId(userId);
            user.setUsername("IntegrityUser" + i);
            user.setEmail("integrity" + i + "@" + domain);
            user.setCountry(country);
            user.setSignupDate("2024-08-09");
            integrityUsers.add(user);
        }
        
        BatchCreateResponse response = executeBatchWrite(integrityUsers, "data integrity");
        assertEquals(15, response.getSuccessCount());
        
        // Validate data integrity by checking every user
        for (User originalUser : integrityUsers) {
            Map<String, Object> retrievedUser = readUser(originalUser.getUserId());
            
            // Verify all fields match exactly
            assertEquals(originalUser.getUserId(), retrievedUser.get("user_id"));
            assertEquals(originalUser.getUsername(), retrievedUser.get("username"));
            assertEquals(originalUser.getEmail(), retrievedUser.get("email"));
            assertEquals(originalUser.getCountry(), retrievedUser.get("country"));
            assertEquals(originalUser.getSignupDate(), retrievedUser.get("signup_date"));
        }
        
        log.info("✅ Data integrity scenario completed successfully");
    }

    private List<User> createSimpleTestUsers(int count, String prefix) {
        List<User> users = new ArrayList<>();
        String[] countries = {"US", "UK", "CA", "AU", "DE"};
        String[] domains = {"gmail.com", "yahoo.com", "test.com"};
        
        for (int i = 1; i <= count; i++) {
            String userId = testSessionId + "-" + prefix + "-" + String.format("%03d", i);
            String country = countries[(i - 1) % countries.length];
            String domain = domains[(i - 1) % domains.length];
            
            User user = new User();
            user.setUserId(userId);
            user.setUsername("User" + i);
            user.setEmail("user" + i + "@" + domain);
            user.setCountry(country);
            user.setSignupDate("2024-08-09");
            users.add(user);
        }
        
        return users;
    }

    private BatchCreateResponse executeBatchWrite(List<User> users, String batchDescription) {
        log.info("Writing {} users for {}", users.size(), batchDescription);
        
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<BatchCreateRequest> entity = new HttpEntity<>(request, headers);
        
        ResponseEntity<BatchCreateResponse> response = restTemplate.postForEntity(
            "/api/v1/users/batch", entity, BatchCreateResponse.class);
        
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        
        BatchCreateResponse result = response.getBody();
        log.info("✅ {} write successful: {}/{} successful, {} failed", 
            batchDescription, result.getSuccessCount(), result.getTotalRequested(), result.getFailureCount());
        
        return result;
    }

    private void validateUserReadback(User originalUser) {
        String userId = originalUser.getUserId();
        Map<String, Object> retrievedUser = readUser(userId);
        
        assertNotNull(retrievedUser, "User " + userId + " should be retrievable");
        assertEquals(originalUser.getUserId(), retrievedUser.get("user_id"));
        assertEquals(originalUser.getUsername(), retrievedUser.get("username"));
        assertEquals(originalUser.getEmail(), retrievedUser.get("email"));
        assertEquals(originalUser.getCountry(), retrievedUser.get("country"));
        assertEquals(originalUser.getSignupDate(), retrievedUser.get("signup_date"));
    }

    private Map<String, Object> readUser(String userId) {
        ResponseEntity<Map> response = restTemplate.getForEntity(
            "/api/v1/users/{userId}", Map.class, userId);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        return response.getBody();
    }
}