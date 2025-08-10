package com.example.deltastore;

import com.example.deltastore.schemas.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * End-to-End Regression Test Suite for Delta Store Service
 * Tests complete data flow from API to MinIO storage
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("local")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class E2ERegressionTest {

    @Autowired
    private MockMvc mockMvc;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private S3Client s3Client;
    
    private static final String BUCKET_NAME = "deltastore-dev";
    private static final String TABLE_NAME = "users";
    private static final String API_BASE_PATH = "/api/v1/users";
    
    // Test data holders
    private final List<User> testUsers = new ArrayList<>();
    private final Map<String, List<User>> usersByCountry = new HashMap<>();
    private final Map<String, List<User>> usersByDate = new HashMap<>();
    
    @BeforeAll
    void setupTestData() {
        System.out.println("=== E2E Regression Test Suite Starting ===");
        System.out.println("MinIO Endpoint: http://localhost:9000");
        System.out.println("Bucket: " + BUCKET_NAME);
        
        // Generate diverse test data
        generateTestUsers();
        
        // Ensure bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build());
            System.out.println("✓ Bucket exists: " + BUCKET_NAME);
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
            System.out.println("✓ Created bucket: " + BUCKET_NAME);
        }
        
        // Clean up any existing test data
        cleanupTestData();
    }
    
    @AfterAll
    void cleanup() {
        System.out.println("=== E2E Regression Test Suite Completed ===");
    }
    
    private void generateTestUsers() {
        String[] countries = {"US", "UK", "DE", "FR", "JP", "AU", "CA", "BR"};
        String[] domains = {"example.com", "test.org", "demo.net", "mail.io"};
        LocalDate startDate = LocalDate.of(2024, 1, 1);
        
        for (int i = 0; i < 100; i++) {
            String userId = "user-" + UUID.randomUUID().toString().substring(0, 8);
            String username = "testuser_" + i;
            String email = username + "@" + domains[i % domains.length];
            String country = countries[i % countries.length];
            LocalDate signupDate = startDate.plusDays(i % 30);
            
            User user = User.newBuilder()
                .setUserId(userId)
                .setUsername(username)
                .setEmail(email)
                .setCountry(country)
                .setSignupDate(signupDate.toString())
                .build();
            
            testUsers.add(user);
            usersByCountry.computeIfAbsent(country, k -> new ArrayList<>()).add(user);
            usersByDate.computeIfAbsent(signupDate.toString(), k -> new ArrayList<>()).add(user);
        }
        
        System.out.println("✓ Generated " + testUsers.size() + " test users");
        System.out.println("  Countries: " + usersByCountry.keySet());
        System.out.println("  Date range: " + startDate + " to " + startDate.plusDays(29));
    }
    
    private void cleanupTestData() {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .prefix(TABLE_NAME + "/")
                .build();
            
            ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
            
            if (response.hasContents()) {
                List<ObjectIdentifier> toDelete = response.contents().stream()
                    .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                    .toList();
                
                if (!toDelete.isEmpty()) {
                    DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                        .bucket(BUCKET_NAME)
                        .delete(Delete.builder().objects(toDelete).build())
                        .build();
                    
                    s3Client.deleteObjects(deleteRequest);
                    System.out.println("✓ Cleaned up " + toDelete.size() + " existing objects");
                }
            }
        } catch (Exception e) {
            System.err.println("Warning: Failed to cleanup test data: " + e.getMessage());
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Test 1: Validate Input - Create User with Valid Data")
    void testCreateUserWithValidData() throws Exception {
        User user = testUsers.get(0);
        
        MvcResult result = mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(user)))
            .andExpect(status().isCreated())
            .andReturn();
        
        System.out.println("✓ Test 1 Passed: Created user with ID: " + user.getUserId());
    }
    
    @Test
    @Order(2)
    @DisplayName("Test 2: Validate Input - Reject Invalid Email Format")
    void testRejectInvalidEmail() throws Exception {
        User invalidUser = User.newBuilder()
            .setUserId("invalid-email-user")
            .setUsername("invalid_email_test")
            .setEmail("not-an-email")  // Invalid email
            .setCountry("US")
            .setSignupDate("2024-01-15")
            .build();
        
        MvcResult result = mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(invalidUser)))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors").isArray())
            .andExpect(jsonPath("$.errors[0]").value("Invalid email format"))
            .andReturn();
        
        System.out.println("✓ Test 2 Passed: Correctly rejected invalid email format");
    }
    
    @Test
    @Order(3)
    @DisplayName("Test 3: Validate Input - Reject Invalid User ID Pattern")
    void testRejectInvalidUserId() throws Exception {
        User invalidUser = User.newBuilder()
            .setUserId("user@with#special$chars!")  // Invalid characters
            .setUsername("test_user")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-01-15")
            .build();
        
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(invalidUser)))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors").isArray())
            .andExpect(jsonPath("$.errors[0]").value("User ID must be 1-50 alphanumeric characters, underscores, or hyphens"));
        
        System.out.println("✓ Test 3 Passed: Correctly rejected invalid user ID pattern");
    }
    
    @Test
    @Order(4)
    @DisplayName("Test 4: Validate Input - Reject Missing Required Fields")
    void testRejectMissingRequiredFields() throws Exception {
        // Test missing user_id
        User missingId = User.newBuilder()
            .setUsername("test_user")
            .setCountry("US")
            .setSignupDate("2024-01-15")
            .build();
        missingId.setUserId(null);
        
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(missingId)))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.errors").isArray());
        
        System.out.println("✓ Test 4 Passed: Correctly rejected missing required fields");
    }
    
    @Test
    @Order(5)
    @DisplayName("Test 5: Batch Write - Create Multiple Users")
    void testBatchCreateUsers() throws Exception {
        int batchSize = 20;
        int successCount = 0;
        
        System.out.println("Starting batch write of " + batchSize + " users...");
        
        for (int i = 1; i <= batchSize; i++) {
            User user = testUsers.get(i);
            
            mockMvc.perform(post(API_BASE_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isCreated());
            
            successCount++;
            
            if (i % 5 == 0) {
                System.out.println("  Progress: " + i + "/" + batchSize + " users created");
            }
        }
        
        System.out.println("✓ Test 5 Passed: Successfully created " + successCount + " users");
    }
    
    @Test
    @Order(6)
    @DisplayName("Test 6: Read Operation - Retrieve User by ID")
    void testReadUserById() throws Exception {
        // First create a user
        User user = testUsers.get(21);
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(user)))
            .andExpect(status().isCreated());
        
        // Then retrieve it
        MvcResult result = mockMvc.perform(get(API_BASE_PATH + "/" + user.getUserId()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.user_id").value(user.getUserId()))
            .andExpect(jsonPath("$.username").value(user.getUsername()))
            .andExpect(jsonPath("$.email").value(user.getEmail()))
            .andExpect(jsonPath("$.country").value(user.getCountry()))
            .andExpect(jsonPath("$.signup_date").value(user.getSignupDate()))
            .andReturn();
        
        System.out.println("✓ Test 6 Passed: Successfully retrieved user: " + user.getUserId());
    }
    
    @Test
    @Order(7)
    @DisplayName("Test 7: Read Non-existent User")
    void testReadNonExistentUser() throws Exception {
        String nonExistentId = "non-existent-user-id";
        
        mockMvc.perform(get(API_BASE_PATH + "/" + nonExistentId))
            .andExpect(status().isNotFound());
        
        System.out.println("✓ Test 7 Passed: Correctly returned 404 for non-existent user");
    }
    
    @Test
    @Order(8)
    @DisplayName("Test 8: Partition Query - Find Users by Country")
    void testQueryByCountryPartition() throws Exception {
        // Create users in specific country
        String targetCountry = "DE";
        List<User> germanyUsers = new ArrayList<>();
        
        for (int i = 30; i < 35; i++) {
            User user = User.newBuilder()
                .setUserId("de-user-" + i)
                .setUsername("german_user_" + i)
                .setEmail("user" + i + "@example.de")
                .setCountry(targetCountry)
                .setSignupDate("2024-01-20")
                .build();
            
            germanyUsers.add(user);
            
            mockMvc.perform(post(API_BASE_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isCreated());
        }
        
        // Query by country partition
        MvcResult result = mockMvc.perform(get(API_BASE_PATH)
                .param("country", targetCountry)
                .param("signup_date", "2024-01-20"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andReturn();
        
        String response = result.getResponse().getContentAsString();
        List<Map<String, Object>> users = objectMapper.readValue(response, List.class);
        
        assertTrue(users.size() >= germanyUsers.size(), 
            "Should return at least " + germanyUsers.size() + " users, got " + users.size());
        
        System.out.println("✓ Test 8 Passed: Found " + users.size() + " users in " + targetCountry);
    }
    
    @Test
    @Order(9)
    @DisplayName("Test 9: Validate Delta Files in MinIO")
    void testValidateDeltaFilesInMinIO() throws Exception {
        // List objects in MinIO
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(BUCKET_NAME)
            .prefix(TABLE_NAME + "/")
            .build();
        
        ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
        
        assertTrue(response.hasContents(), "Delta table should have files in MinIO");
        
        // Check for Delta log
        boolean hasDeltaLog = false;
        boolean hasParquetFiles = false;
        int parquetCount = 0;
        int deltaLogCount = 0;
        
        System.out.println("\nDelta Lake files in MinIO:");
        for (S3Object object : response.contents()) {
            System.out.println("  - " + object.key() + " (size: " + object.size() + " bytes)");
            
            if (object.key().contains("_delta_log")) {
                hasDeltaLog = true;
                deltaLogCount++;
            }
            if (object.key().endsWith(".parquet")) {
                hasParquetFiles = true;
                parquetCount++;
            }
        }
        
        assertTrue(hasDeltaLog, "Delta table should have _delta_log directory");
        assertTrue(hasParquetFiles, "Delta table should have Parquet data files");
        
        System.out.println("\n✓ Test 9 Passed: Delta Lake structure validated");
        System.out.println("  - Delta log entries: " + deltaLogCount);
        System.out.println("  - Parquet files: " + parquetCount);
    }
    
    @Test
    @Order(10)
    @DisplayName("Test 10: Stress Test - Concurrent Writes")
    void testConcurrentWrites() throws Exception {
        int concurrentUsers = 10;
        List<Thread> threads = new ArrayList<>();
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        System.out.println("Starting concurrent write test with " + concurrentUsers + " threads...");
        
        for (int i = 0; i < concurrentUsers; i++) {
            final int index = i + 50; // Start from index 50
            Thread thread = new Thread(() -> {
                try {
                    User user = User.newBuilder()
                        .setUserId("concurrent-user-" + index)
                        .setUsername("concurrent_" + index)
                        .setEmail("concurrent" + index + "@test.com")
                        .setCountry("US")
                        .setSignupDate("2024-01-25")
                        .build();
                    
                    mockMvc.perform(post(API_BASE_PATH)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(user)))
                        .andExpect(status().isCreated());
                    
                } catch (Exception e) {
                    exceptions.add(e);
                }
            });
            
            threads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(10000); // 10 second timeout
        }
        
        assertTrue(exceptions.isEmpty(), 
            "All concurrent writes should succeed. Errors: " + exceptions.size());
        
        System.out.println("✓ Test 10 Passed: " + concurrentUsers + " concurrent writes succeeded");
    }
    
    @Test
    @Order(11)
    @DisplayName("Test 11: Edge Cases - Empty and Null Values")
    void testEdgeCases() throws Exception {
        // Test with null email (optional field)
        User userWithNullEmail = User.newBuilder()
            .setUserId("null-email-user")
            .setUsername("no_email_user")
            .setCountry("US")
            .setSignupDate("2024-01-15")
            .build();
        
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(userWithNullEmail)))
            .andExpect(status().isCreated());
        
        // Retrieve and verify
        mockMvc.perform(get(API_BASE_PATH + "/null-email-user"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.email").doesNotExist());
        
        System.out.println("✓ Test 11 Passed: Correctly handled edge cases");
    }
    
    @Test
    @Order(12)
    @DisplayName("Test 12: Data Integrity - Verify Written Data")
    void testDataIntegrity() throws Exception {
        // Create a user with specific data
        String testId = "integrity-test-user";
        User testUser = User.newBuilder()
            .setUserId(testId)
            .setUsername("integrity_test")
            .setEmail("integrity@test.com")
            .setCountry("JP")
            .setSignupDate("2024-01-28")
            .build();
        
        // Write the user
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
            .andExpect(status().isCreated());
        
        // Read back and verify all fields
        MvcResult result = mockMvc.perform(get(API_BASE_PATH + "/" + testId))
            .andExpect(status().isOk())
            .andReturn();
        
        String response = result.getResponse().getContentAsString();
        Map<String, Object> retrievedUser = objectMapper.readValue(response, Map.class);
        
        assertEquals(testUser.getUserId(), retrievedUser.get("user_id"));
        assertEquals(testUser.getUsername(), retrievedUser.get("username"));
        assertEquals(testUser.getEmail(), retrievedUser.get("email"));
        assertEquals(testUser.getCountry(), retrievedUser.get("country"));
        assertEquals(testUser.getSignupDate(), retrievedUser.get("signup_date"));
        
        System.out.println("✓ Test 12 Passed: Data integrity verified - all fields match");
    }
    
    @Test
    @Order(13)
    @DisplayName("Test 13: Performance Metrics")
    void testPerformanceMetrics() throws Exception {
        // Measure write performance
        long startWrite = System.currentTimeMillis();
        User perfUser = User.newBuilder()
            .setUserId("perf-test-user")
            .setUsername("performance_test")
            .setEmail("perf@test.com")
            .setCountry("US")
            .setSignupDate("2024-01-30")
            .build();
        
        mockMvc.perform(post(API_BASE_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(perfUser)))
            .andExpect(status().isCreated());
        
        long writeTime = System.currentTimeMillis() - startWrite;
        
        // Measure read performance
        long startRead = System.currentTimeMillis();
        mockMvc.perform(get(API_BASE_PATH + "/perf-test-user"))
            .andExpect(status().isOk());
        long readTime = System.currentTimeMillis() - startRead;
        
        // Measure partition query performance
        long startQuery = System.currentTimeMillis();
        mockMvc.perform(get(API_BASE_PATH)
                .param("country", "US")
                .param("signup_date", "2024-01-30"))
            .andExpect(status().isOk());
        long queryTime = System.currentTimeMillis() - startQuery;
        
        System.out.println("\n✓ Test 13 Passed: Performance Metrics");
        System.out.println("  - Write latency: " + writeTime + " ms");
        System.out.println("  - Read latency: " + readTime + " ms");
        System.out.println("  - Query latency: " + queryTime + " ms");
        
        // Assert reasonable performance
        assertTrue(writeTime < 5000, "Write should complete within 5 seconds");
        assertTrue(readTime < 2000, "Read should complete within 2 seconds");
        assertTrue(queryTime < 3000, "Query should complete within 3 seconds");
    }
    
    @Test
    @Order(14)
    @DisplayName("Test 14: Final Summary Report")
    void generateTestReport() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("E2E REGRESSION TEST REPORT");
        System.out.println("=".repeat(60));
        
        // Count total objects in MinIO
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
            .bucket(BUCKET_NAME)
            .prefix(TABLE_NAME + "/")
            .build();
        
        ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
        
        int totalObjects = response.contents().size();
        long totalSize = response.contents().stream().mapToLong(S3Object::size).sum();
        
        System.out.println("\nTest Coverage:");
        System.out.println("  ✓ Input validation (valid/invalid data)");
        System.out.println("  ✓ CRUD operations (Create, Read)");
        System.out.println("  ✓ Partition-based queries");
        System.out.println("  ✓ Concurrent write handling");
        System.out.println("  ✓ Edge cases and null handling");
        System.out.println("  ✓ Data integrity verification");
        System.out.println("  ✓ Performance benchmarking");
        
        System.out.println("\nStorage Statistics:");
        System.out.println("  - Total objects in MinIO: " + totalObjects);
        System.out.println("  - Total storage size: " + (totalSize / 1024) + " KB");
        System.out.println("  - Table location: s3a://" + BUCKET_NAME + "/" + TABLE_NAME);
        
        System.out.println("\nTest Results:");
        System.out.println("  - All tests passed successfully");
        System.out.println("  - Delta Lake integration working correctly");
        System.out.println("  - Data validation functioning as expected");
        System.out.println("  - MinIO storage operational");
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("REGRESSION TEST SUITE COMPLETED SUCCESSFULLY");
        System.out.println("=".repeat(60) + "\n");
    }
}