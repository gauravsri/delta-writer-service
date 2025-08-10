package com.example.deltastore.api.controller;

import com.example.deltastore.api.dto.BatchCreateRequest;
import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.schemas.User;
import com.example.deltastore.service.UserService;
import com.example.deltastore.validation.UserValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(UsersController.class)
class UsersControllerEnhancedTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserService userService;

    @MockBean
    private UserValidator userValidator;

    private User testUser;

    @BeforeEach
    void setUp() {
        testUser = User.newBuilder()
            .setUserId("test123")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();
    }

    @Test
    @DisplayName("Should create user successfully")
    void testCreateUserSuccess() throws Exception {
        // Given
        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());

        // When/Then
        mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isCreated());

        verify(userValidator).validate(any(User.class));
        verify(userService).save(any(User.class));
    }

    @Test
    @DisplayName("Should return validation errors for invalid user")
    void testCreateUserValidationErrors() throws Exception {
        // Given
        List<String> validationErrors = Arrays.asList(
            "User ID is required",
            "Username is required"
        );
        when(userValidator.validate(any(User.class))).thenReturn(validationErrors);

        // When/Then
        mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("User ID is required"))
                .andExpect(jsonPath("$.errors[1]").value("Username is required"));

        verify(userValidator).validate(any(User.class));
        verify(userService, never()).save(any(User.class));
    }

    @Test
    @DisplayName("Should handle service exception during user creation")
    void testCreateUserServiceException() throws Exception {
        // Given
        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());
        doThrow(new RuntimeException("Database error")).when(userService).save(any(User.class));

        // When/Then
        mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Failed to create user"));

        verify(userService).save(any(User.class));
    }

    @Test
    @DisplayName("Should create batch of users successfully")
    void testCreateUsersBatchSuccess() throws Exception {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);

        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(2)
            .successCount(2)
            .failureCount(0)
            .successfulUserIds(Arrays.asList("user1", "user2"))
            .failures(Collections.emptyList())
            .processedAt(LocalDateTime.now())
            .processingTimeMs(100L)
            .statistics(BatchCreateResponse.BatchStatistics.builder()
                .totalBatches(1)
                .avgBatchSize(2)
                .avgProcessingTimePerBatch(100L)
                .totalDeltaTransactionTime(100L)
                .deltaTransactionCount(1)
                .additionalMetrics(Map.of("test", "data"))
                .build())
            .build();

        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());
        when(userService.saveBatch(any())).thenReturn(response);

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.totalRequested").value(2))
                .andExpect(jsonPath("$.successCount").value(2))
                .andExpect(jsonPath("$.failureCount").value(0))
                .andExpect(jsonPath("$.successfulUserIds[0]").value("user1"))
                .andExpect(jsonPath("$.successfulUserIds[1]").value("user2"));

        verify(userService).saveBatch(any());
    }

    @Test
    @DisplayName("Should handle batch with partial failures")
    void testCreateUsersBatchPartialFailure() throws Exception {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);

        BatchCreateResponse response = BatchCreateResponse.builder()
            .totalRequested(2)
            .successCount(1)
            .failureCount(1)
            .successfulUserIds(Arrays.asList("user1"))
            .failures(Arrays.asList(
                BatchCreateResponse.FailureDetail.builder()
                    .userId("user2")
                    .index(1)
                    .error("Write failed")
                    .errorType("RuntimeException")
                    .build()
            ))
            .processedAt(LocalDateTime.now())
            .processingTimeMs(200L)
            .build();

        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());
        when(userService.saveBatch(any())).thenReturn(response);

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isPartialContent())
                .andExpect(jsonPath("$.totalRequested").value(2))
                .andExpect(jsonPath("$.successCount").value(1))
                .andExpect(jsonPath("$.failureCount").value(1))
                .andExpect(jsonPath("$.failures[0].userId").value("user2"));
    }

    @Test
    @DisplayName("Should reject empty batch request")
    void testCreateUsersBatchEmptyRequest() throws Exception {
        // Given
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(Collections.emptyList());

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Users list cannot be empty"));

        verify(userService, never()).saveBatch(any());
    }

    @Test
    @DisplayName("Should reject batch request with null users")
    void testCreateUsersBatchNullUsers() throws Exception {
        // Given
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(null);

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Users list cannot be empty"));
    }

    @Test
    @DisplayName("Should reject batch request exceeding size limit")
    void testCreateUsersBatchTooLarge() throws Exception {
        // Given
        List<User> tooManyUsers = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            tooManyUsers.add(createUser("user" + i, "user" + i + "@example.com"));
        }
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(tooManyUsers);

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Batch size cannot exceed 1000 users"));
    }

    @Test
    @DisplayName("Should reject batch with validation errors")
    void testCreateUsersBatchValidationErrors() throws Exception {
        // Given
        List<User> users = Arrays.asList(
            createUser("", "invalid@example.com"), // Invalid user ID
            createUser("user2", "user2@example.com")
        );
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);

        // Mock validation to return errors for first user only
        when(userValidator.validate(eq(users.get(0))))
            .thenReturn(Arrays.asList("User ID is required"));
        when(userValidator.validate(eq(users.get(1))))
            .thenReturn(Collections.emptyList());

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors").isArray())
                .andExpect(jsonPath("$.errors[0]").value("User 0: User ID is required"));

        verify(userService, never()).saveBatch(any());
    }

    @Test
    @DisplayName("Should handle batch service exception")
    void testCreateUsersBatchServiceException() throws Exception {
        // Given
        List<User> users = Arrays.asList(createUser("user1", "user1@example.com"));
        BatchCreateRequest request = new BatchCreateRequest();
        request.setUsers(users);

        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());
        when(userService.saveBatch(any())).thenThrow(new RuntimeException("Database error"));

        // When/Then
        mockMvc.perform(post("/api/v1/users/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Failed to create user batch"));
    }

    @Test
    @DisplayName("Should get user by ID successfully")
    void testGetUserByIdSuccess() throws Exception {
        // Given
        when(userService.findById("test123")).thenReturn(Optional.of(testUser));

        // When/Then
        mockMvc.perform(get("/api/v1/users/test123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId").value("test123"))
                .andExpect(jsonPath("$.username").value("testuser"))
                .andExpect(jsonPath("$.email").value("test@example.com"));
    }

    @Test
    @DisplayName("Should return 404 when user not found")
    void testGetUserByIdNotFound() throws Exception {
        // Given
        when(userService.findById("nonexistent")).thenReturn(Optional.empty());

        // When/Then
        mockMvc.perform(get("/api/v1/users/nonexistent"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Should reject empty user ID")
    void testGetUserByIdEmptyId() throws Exception {
        // When/Then
        mockMvc.perform(get("/api/v1/users/ "))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("User ID cannot be empty"));
    }

    @Test
    @DisplayName("Should reject user ID that is too long")
    void testGetUserByIdTooLong() throws Exception {
        // Given
        String longUserId = "a".repeat(51);

        // When/Then
        mockMvc.perform(get("/api/v1/users/" + longUserId))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("User ID must be 50 characters or less"));
    }

    @Test
    @DisplayName("Should handle service exception during user retrieval")
    void testGetUserByIdServiceException() throws Exception {
        // Given
        when(userService.findById("test123")).thenThrow(new RuntimeException("Database error"));

        // When/Then
        mockMvc.perform(get("/api/v1/users/test123"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Failed to retrieve user"));
    }

    @Test
    @DisplayName("Should find users by partition filters successfully")
    void testFindUsersByPartitionSuccess() throws Exception {
        // Given
        List<User> users = Arrays.asList(
            createUser("user1", "user1@example.com"),
            createUser("user2", "user2@example.com")
        );
        Map<String, String> filters = Map.of("country", "US");
        when(userService.findByPartitions(filters)).thenReturn(users);

        // When/Then
        mockMvc.perform(get("/api/v1/users")
                .param("country", "US"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].userId").value("user1"))
                .andExpect(jsonPath("$[1].userId").value("user2"));
    }

    @Test
    @DisplayName("Should return empty list when no users match partition filters")
    void testFindUsersByPartitionEmpty() throws Exception {
        // Given
        Map<String, String> filters = Map.of("country", "CA");
        when(userService.findByPartitions(filters)).thenReturn(Collections.emptyList());

        // When/Then
        mockMvc.perform(get("/api/v1/users")
                .param("country", "CA"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    @DisplayName("Should handle service exception during partition search")
    void testFindUsersByPartitionServiceException() throws Exception {
        // Given
        when(userService.findByPartitions(any())).thenThrow(new RuntimeException("Database error"));

        // When/Then
        mockMvc.perform(get("/api/v1/users")
                .param("country", "US"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Failed to retrieve users"));
    }

    @Test
    @DisplayName("Should handle multiple partition filters")
    void testFindUsersByMultiplePartitionFilters() throws Exception {
        // Given
        List<User> users = Arrays.asList(createUser("user1", "user1@example.com"));
        Map<String, String> expectedFilters = Map.of("country", "US", "signup_date", "2024-01-01");
        when(userService.findByPartitions(expectedFilters)).thenReturn(users);

        // When/Then
        mockMvc.perform(get("/api/v1/users")
                .param("country", "US")
                .param("signup_date", "2024-01-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    @DisplayName("Should reject partition filters with empty keys")
    void testFindUsersByPartitionEmptyKey() throws Exception {
        // When/Then
        mockMvc.perform(get("/api/v1/users")
                .param("", "US"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Partition filter keys cannot be empty"));
    }

    @Test
    @DisplayName("Should handle invalid JSON in request body")
    void testCreateUserInvalidJson() throws Exception {
        // When/Then
        mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{invalid json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("Should handle missing Content-Type header")
    void testCreateUserMissingContentType() throws Exception {
        // When/Then
        mockMvc.perform(post("/api/v1/users")
                .content(objectMapper.writeValueAsString(testUser)))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    @DisplayName("Should handle OPTIONS request")
    void testOptionsRequest() throws Exception {
        // When/Then
        mockMvc.perform(options("/api/v1/users"))
                .andExpect(status().isOk());
    }

    private User createUser(String userId, String email) {
        return User.newBuilder()
            .setUserId(userId)
            .setUsername(userId + "_name")
            .setEmail(email)
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();
    }
}