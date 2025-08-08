package com.example.deltastore.api.controller;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import org.junit.jupiter.api.Disabled;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("local")
@Testcontainers
class DataControllerIntegrationTest {

    @Container
    private static final MinIOContainer minioContainer = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-20Z");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private S3Client s3Client;

    @Value("${app.storage.bucket-name}")
    private String bucketName;

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        minioContainer.start();
        registry.add("app.storage.endpoint", minioContainer::getS3URL);
        registry.add("app.storage.access-key", minioContainer::getUserName);
        registry.add("app.storage.secret-key", minioContainer::getPassword);
        registry.add("app.storage.bucket-name", () -> "test-bucket");
    }

    @BeforeEach
    void setUp() {
        // Ensure bucket exists before each test
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @Test
    void whenPostValidData_thenReturnsCreatedAndFileIsWritten() throws Exception {
        String userJson = """
                {
                    "user_id": "u123",
                    "username": "testuser",
                    "email": "test@example.com",
                    "country": "US",
                    "signup_date": "2024-01-15"
                }
                """;

        mockMvc.perform(post("/api/v1/data/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(userJson))
                .andExpect(status().isCreated());

        var objects = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix("users/").build());
        assertThat(objects.contents()).anyMatch(s3Object -> s3Object.key().endsWith(".parquet"));
    }

    @Test
    void givenRecordExists_whenGetByPrimaryKey_thenReturnsRecord() throws Exception {
        // Step 1: Create a record to ensure it exists
        String userId = "u456";
        String userJson = String.format("""
                {
                    "user_id": "%s",
                    "username": "get_test_user",
                    "email": "get@example.com",
                    "country": "CA",
                    "signup_date": "2024-02-20"
                }
                """, userId);

        mockMvc.perform(post("/api/v1/data/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(userJson))
                .andExpect(status().isCreated());

        // Step 2: Attempt to GET the record by its primary key
        mockMvc.perform(get("/api/v1/data/users/" + userId)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("get_test_user"))
                .andExpect(jsonPath("$.user_id").value(userId));
    }

    @Test
    void givenRecordsInPartition_whenGetByPartition_thenReturnsOnlyPartitionedData() throws Exception {
        // Step 1: Create multiple records in different partitions
        postUser("u_us_1", "user_us_1", "US", "2024-01-15");
        postUser("u_us_2", "user_us_2", "US", "2024-01-20");
        postUser("u_ca_1", "user_ca_1", "CA", "2024-01-15");

        // Step 2: GET the data with a partition filter
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("country", "US");
        params.add("signup_date", "2024-01-15");

        mockMvc.perform(get("/api/v1/data/users")
                        .params(params)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].user_id").value("u_us_1"));
    }

    private void postUser(String userId, String username, String country, String signupDate) throws Exception {
        String userJson = String.format("""
                {
                    "user_id": "%s",
                    "username": "%s",
                    "country": "%s",
                    "signup_date": "%s"
                }
                """, userId, username, country, signupDate);

        mockMvc.perform(post("/api/v1/data/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(userJson))
                .andExpect(status().isCreated());
    }
}
