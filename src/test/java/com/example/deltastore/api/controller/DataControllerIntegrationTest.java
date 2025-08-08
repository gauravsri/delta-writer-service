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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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

        var objects = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
        assertThat(objects.contents()).anyMatch(s3Object -> s3Object.key().endsWith(".parquet"));
    }
}
