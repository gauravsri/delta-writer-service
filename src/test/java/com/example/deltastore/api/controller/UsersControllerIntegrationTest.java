package com.example.deltastore.api.controller;

import com.example.deltastore.schemas.User;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.Arrays;
import java.util.List;

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
class UsersControllerIntegrationTest {

    @Container
    private static final MinIOContainer minioContainer = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-20Z");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private S3Client s3Client;

    @Autowired
    private ObjectMapper objectMapper;

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
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @Test
    void whenPostValidUser_thenReturnsCreatedAndFileIsWritten() throws Exception {
        User newUser = User.newBuilder()
                .setUserId("u123")
                .setUsername("testuser")
                .setEmail("test@example.com")
                .setCountry("US")
                .setSignupDate("2024-01-15")
                .build();

        postUser(newUser);

        var objects = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix("users/").build());
        assertThat(objects.contents()).anyMatch(s3Object -> s3Object.key().endsWith(".parquet"));
    }

    @Test
    void givenUserExists_whenGetByPrimaryKey_thenReturnsUser() throws Exception {
        User newUser = User.newBuilder()
                .setUserId("u456")
                .setUsername("get_test_user")
                .setEmail("get@example.com")
                .setCountry("CA")
                .setSignupDate("2024-02-20")
                .build();
        postUser(newUser);

        MvcResult result = mockMvc.perform(get("/api/v1/users/" + newUser.getUserId())
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andReturn();

        User foundUser = objectMapper.readValue(result.getResponse().getContentAsString(), User.class);
        assertThat(foundUser.getUsername()).isEqualTo(newUser.getUsername());
        assertThat(foundUser.getUserId()).isEqualTo(newUser.getUserId());
    }

    @Test
    void givenRecordsInPartition_whenGetByPartition_thenReturnsOnlyPartitionedData() throws Exception {
        User userUS1 = User.newBuilder().setUserId("u_us_1").setUsername("user_us_1").setCountry("US").setSignupDate("2024-01-15").build();
        User userUS2 = User.newBuilder().setUserId("u_us_2").setUsername("user_us_2").setCountry("US").setSignupDate("2024-01-20").build();
        User userCA1 = User.newBuilder().setUserId("u_ca_1").setUsername("user_ca_1").setCountry("CA").setSignupDate("2024-01-15").build();
        postUser(userUS1);
        postUser(userUS2);
        postUser(userCA1);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("country", "US");
        params.add("signup_date", "2024-01-20");

        MvcResult result = mockMvc.perform(get("/api/v1/users")
                        .params(params)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andReturn();

        List<User> foundUsers = Arrays.asList(objectMapper.readValue(result.getResponse().getContentAsString(), User[].class));
        assertThat(foundUsers).hasSize(1);
        assertThat(foundUsers.get(0).getUserId()).isEqualTo(userUS2.getUserId());
    }

    private void postUser(User user) throws Exception {
        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isCreated());
    }
}
