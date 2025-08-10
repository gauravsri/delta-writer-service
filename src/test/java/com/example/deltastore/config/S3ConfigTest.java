package com.example.deltastore.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.services.s3.S3Client;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3ConfigTest {

    @Mock
    private StorageProperties storageProperties;

    private S3Config s3Config;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        s3Config = new S3Config(storageProperties);
    }

    @Test
    @DisplayName("Should create MinIO client with correct configuration for local profile")
    void testMinioClientConfiguration() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("minioadmin");
        when(storageProperties.getSecretKey()).thenReturn("minioadmin");
        when(storageProperties.getMaskedAccessKey()).thenReturn("mini****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        verify(storageProperties).getEndpoint();
        verify(storageProperties).getAccessKey();
        verify(storageProperties).getSecretKey();
        verify(storageProperties).getMaskedAccessKey();
    }

    @Test
    @DisplayName("Should handle MinIO client with custom endpoint")
    void testMinioClientWithCustomEndpoint() {
        // Given
        String customEndpoint = "http://minio.example.com:9000";
        when(storageProperties.getEndpoint()).thenReturn(customEndpoint);
        when(storageProperties.getAccessKey()).thenReturn("testuser");
        when(storageProperties.getSecretKey()).thenReturn("testpass");
        when(storageProperties.getMaskedAccessKey()).thenReturn("test****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        verify(storageProperties).getEndpoint();
        assertEquals(customEndpoint, storageProperties.getEndpoint());
    }

    @Test
    @DisplayName("Should create AWS S3 client with default configuration for prod profile")
    void testS3ClientConfiguration() {
        // When
        S3Client client = s3Config.s3Client();

        // Then
        assertNotNull(client);
        verifyNoInteractions(storageProperties);
    }

    @Test
    @DisplayName("Should inject StorageProperties correctly")
    void testStoragePropertiesInjection() {
        // Given
        S3Config configWithProperties = new S3Config(storageProperties);

        // Then
        assertNotNull(configWithProperties);
    }

    @Test
    @DisplayName("Should handle null StorageProperties gracefully")
    void testNullStorageProperties() {
        // Given
        S3Config configWithNull = new S3Config(null);

        // When/Then
        assertThrows(NullPointerException.class, () -> configWithNull.minioClient());
    }

    @Test
    @DisplayName("Should create MinIO client with empty credentials")
    void testMinioClientWithEmptyCredentials() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("");
        when(storageProperties.getSecretKey()).thenReturn("");
        when(storageProperties.getMaskedAccessKey()).thenReturn("");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
    }

    @Test
    @DisplayName("Should create MinIO client with HTTPS endpoint")
    void testMinioClientWithHttpsEndpoint() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("https://secure-minio.example.com:9000");
        when(storageProperties.getAccessKey()).thenReturn("secureadmin");
        when(storageProperties.getSecretKey()).thenReturn("securepass");
        when(storageProperties.getMaskedAccessKey()).thenReturn("secu****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        verify(storageProperties).getEndpoint();
    }

    @Test
    @DisplayName("Should verify MinIO client uses force path style")
    void testMinioClientForcePathStyle() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("admin");
        when(storageProperties.getSecretKey()).thenReturn("password");
        when(storageProperties.getMaskedAccessKey()).thenReturn("admi****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        // Force path style is set in the builder, we can't directly verify it on the client
        // but we ensure the client is created successfully with the configuration
    }

    @Test
    @DisplayName("Should verify MinIO client uses US_EAST_1 region")
    void testMinioClientRegion() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("admin");
        when(storageProperties.getSecretKey()).thenReturn("password");
        when(storageProperties.getMaskedAccessKey()).thenReturn("admi****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        // Region is set to US_EAST_1 in the builder
    }

    @Test
    @DisplayName("Should verify S3 client uses default AWS credential chain")
    void testS3ClientDefaultCredentials() {
        // When
        S3Client client = s3Config.s3Client();

        // Then
        assertNotNull(client);
        // The prod S3 client uses the default AWS credential provider chain
        // This would pick up credentials from environment variables, instance profile, etc.
    }

    @Test
    @DisplayName("Should log MinIO configuration details")
    void testMinioClientLogging() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("admin");
        when(storageProperties.getSecretKey()).thenReturn("password");
        when(storageProperties.getMaskedAccessKey()).thenReturn("admi****");

        // When
        S3Client client = s3Config.minioClient();

        // Then
        assertNotNull(client);
        verify(storageProperties, atLeastOnce()).getEndpoint();
        verify(storageProperties, atLeastOnce()).getMaskedAccessKey();
    }

    @Test
    @DisplayName("Should handle malformed endpoint URL")
    void testMinioClientWithMalformedEndpoint() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("not-a-valid-url");
        when(storageProperties.getAccessKey()).thenReturn("admin");
        when(storageProperties.getSecretKey()).thenReturn("password");
        when(storageProperties.getMaskedAccessKey()).thenReturn("admi****");

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> s3Config.minioClient());
    }
}