package com.example.deltastore.config;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class BucketInitializerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private StorageProperties storageProperties;

    private BucketInitializer bucketInitializer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        bucketInitializer = new BucketInitializer(s3Client, storageProperties);
    }

    @Test
    void testBucketExistsAlready() {
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        
        // Mock successful headBucket call (bucket exists)
        when(s3Client.headBucket(any(HeadBucketRequest.class)))
            .thenReturn(null); // Successful response

        assertDoesNotThrow(() -> bucketInitializer.ensureBucketExists());
        
        verify(s3Client).headBucket(any(HeadBucketRequest.class));
        verify(s3Client, never()).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    void testCreateBucketWhenNotExists() {
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        
        // Mock bucket doesn't exist
        when(s3Client.headBucket(any(HeadBucketRequest.class)))
            .thenThrow(NoSuchBucketException.builder().build());
        
        // Mock successful bucket creation
        when(s3Client.createBucket(any(CreateBucketRequest.class)))
            .thenReturn(null);

        assertDoesNotThrow(() -> bucketInitializer.ensureBucketExists());
        
        verify(s3Client).headBucket(any(HeadBucketRequest.class));
        verify(s3Client).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    void testCreateBucketFailure() {
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        
        // Mock bucket doesn't exist
        when(s3Client.headBucket(any(HeadBucketRequest.class)))
            .thenThrow(NoSuchBucketException.builder().build());
        
        // Mock bucket creation failure
        when(s3Client.createBucket(any(CreateBucketRequest.class)))
            .thenThrow(new RuntimeException("Failed to create bucket"));

        // Should not throw exception (logs error and continues)
        assertDoesNotThrow(() -> bucketInitializer.ensureBucketExists());
        
        verify(s3Client).headBucket(any(HeadBucketRequest.class));
        verify(s3Client).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    void testHeadBucketGeneralFailure() {
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        
        // Mock general exception (not NoSuchBucketException)
        when(s3Client.headBucket(any(HeadBucketRequest.class)))
            .thenThrow(new RuntimeException("Connection failed"));

        // Should not throw exception (logs error and continues)
        assertDoesNotThrow(() -> bucketInitializer.ensureBucketExists());
        
        verify(s3Client).headBucket(any(HeadBucketRequest.class));
        verify(s3Client, never()).createBucket(any(CreateBucketRequest.class));
    }

    @Test
    void testGetBucketName() {
        when(storageProperties.getBucketName()).thenReturn("my-test-bucket");
        
        when(s3Client.headBucket(any(HeadBucketRequest.class)))
            .thenReturn(null);

        bucketInitializer.ensureBucketExists();
        
        verify(storageProperties).getBucketName();
    }

    @Test
    void testConstructorWithRequiredArgsAnnotation() {
        // Test that the constructor works properly with @RequiredArgsConstructor
        assertNotNull(bucketInitializer);
    }

    @Test
    void testComponentAnnotation() {
        // This test verifies the class is properly annotated as a Spring Component
        assertTrue(BucketInitializer.class.isAnnotationPresent(org.springframework.stereotype.Component.class));
    }

    @Test
    void testProfileAnnotation() {
        // This test verifies the class is properly annotated with @Profile("local")
        assertTrue(BucketInitializer.class.isAnnotationPresent(org.springframework.context.annotation.Profile.class));
        org.springframework.context.annotation.Profile profile = BucketInitializer.class.getAnnotation(org.springframework.context.annotation.Profile.class);
        assertEquals("local", profile.value()[0]);
    }

    @Test
    void testSlf4jAnnotation() {
        // This test verifies the class is properly annotated with @Slf4j
        assertTrue(BucketInitializer.class.isAnnotationPresent(lombok.extern.slf4j.Slf4j.class));
    }
}