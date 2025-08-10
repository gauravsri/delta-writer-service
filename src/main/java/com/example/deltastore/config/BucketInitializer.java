package com.example.deltastore.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

@Component
@Profile("local")
@RequiredArgsConstructor
@Slf4j
public class BucketInitializer {
    
    private final S3Client s3Client;
    private final StorageProperties storageProperties;
    
    @EventListener(ApplicationReadyEvent.class)
    public void ensureBucketExists() {
        String bucketName = storageProperties.getBucketName();
        
        try {
            // Check if bucket exists
            try {
                s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
                log.info("✓ Bucket '{}' already exists in MinIO", bucketName);
            } catch (NoSuchBucketException e) {
                // Create the bucket
                s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
                log.info("✓ Created bucket '{}' in MinIO", bucketName);
            }
        } catch (Exception e) {
            log.error("Failed to ensure bucket exists: {}", bucketName, e);
        }
    }
}