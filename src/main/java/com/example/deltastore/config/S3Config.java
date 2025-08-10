package com.example.deltastore.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.net.URI;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class S3Config {

    private final StorageProperties storageProperties;

    @Profile("local")
    @Bean
    public S3Client minioClient() {
        log.info("Configuring MinIO client for endpoint: {}", storageProperties.getEndpoint());
        log.debug("Using access key: {}", storageProperties.getMaskedAccessKey());
        
        return S3Client.builder()
                .endpointOverride(URI.create(storageProperties.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                            storageProperties.getAccessKey(), 
                            storageProperties.getSecretKey())))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
    }

    @Profile("prod")
    @Bean
    public S3Client s3Client() {
        log.info("Configuring AWS S3 client using default credential provider chain");
        
        return S3Client.builder()
                .build();
    }
}
