package com.example.deltastore.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
public class S3Config {

    @Value("${app.storage.endpoint:}")
    private String endpoint;

    @Value("${app.storage.access-key:}")
    private String accessKey;

    @Value("${app.storage.secret-key:}")
    private String secretKey;

    @Profile("local")
    @Bean
    public S3Client minioClient() {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1) // MinIO needs a region
                .forcePathStyle(true)
                .build();
    }

    @Profile("prod")
    @Bean

    public S3Client s3Client() {
        // The client will use the default credential provider chain.
        // For example, IAM roles on EC2, environment variables, or ~/.aws/credentials.
        // The region should also be configured in the environment.
        return S3Client.builder()
                .build();
    }
}
