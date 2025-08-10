package com.example.deltastore.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.storage")
@Data
public class StorageProperties {
    private String bucketName;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    
    public String getMaskedAccessKey() {
        return accessKey != null && accessKey.length() > 4 
            ? accessKey.substring(0, 4) + "****" 
            : "****";
    }
    
    public String getMaskedSecretKey() {
        return secretKey != null && secretKey.length() > 4 
            ? "****" 
            : "****";
    }
}