package com.example.deltastore.storage;

import com.example.deltastore.config.DeltaStoreConfiguration;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a complete storage path with all components
 */
@Data
@Builder
public class StoragePath {
    private String basePath;
    private String partitionPath;
    private String fullPath;
    private String entityType;
    private DeltaStoreConfiguration.StorageType storageType;
    
    /**
     * Gets the table path without partitions
     */
    public String getTablePath() {
        return basePath;
    }
    
    /**
     * Checks if path has partitions
     */
    public boolean isPartitioned() {
        return partitionPath != null && !partitionPath.isEmpty();
    }
    
    /**
     * Gets storage protocol (s3a, file, hdfs, etc.)
     */
    public String getProtocol() {
        if (fullPath == null) {
            return "unknown";
        }
        
        int protocolIndex = fullPath.indexOf("://");
        if (protocolIndex > 0) {
            return fullPath.substring(0, protocolIndex);
        }
        
        return "file"; // Default
    }
}