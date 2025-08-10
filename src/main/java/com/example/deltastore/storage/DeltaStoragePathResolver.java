package com.example.deltastore.storage;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.config.StorageProperties;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Strategic storage path resolver that handles different storage backends
 * and partitioning strategies dynamically.
 */
@Component
@Slf4j
public class DeltaStoragePathResolver {
    
    private final DeltaStoreConfiguration config;
    private final StorageProperties storageProperties; // Legacy support
    private final PartitionStrategyFactory partitionStrategyFactory;
    
    public DeltaStoragePathResolver(DeltaStoreConfiguration config, 
                                   StorageProperties storageProperties) {
        this.config = config;
        this.storageProperties = storageProperties;
        this.partitionStrategyFactory = new PartitionStrategyFactory();
    }
    
    /**
     * Resolves complete storage path for a table with partitions
     */
    public StoragePath resolveTablePath(String entityType, Map<String, Object> partitionValues) {
        DeltaStoreConfiguration.TableConfig tableConfig = config.getTableConfigOrDefault(entityType);
        
        String basePath = buildBasePath(entityType);
        String partitionPath = buildPartitionPath(tableConfig, partitionValues);
        
        StoragePath path = StoragePath.builder()
            .basePath(basePath)
            .partitionPath(partitionPath)
            .fullPath(basePath + partitionPath)
            .entityType(entityType)
            .storageType(config.getStorage().getType())
            .build();
        
        log.debug("Resolved storage path for {}: {}", entityType, path.getFullPath());
        return path;
    }
    
    /**
     * Resolves base table path without partitions
     */
    public String resolveBaseTablePath(String entityType) {
        return buildBasePath(entityType);
    }
    
    /**
     * Builds base path based on storage type
     */
    private String buildBasePath(String entityType) {
        DeltaStoreConfiguration.StorageType storageType = config.getStorage().getType();
        String basePath = config.getStorage().getBasePath();
        
        switch (storageType) {
            case S3A:
                return buildS3APath(entityType, basePath);
            case LOCAL:
                return buildLocalPath(entityType, basePath);
            case HDFS:
                return buildHdfsPath(entityType, basePath);
            case AZURE:
                return buildAzurePath(entityType, basePath);
            case GCS:
                return buildGcsPath(entityType, basePath);
            default:
                throw new UnsupportedStorageTypeException("Unsupported storage type: " + storageType);
        }
    }
    
    /**
     * Builds S3A storage path
     */
    private String buildS3APath(String entityType, String basePath) {
        String bucketName = storageProperties.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            throw new IllegalArgumentException("S3 bucket name not configured");
        }
        return String.format("s3a://%s%s/%s", bucketName, basePath, entityType);
    }
    
    /**
     * Builds local filesystem path
     */
    private String buildLocalPath(String entityType, String basePath) {
        String localBasePath = basePath.startsWith("/") ? basePath : "/tmp" + basePath;
        return String.format("file://%s/%s", localBasePath, entityType);
    }
    
    /**
     * Builds HDFS path
     */
    private String buildHdfsPath(String entityType, String basePath) {
        // Would need HDFS namenode configuration
        String hdfsNamenode = "hdfs://namenode:8020"; // Could be configurable
        return String.format("%s%s/%s", hdfsNamenode, basePath, entityType);
    }
    
    /**
     * Builds Azure Data Lake path
     */
    private String buildAzurePath(String entityType, String basePath) {
        // Would need Azure account and container configuration
        String azureAccount = "myaccount"; // Could be configurable
        String container = "delta-tables";
        return String.format("abfss://%s@%s.dfs.core.windows.net%s/%s", 
            container, azureAccount, basePath, entityType);
    }
    
    /**
     * Builds Google Cloud Storage path
     */
    private String buildGcsPath(String entityType, String basePath) {
        String bucketName = storageProperties.getBucketName(); // Reuse bucket name
        return String.format("gs://%s%s/%s", bucketName, basePath, entityType);
    }
    
    /**
     * Builds partition path using configured strategy
     */
    private String buildPartitionPath(DeltaStoreConfiguration.TableConfig tableConfig, 
                                    Map<String, Object> partitionValues) {
        if (partitionValues == null || partitionValues.isEmpty()) {
            return "";
        }
        
        DeltaStoreConfiguration.PartitionStrategy strategy = 
            tableConfig.getPartitionStrategy() != null ? 
            tableConfig.getPartitionStrategy() : 
            config.getStorage().getPartitionStrategy();
        
        PartitionStrategy partitioner = partitionStrategyFactory.getStrategy(strategy);
        return partitioner.buildPartitionPath(partitionValues);
    }
    
    /**
     * Factory for partition strategies
     */
    private static class PartitionStrategyFactory {
        
        public PartitionStrategy getStrategy(DeltaStoreConfiguration.PartitionStrategy strategyType) {
            switch (strategyType) {
                case DATE_BASED:
                    return new DateBasedPartitionStrategy();
                case HASH_BASED:
                    return new HashBasedPartitionStrategy();
                case RANGE_BASED:
                    return new RangeBasedPartitionStrategy();
                case NONE:
                default:
                    return new NoPartitionStrategy();
            }
        }
    }
    
    /**
     * Interface for partition strategies
     */
    public interface PartitionStrategy {
        String buildPartitionPath(Map<String, Object> partitionValues);
    }
    
    /**
     * No partitioning strategy
     */
    private static class NoPartitionStrategy implements PartitionStrategy {
        @Override
        public String buildPartitionPath(Map<String, Object> partitionValues) {
            return "";
        }
    }
    
    /**
     * Date-based partitioning strategy (year/month/day)
     */
    private static class DateBasedPartitionStrategy implements PartitionStrategy {
        
        @Override
        public String buildPartitionPath(Map<String, Object> partitionValues) {
            LocalDate date = extractDate(partitionValues);
            if (date == null) {
                return "";
            }
            
            return String.format("/year=%d/month=%02d/day=%02d", 
                date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        }
        
        private LocalDate extractDate(Map<String, Object> partitionValues) {
            // Try common date field names
            String[] dateFields = {"date", "signup_date", "created_date", "order_date", "event_date"};
            
            for (String field : dateFields) {
                Object value = partitionValues.get(field);
                if (value != null) {
                    try {
                        if (value instanceof LocalDate) {
                            return (LocalDate) value;
                        } else if (value instanceof String) {
                            return LocalDate.parse((String) value, DateTimeFormatter.ISO_LOCAL_DATE);
                        }
                    } catch (Exception e) {
                        // Continue trying other fields
                    }
                }
            }
            
            return LocalDate.now(); // Default to current date
        }
    }
    
    /**
     * Hash-based partitioning strategy
     */
    private static class HashBasedPartitionStrategy implements PartitionStrategy {
        
        @Override
        public String buildPartitionPath(Map<String, Object> partitionValues) {
            if (partitionValues.isEmpty()) {
                return "";
            }
            
            // Use first value for hashing
            Object firstValue = partitionValues.values().iterator().next();
            int hash = firstValue.hashCode();
            int partition = Math.abs(hash % 100); // 100 partitions
            
            return String.format("/partition=%02d", partition);
        }
    }
    
    /**
     * Range-based partitioning strategy
     */
    private static class RangeBasedPartitionStrategy implements PartitionStrategy {
        
        @Override
        public String buildPartitionPath(Map<String, Object> partitionValues) {
            // Simple range partitioning based on numeric values
            for (Object value : partitionValues.values()) {
                if (value instanceof Number) {
                    long numValue = ((Number) value).longValue();
                    String range = determineRange(numValue);
                    return "/range=" + range;
                }
            }
            
            return "";
        }
        
        private String determineRange(long value) {
            if (value < 1000) return "0-1K";
            if (value < 10000) return "1K-10K";
            if (value < 100000) return "10K-100K";
            return "100K+";
        }
    }
    
    /**
     * Exception for unsupported storage types
     */
    public static class UnsupportedStorageTypeException extends RuntimeException {
        public UnsupportedStorageTypeException(String message) {
            super(message);
        }
    }
}