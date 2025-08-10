package com.example.deltastore.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Centralized configuration management for Delta Store.
 * Replaces scattered constants with organized, type-safe configuration.
 */
@ConfigurationProperties(prefix = "deltastore")
@Component
@Data
@Slf4j
public class DeltaStoreConfiguration {
    
    // Performance Configuration
    private Performance performance = new Performance();
    
    // Schema Configuration
    private SchemaConfig schema = new SchemaConfig();
    
    // Storage Configuration  
    private Storage storage = new Storage();
    
    // Table Configuration
    private Map<String, TableConfig> tables = new HashMap<>();
    
    @PostConstruct
    public void logConfiguration() {
        log.info("DeltaStore Configuration loaded:");
        log.info("  Performance - Cache TTL: {}ms, Batch timeout: {}ms", 
            performance.cacheTtlMs, performance.batchTimeoutMs);
        log.info("  Schema - Evolution policy: {}, Auto-register: {}", 
            schema.evolutionPolicy, schema.autoRegisterSchemas);
        log.info("  Storage - Type: {}, Base path: {}", 
            storage.type, storage.basePath);
        log.info("  Tables configured: {}", tables.keySet());
    }
    
    @Data
    public static class Performance {
        private long cacheTtlMs = 30000;           // 30 seconds
        private long batchTimeoutMs = 50;          // 50ms
        private int maxBatchSize = 100;            // Max records per batch
        private int maxRetries = 3;                // Retry attempts
        private int connectionPoolSize = 200;      // Connection pool size
        private long writeTimeoutMs = 30000;       // 30 seconds write timeout
        private int commitThreads = 2;             // Commit executor threads
    }
    
    @Data
    public static class SchemaConfig {
        private SchemaEvolutionPolicy evolutionPolicy = SchemaEvolutionPolicy.BACKWARD_COMPATIBLE;
        private boolean enableSchemaValidation = true;
        private boolean autoRegisterSchemas = true;
        private boolean cacheSchemas = true;
        private long schemaCacheTtlMs = 300000;    // 5 minutes
    }
    
    @Data
    public static class Storage {
        private StorageType type = StorageType.S3A;
        private String basePath = "/delta-tables";
        private PartitionStrategy partitionStrategy = PartitionStrategy.NONE;
        private boolean enableCompression = true;
        private String compressionCodec = "snappy";
    }
    
    @Data
    public static class TableConfig {
        private String primaryKeyColumn;
        private List<String> partitionColumns;
        private Map<String, String> properties = new HashMap<>();
        private SchemaEvolutionPolicy evolutionPolicy;
        private PartitionStrategy partitionStrategy;
        private boolean enableOptimization = true;
        private long optimizationIntervalMs = 3600000; // 1 hour
    }
    
    // Enums for type safety
    public enum SchemaEvolutionPolicy {
        BACKWARD_COMPATIBLE,
        FORWARD_COMPATIBLE, 
        FULL_COMPATIBLE,
        NONE
    }
    
    public enum StorageType {
        S3A,
        LOCAL,
        HDFS,
        AZURE,
        GCS
    }
    
    public enum PartitionStrategy {
        NONE,
        DATE_BASED,
        HASH_BASED,
        RANGE_BASED,
        CUSTOM
    }
    
    /**
     * Get table configuration with fallback to defaults
     */
    public TableConfig getTableConfigOrDefault(String tableName) {
        return tables.getOrDefault(tableName, createDefaultTableConfig());
    }
    
    /**
     * Creates default table configuration
     */
    private TableConfig createDefaultTableConfig() {
        TableConfig defaultConfig = new TableConfig();
        defaultConfig.setEvolutionPolicy(schema.getEvolutionPolicy());
        defaultConfig.setPartitionStrategy(storage.getPartitionStrategy());
        return defaultConfig;
    }
    
    /**
     * Validates configuration on startup
     */
    public void validateConfiguration() {
        // Validate performance settings
        if (performance.getCacheTtlMs() <= 0) {
            throw new IllegalArgumentException("Cache TTL must be positive");
        }
        if (performance.getBatchTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Batch timeout must be positive");
        }
        if (performance.getMaxBatchSize() <= 0) {
            throw new IllegalArgumentException("Max batch size must be positive");
        }
        
        // Validate storage settings
        if (storage.getBasePath() == null || storage.getBasePath().trim().isEmpty()) {
            throw new IllegalArgumentException("Storage base path cannot be empty");
        }
        
        log.info("Configuration validation passed");
    }
}