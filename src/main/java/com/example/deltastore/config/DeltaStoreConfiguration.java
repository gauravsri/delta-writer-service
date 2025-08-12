package com.example.deltastore.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

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
        private long batchTimeoutMs = 50;          // 50ms - reduced for faster response
        private int maxBatchSize = 1000;           // Increased from 100 for better Parquet file sizes  
        private int maxRetries = 3;                // Retry attempts
        private int connectionPoolSize = 200;      // Connection pool size
        private long writeTimeoutMs = 30000;       // 30 seconds write timeout
        private int commitThreads = 2;             // Commit executor threads
        private int checkpointInterval = 10;       // Create checkpoint every N versions
        private int optimalParquetSizeMB = 256;    // Target Parquet file size in MB
        private boolean enableBatchConsolidation = true; // Enable batch consolidation
        
        // HIGH PRIORITY ISSUE #6: Additional timeout settings for missing timeout handling
        private long schemaOperationTimeoutMs = 10000;     // 10 seconds for schema operations
        private long batchProcessingTimeoutMs = 60000;     // 60 seconds for batch processing
        private long checkpointTimeoutMs = 120000;         // 2 minutes for checkpoint operations
        private long backgroundOperationTimeoutMs = 30000; // 30 seconds for background tasks
        private long threadPoolAwaitTimeoutMs = 10000;     // 10 seconds for thread pool shutdown
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
     * Validates configuration on startup with comprehensive checks
     */
    public void validateConfiguration() {
        List<String> errors = new ArrayList<>();
        
        // Validate performance settings
        validatePerformanceSettings(errors);
        
        // Validate storage settings  
        validateStorageSettings(errors);
        
        // Validate table configurations
        validateTableConfigurations(errors);
        
        // Validate schema settings
        validateSchemaSettings(errors);
        
        // Validate thread pool sizing
        validateThreadPoolSizing(errors);
        
        // Validate monitoring settings
        validateMonitoringSettings(errors);
        
        if (!errors.isEmpty()) {
            String errorMessage = "Configuration validation failed:\n" + String.join("\n", errors);
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        
        log.info("Configuration validation passed - all {} settings are valid", 
            getTotalConfigurationSettings());
    }
    
    private void validatePerformanceSettings(List<String> errors) {
        if (performance == null) {
            errors.add("Performance configuration is required");
            return;
        }
        
        // Cache TTL validation
        if (performance.getCacheTtlMs() <= 0) {
            errors.add("Cache TTL must be positive, got: " + performance.getCacheTtlMs());
        } else if (performance.getCacheTtlMs() > 3600000) { // 1 hour max
            errors.add("Cache TTL should not exceed 1 hour (3600000ms), got: " + performance.getCacheTtlMs());
        }
        
        // Batch timeout validation
        if (performance.getBatchTimeoutMs() <= 0) {
            errors.add("Batch timeout must be positive, got: " + performance.getBatchTimeoutMs());
        } else if (performance.getBatchTimeoutMs() > 300000) { // 5 minutes max
            errors.add("Batch timeout should not exceed 5 minutes (300000ms), got: " + performance.getBatchTimeoutMs());
        }
        
        // Batch size validation
        if (performance.getMaxBatchSize() <= 0) {
            errors.add("Max batch size must be positive, got: " + performance.getMaxBatchSize());
        } else if (performance.getMaxBatchSize() > 10000) {
            errors.add("Max batch size should not exceed 10000 for memory safety, got: " + performance.getMaxBatchSize());
        }
        
        // Max retries validation
        if (performance.getMaxRetries() < 0) {
            errors.add("Max retries cannot be negative, got: " + performance.getMaxRetries());
        } else if (performance.getMaxRetries() > 10) {
            errors.add("Max retries should not exceed 10 to prevent infinite loops, got: " + performance.getMaxRetries());
        }
        
        // Thread pool validation
        if (performance.getCommitThreads() <= 0) {
            errors.add("Commit threads must be positive, got: " + performance.getCommitThreads());
        } else if (performance.getCommitThreads() > 20) {
            errors.add("Commit threads should not exceed 20 to prevent resource exhaustion, got: " + performance.getCommitThreads());
        }
        
        // Connection pool validation
        if (performance.getConnectionPoolSize() <= 0) {
            errors.add("Connection pool size must be positive, got: " + performance.getConnectionPoolSize());
        } else if (performance.getConnectionPoolSize() > 1000) {
            errors.add("Connection pool size should not exceed 1000 to prevent resource exhaustion, got: " + performance.getConnectionPoolSize());
        }
        
        // Write timeout validation
        if (performance.getWriteTimeoutMs() <= 0) {
            errors.add("Write timeout must be positive, got: " + performance.getWriteTimeoutMs());
        } else if (performance.getWriteTimeoutMs() > 300000) { // 5 minutes max
            errors.add("Write timeout should not exceed 5 minutes (300000ms), got: " + performance.getWriteTimeoutMs());
        }
        
        // Checkpoint interval validation
        if (performance.getCheckpointInterval() <= 0) {
            errors.add("Checkpoint interval must be positive, got: " + performance.getCheckpointInterval());
        } else if (performance.getCheckpointInterval() > 100) {
            errors.add("Checkpoint interval should not exceed 100 to maintain performance, got: " + performance.getCheckpointInterval());
        }
        
        // HIGH PRIORITY ISSUE #6: Validate new timeout settings
        validateTimeoutSettings(performance, errors);
    }
    
    private void validateStorageSettings(List<String> errors) {
        if (storage == null) {
            errors.add("Storage configuration is required");
            return;
        }
        
        // Base path validation
        if (storage.getBasePath() == null || storage.getBasePath().trim().isEmpty()) {
            errors.add("Storage base path cannot be empty");
        } else {
            String basePath = storage.getBasePath().trim();
            if (basePath.contains("..")) {
                errors.add("Storage base path cannot contain '..' for security reasons");
            }
            if (basePath.length() > 1000) {
                errors.add("Storage base path is too long (max 1000 chars), got: " + basePath.length());
            }
        }
        
        // Storage type validation
        if (storage.getType() == null) {
            errors.add("Storage type is required");
        } else {
            String type = storage.getType().toString().toUpperCase();
            if (!java.util.Set.of("S3A", "LOCAL", "HDFS", "AZURE", "GCS").contains(type)) {
                errors.add("Unsupported storage type: " + storage.getType() + 
                    ". Supported types: S3A, LOCAL, HDFS, AZURE, GCS");
            }
        }
        
        // Note: No endpoint validation needed as it's not in the current Storage class
        // If endpoint support is added later, validation can be uncommented
        // if (storage.getEndpoint() != null && !storage.getEndpoint().trim().isEmpty()) {
        //     String endpoint = storage.getEndpoint().trim();
        //     if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
        //         errors.add("Storage endpoint must start with http:// or https://, got: " + endpoint);
        //     }
        //     try {
        //         new java.net.URL(endpoint);
        //     } catch (java.net.MalformedURLException e) {
        //         errors.add("Invalid storage endpoint URL: " + endpoint);
        //     }
        // }
    }
    
    private void validateTableConfigurations(List<String> errors) {
        if (tables == null || tables.isEmpty()) {
            log.warn("No table configurations found - using runtime entity discovery");
            return;
        }
        
        for (Map.Entry<String, TableConfig> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            TableConfig config = entry.getValue();
            
            // Table name validation
            if (tableName == null || tableName.trim().isEmpty()) {
                errors.add("Table name cannot be empty");
                continue;
            }
            
            if (!tableName.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                errors.add("Invalid table name '" + tableName + "': must start with letter and contain only alphanumeric characters and underscores");
            }
            
            if (tableName.length() > 255) {
                errors.add("Table name '" + tableName + "' is too long (max 255 chars)");
            }
            
            if (config == null) {
                errors.add("Table configuration for '" + tableName + "' cannot be null");
                continue;
            }
            
            // Primary key validation
            if (config.getPrimaryKeyColumn() != null) {
                String primaryKey = config.getPrimaryKeyColumn().trim();
                if (primaryKey.isEmpty()) {
                    errors.add("Primary key column for table '" + tableName + "' cannot be empty");
                } else if (!primaryKey.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                    errors.add("Invalid primary key column '" + primaryKey + "' for table '" + tableName + "'");
                }
            }
            
            // Partition columns validation
            if (config.getPartitionColumns() != null) {
                List<String> partitionColumns = config.getPartitionColumns();
                if (partitionColumns.isEmpty()) {
                    errors.add("Partition columns list for table '" + tableName + "' should not be empty (use null instead)");
                } else {
                    for (String column : partitionColumns) {
                        if (column == null || column.trim().isEmpty()) {
                            errors.add("Partition column name cannot be empty for table '" + tableName + "'");
                        } else if (!column.matches("^[a-zA-Z][a-zA-Z0-9_]*$")) {
                            errors.add("Invalid partition column '" + column + "' for table '" + tableName + "'");
                        }
                    }
                    
                    // Check for duplicates
                    Set<String> uniqueColumns = new HashSet<>(partitionColumns);
                    if (uniqueColumns.size() != partitionColumns.size()) {
                        errors.add("Duplicate partition columns found for table '" + tableName + "'");
                    }
                    
                    // Check if too many partition columns
                    if (partitionColumns.size() > 10) {
                        errors.add("Too many partition columns for table '" + tableName + "' (max 10, got " + partitionColumns.size() + ")");
                    }
                }
            }
            
            // Evolution policy validation
            if (config.getEvolutionPolicy() == null) {
                log.warn("No evolution policy specified for table '{}', using default BACKWARD_COMPATIBLE", tableName);
            }
        }
    }
    
    private void validateSchemaSettings(List<String> errors) {
        if (schema == null) {
            errors.add("Schema configuration is required");
            return;
        }
        
        // Schema cache TTL validation
        if (schema.getSchemaCacheTtlMs() <= 0) {
            errors.add("Schema cache TTL must be positive, got: " + schema.getSchemaCacheTtlMs());
        } else if (schema.getSchemaCacheTtlMs() > 3600000) { // 1 hour max
            errors.add("Schema cache TTL should not exceed 1 hour, got: " + schema.getSchemaCacheTtlMs());
        }
        
        // Auto-registration validation is just a boolean, no validation needed
        log.debug("Schema auto-registration enabled: {}", schema.isAutoRegisterSchemas());
    }
    
    private void validateThreadPoolSizing(List<String> errors) {
        if (performance == null) {
            return; // Already validated in validatePerformanceSettings
        }
        
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        
        // Warn if commit threads exceed CPU cores
        if (performance.getCommitThreads() > availableProcessors) {
            log.warn("Commit threads ({}) exceeds available processors ({}). Consider reducing for better performance.",
                performance.getCommitThreads(), availableProcessors);
        }
        
        // Validate connection pool sizing relative to system capacity
        if (performance.getConnectionPoolSize() > availableProcessors * 50) {
            log.warn("Connection pool size ({}) is very large relative to available processors ({}). This may cause resource contention.",
                performance.getConnectionPoolSize(), availableProcessors);
        }
        
        // Warn about optimal Parquet file size
        if (performance.getOptimalParquetSizeMB() < 64) {
            log.warn("Optimal Parquet size ({}MB) is smaller than recommended minimum of 64MB. This may impact performance.",
                performance.getOptimalParquetSizeMB());
        } else if (performance.getOptimalParquetSizeMB() > 1024) {
            log.warn("Optimal Parquet size ({}MB) is larger than recommended maximum of 1024MB. This may impact memory usage.",
                performance.getOptimalParquetSizeMB());
        }
    }
    
    private void validateMonitoringSettings(List<String> errors) {
        // Future monitoring configuration validation can go here
        // For now, just log that monitoring validation is complete
        log.debug("Monitoring settings validation complete");
    }
    
    /**
     * Validates timeout settings (HIGH PRIORITY ISSUE #6)
     */
    private void validateTimeoutSettings(Performance performance, List<String> errors) {
        // Schema operation timeout validation
        if (performance.getSchemaOperationTimeoutMs() <= 0) {
            errors.add("Schema operation timeout must be positive, got: " + performance.getSchemaOperationTimeoutMs());
        } else if (performance.getSchemaOperationTimeoutMs() > 60000) { // 1 minute max
            errors.add("Schema operation timeout should not exceed 1 minute (60000ms), got: " + performance.getSchemaOperationTimeoutMs());
        }
        
        // Batch processing timeout validation
        if (performance.getBatchProcessingTimeoutMs() <= 0) {
            errors.add("Batch processing timeout must be positive, got: " + performance.getBatchProcessingTimeoutMs());
        } else if (performance.getBatchProcessingTimeoutMs() > 300000) { // 5 minutes max
            errors.add("Batch processing timeout should not exceed 5 minutes (300000ms), got: " + performance.getBatchProcessingTimeoutMs());
        }
        
        // Checkpoint timeout validation
        if (performance.getCheckpointTimeoutMs() <= 0) {
            errors.add("Checkpoint timeout must be positive, got: " + performance.getCheckpointTimeoutMs());
        } else if (performance.getCheckpointTimeoutMs() > 600000) { // 10 minutes max
            errors.add("Checkpoint timeout should not exceed 10 minutes (600000ms), got: " + performance.getCheckpointTimeoutMs());
        }
        
        // Background operation timeout validation
        if (performance.getBackgroundOperationTimeoutMs() <= 0) {
            errors.add("Background operation timeout must be positive, got: " + performance.getBackgroundOperationTimeoutMs());
        } else if (performance.getBackgroundOperationTimeoutMs() > 180000) { // 3 minutes max
            errors.add("Background operation timeout should not exceed 3 minutes (180000ms), got: " + performance.getBackgroundOperationTimeoutMs());
        }
        
        // Thread pool await timeout validation
        if (performance.getThreadPoolAwaitTimeoutMs() <= 0) {
            errors.add("Thread pool await timeout must be positive, got: " + performance.getThreadPoolAwaitTimeoutMs());
        } else if (performance.getThreadPoolAwaitTimeoutMs() > 30000) { // 30 seconds max
            errors.add("Thread pool await timeout should not exceed 30 seconds (30000ms), got: " + performance.getThreadPoolAwaitTimeoutMs());
        }
        
        log.debug("Timeout settings validation completed");
    }
    
    private int getTotalConfigurationSettings() {
        int count = 0;
        if (performance != null) count += 6; // batch threads, commit threads, etc.
        if (storage != null) count += 3; // type, base path, endpoint
        if (schema != null) count += 2; // cache TTL, auto-register
        if (tables != null) count += tables.size();
        return count;
    }
}