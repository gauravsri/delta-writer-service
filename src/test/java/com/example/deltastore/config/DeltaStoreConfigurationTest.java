package com.example.deltastore.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeltaStoreConfigurationTest {

    private DeltaStoreConfiguration config;

    @BeforeEach
    void setUp() {
        config = new DeltaStoreConfiguration();
    }

    @Test
    void testDefaultConfigurationValues() {
        // Test performance defaults
        DeltaStoreConfiguration.Performance performance = config.getPerformance();
        assertEquals(30000L, performance.getCacheTtlMs());
        assertEquals(50L, performance.getBatchTimeoutMs());
        assertEquals(1000, performance.getMaxBatchSize());
        assertEquals(3, performance.getMaxRetries());
        assertEquals(200, performance.getConnectionPoolSize());
        assertEquals(30000L, performance.getWriteTimeoutMs());
        assertEquals(2, performance.getCommitThreads());
        
        // Test schema defaults
        DeltaStoreConfiguration.SchemaConfig schema = config.getSchema();
        assertEquals(DeltaStoreConfiguration.SchemaEvolutionPolicy.BACKWARD_COMPATIBLE, schema.getEvolutionPolicy());
        assertTrue(schema.isEnableSchemaValidation());
        assertTrue(schema.isAutoRegisterSchemas());
        assertTrue(schema.isCacheSchemas());
        assertEquals(300000L, schema.getSchemaCacheTtlMs());
        
        // Test storage defaults
        DeltaStoreConfiguration.Storage storage = config.getStorage();
        assertEquals(DeltaStoreConfiguration.StorageType.S3A, storage.getType());
        assertEquals("/delta-tables", storage.getBasePath());
        assertEquals(DeltaStoreConfiguration.PartitionStrategy.NONE, storage.getPartitionStrategy());
        assertTrue(storage.isEnableCompression());
        assertEquals("snappy", storage.getCompressionCodec());
        
        // Test empty tables map
        assertTrue(config.getTables().isEmpty());
    }

    @Test
    void testTableConfigurationSetup() {
        // Setup table configurations
        Map<String, DeltaStoreConfiguration.TableConfig> tables = new HashMap<>();
        
        DeltaStoreConfiguration.TableConfig userConfig = new DeltaStoreConfiguration.TableConfig();
        userConfig.setPrimaryKeyColumn("user_id");
        userConfig.setPartitionColumns(List.of("country", "signup_date"));
        userConfig.setEvolutionPolicy(DeltaStoreConfiguration.SchemaEvolutionPolicy.BACKWARD_COMPATIBLE);
        userConfig.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED);
        userConfig.setEnableOptimization(true);
        
        Map<String, String> userProperties = new HashMap<>();
        userProperties.put("delta.autoOptimize.optimizeWrite", "true");
        userConfig.setProperties(userProperties);
        
        tables.put("users", userConfig);
        config.setTables(tables);
        
        // Verify configuration
        DeltaStoreConfiguration.TableConfig retrievedConfig = config.getTables().get("users");
        assertNotNull(retrievedConfig);
        assertEquals("user_id", retrievedConfig.getPrimaryKeyColumn());
        assertEquals(2, retrievedConfig.getPartitionColumns().size());
        assertTrue(retrievedConfig.getPartitionColumns().contains("country"));
        assertTrue(retrievedConfig.getPartitionColumns().contains("signup_date"));
        assertEquals(DeltaStoreConfiguration.SchemaEvolutionPolicy.BACKWARD_COMPATIBLE, retrievedConfig.getEvolutionPolicy());
        assertEquals(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED, retrievedConfig.getPartitionStrategy());
        assertTrue(retrievedConfig.isEnableOptimization());
        assertEquals("true", retrievedConfig.getProperties().get("delta.autoOptimize.optimizeWrite"));
    }

    @Test
    void testGetTableConfigOrDefault() {
        // Test with existing table config
        Map<String, DeltaStoreConfiguration.TableConfig> tables = new HashMap<>();
        DeltaStoreConfiguration.TableConfig existingConfig = new DeltaStoreConfiguration.TableConfig();
        existingConfig.setPrimaryKeyColumn("existing_id");
        tables.put("existing_table", existingConfig);
        config.setTables(tables);
        
        DeltaStoreConfiguration.TableConfig result = config.getTableConfigOrDefault("existing_table");
        assertEquals("existing_id", result.getPrimaryKeyColumn());
        
        // Test with non-existing table (should return default)
        DeltaStoreConfiguration.TableConfig defaultResult = config.getTableConfigOrDefault("non_existing_table");
        assertNotNull(defaultResult);
        assertEquals(config.getSchema().getEvolutionPolicy(), defaultResult.getEvolutionPolicy());
        assertEquals(config.getStorage().getPartitionStrategy(), defaultResult.getPartitionStrategy());
    }

    @Test
    void testConfigurationValidation() {
        // Test valid configuration
        assertDoesNotThrow(() -> config.validateConfiguration());
        
        // Test invalid cache TTL
        config.getPerformance().setCacheTtlMs(-1);
        assertThrows(IllegalArgumentException.class, () -> config.validateConfiguration());
        
        // Reset and test invalid batch timeout
        config.getPerformance().setCacheTtlMs(30000);
        config.getPerformance().setBatchTimeoutMs(0);
        assertThrows(IllegalArgumentException.class, () -> config.validateConfiguration());
        
        // Reset and test invalid batch size
        config.getPerformance().setBatchTimeoutMs(50);
        config.getPerformance().setMaxBatchSize(-1);
        assertThrows(IllegalArgumentException.class, () -> config.validateConfiguration());
        
        // Reset and test invalid base path
        config.getPerformance().setMaxBatchSize(1000);
        config.getStorage().setBasePath("");
        assertThrows(IllegalArgumentException.class, () -> config.validateConfiguration());
    }

    @Test
    void testSchemaEvolutionPolicyEnum() {
        DeltaStoreConfiguration.SchemaEvolutionPolicy[] policies = DeltaStoreConfiguration.SchemaEvolutionPolicy.values();
        assertEquals(4, policies.length);
        assertTrue(List.of(policies).contains(DeltaStoreConfiguration.SchemaEvolutionPolicy.BACKWARD_COMPATIBLE));
        assertTrue(List.of(policies).contains(DeltaStoreConfiguration.SchemaEvolutionPolicy.FORWARD_COMPATIBLE));
        assertTrue(List.of(policies).contains(DeltaStoreConfiguration.SchemaEvolutionPolicy.FULL_COMPATIBLE));
        assertTrue(List.of(policies).contains(DeltaStoreConfiguration.SchemaEvolutionPolicy.NONE));
    }

    @Test
    void testStorageTypeEnum() {
        DeltaStoreConfiguration.StorageType[] types = DeltaStoreConfiguration.StorageType.values();
        assertEquals(5, types.length);
        assertTrue(List.of(types).contains(DeltaStoreConfiguration.StorageType.S3A));
        assertTrue(List.of(types).contains(DeltaStoreConfiguration.StorageType.LOCAL));
        assertTrue(List.of(types).contains(DeltaStoreConfiguration.StorageType.HDFS));
        assertTrue(List.of(types).contains(DeltaStoreConfiguration.StorageType.AZURE));
        assertTrue(List.of(types).contains(DeltaStoreConfiguration.StorageType.GCS));
    }

    @Test
    void testPartitionStrategyEnum() {
        DeltaStoreConfiguration.PartitionStrategy[] strategies = DeltaStoreConfiguration.PartitionStrategy.values();
        assertEquals(5, strategies.length);
        assertTrue(List.of(strategies).contains(DeltaStoreConfiguration.PartitionStrategy.NONE));
        assertTrue(List.of(strategies).contains(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED));
        assertTrue(List.of(strategies).contains(DeltaStoreConfiguration.PartitionStrategy.HASH_BASED));
        assertTrue(List.of(strategies).contains(DeltaStoreConfiguration.PartitionStrategy.RANGE_BASED));
        assertTrue(List.of(strategies).contains(DeltaStoreConfiguration.PartitionStrategy.CUSTOM));
    }

    @Test
    void testPerformanceConfigurationCustomization() {
        DeltaStoreConfiguration.Performance performance = config.getPerformance();
        
        performance.setCacheTtlMs(60000L);
        performance.setBatchTimeoutMs(100L);
        performance.setMaxBatchSize(200);
        performance.setMaxRetries(5);
        performance.setConnectionPoolSize(500);
        performance.setWriteTimeoutMs(60000L);
        performance.setCommitThreads(4);
        
        assertEquals(60000L, performance.getCacheTtlMs());
        assertEquals(100L, performance.getBatchTimeoutMs());
        assertEquals(200, performance.getMaxBatchSize());
        assertEquals(5, performance.getMaxRetries());
        assertEquals(500, performance.getConnectionPoolSize());
        assertEquals(60000L, performance.getWriteTimeoutMs());
        assertEquals(4, performance.getCommitThreads());
    }

    @Test
    void testSchemaConfigurationCustomization() {
        DeltaStoreConfiguration.SchemaConfig schema = config.getSchema();
        
        schema.setEvolutionPolicy(DeltaStoreConfiguration.SchemaEvolutionPolicy.FULL_COMPATIBLE);
        schema.setEnableSchemaValidation(false);
        schema.setAutoRegisterSchemas(false);
        schema.setCacheSchemas(false);
        schema.setSchemaCacheTtlMs(600000L);
        
        assertEquals(DeltaStoreConfiguration.SchemaEvolutionPolicy.FULL_COMPATIBLE, schema.getEvolutionPolicy());
        assertFalse(schema.isEnableSchemaValidation());
        assertFalse(schema.isAutoRegisterSchemas());
        assertFalse(schema.isCacheSchemas());
        assertEquals(600000L, schema.getSchemaCacheTtlMs());
    }

    @Test
    void testStorageConfigurationCustomization() {
        DeltaStoreConfiguration.Storage storage = config.getStorage();
        
        storage.setType(DeltaStoreConfiguration.StorageType.LOCAL);
        storage.setBasePath("/custom/path");
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.HASH_BASED);
        storage.setEnableCompression(false);
        storage.setCompressionCodec("gzip");
        
        assertEquals(DeltaStoreConfiguration.StorageType.LOCAL, storage.getType());
        assertEquals("/custom/path", storage.getBasePath());
        assertEquals(DeltaStoreConfiguration.PartitionStrategy.HASH_BASED, storage.getPartitionStrategy());
        assertFalse(storage.isEnableCompression());
        assertEquals("gzip", storage.getCompressionCodec());
    }
}