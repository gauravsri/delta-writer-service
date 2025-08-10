package com.example.deltastore.storage;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.config.StorageProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeltaStoragePathResolverTest {

    @Mock
    private DeltaStoreConfiguration config;
    
    @Mock
    private StorageProperties storageProperties;

    private DeltaStoragePathResolver pathResolver;

    @BeforeEach
    void setUp() {
        pathResolver = new DeltaStoragePathResolver(config, storageProperties);
        
        // Setup default configuration
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setType(DeltaStoreConfiguration.StorageType.S3A);
        storage.setBasePath("/delta-tables");
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.NONE);
        lenient().when(config.getStorage()).thenReturn(storage);
        
        lenient().when(storageProperties.getBucketName()).thenReturn("test-bucket");
        
        DeltaStoreConfiguration.TableConfig defaultTableConfig = new DeltaStoreConfiguration.TableConfig();
        lenient().when(config.getTableConfigOrDefault(anyString())).thenReturn(defaultTableConfig);
    }

    @Test
    void testResolveS3ABasePath() {
        String result = pathResolver.resolveBaseTablePath("users");
        
        assertEquals("s3a://test-bucket/delta-tables/users", result);
    }

    @Test
    void testResolveLocalBasePath() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setType(DeltaStoreConfiguration.StorageType.LOCAL);
        storage.setBasePath("/tmp/delta-tables");
        when(config.getStorage()).thenReturn(storage);
        
        String result = pathResolver.resolveBaseTablePath("users");
        
        assertEquals("file:///tmp/delta-tables/users", result);
    }

    @Test
    void testResolveHdfsBasePath() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setType(DeltaStoreConfiguration.StorageType.HDFS);
        storage.setBasePath("/delta-tables");
        when(config.getStorage()).thenReturn(storage);
        
        String result = pathResolver.resolveBaseTablePath("users");
        
        assertEquals("hdfs://namenode:8020/delta-tables/users", result);
    }

    @Test
    void testResolveTablePathWithNoPartitions() {
        Map<String, Object> emptyPartitions = Map.of();
        
        StoragePath result = pathResolver.resolveTablePath("users", emptyPartitions);
        
        assertEquals("s3a://test-bucket/delta-tables/users", result.getBasePath());
        assertEquals("", result.getPartitionPath());
        assertEquals("s3a://test-bucket/delta-tables/users", result.getFullPath());
        assertEquals("users", result.getEntityType());
        assertEquals(DeltaStoreConfiguration.StorageType.S3A, result.getStorageType());
        assertFalse(result.isPartitioned());
    }

    @Test
    void testResolveTablePathWithDateBasedPartitions() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setType(DeltaStoreConfiguration.StorageType.S3A);
        storage.setBasePath("/delta-tables");
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED);
        when(config.getStorage()).thenReturn(storage);
        
        DeltaStoreConfiguration.TableConfig tableConfig = new DeltaStoreConfiguration.TableConfig();
        tableConfig.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED);
        when(config.getTableConfigOrDefault("users")).thenReturn(tableConfig);
        
        Map<String, Object> partitionValues = Map.of("signup_date", "2024-08-10");
        
        StoragePath result = pathResolver.resolveTablePath("users", partitionValues);
        
        assertEquals("s3a://test-bucket/delta-tables/users", result.getBasePath());
        assertEquals("/year=2024/month=08/day=10", result.getPartitionPath());
        assertEquals("s3a://test-bucket/delta-tables/users/year=2024/month=08/day=10", result.getFullPath());
        assertTrue(result.isPartitioned());
    }

    @Test
    void testResolveTablePathWithHashBasedPartitions() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.HASH_BASED);
        when(config.getStorage()).thenReturn(storage);
        
        DeltaStoreConfiguration.TableConfig tableConfig = new DeltaStoreConfiguration.TableConfig();
        tableConfig.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.HASH_BASED);
        when(config.getTableConfigOrDefault("products")).thenReturn(tableConfig);
        
        Map<String, Object> partitionValues = Map.of("category", "electronics");
        
        StoragePath result = pathResolver.resolveTablePath("products", partitionValues);
        
        assertTrue(result.isPartitioned());
        assertTrue(result.getPartitionPath().startsWith("/partition="));
        assertTrue(result.getPartitionPath().matches("/partition=\\d{2}"));
    }

    @Test
    void testResolveTablePathWithRangeBasedPartitions() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.RANGE_BASED);
        when(config.getStorage()).thenReturn(storage);
        
        DeltaStoreConfiguration.TableConfig tableConfig = new DeltaStoreConfiguration.TableConfig();
        tableConfig.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.RANGE_BASED);
        when(config.getTableConfigOrDefault("orders")).thenReturn(tableConfig);
        
        Map<String, Object> partitionValues = Map.of("order_amount", 5000);
        
        StoragePath result = pathResolver.resolveTablePath("orders", partitionValues);
        
        assertTrue(result.isPartitioned());
        assertEquals("/range=1K-10K", result.getPartitionPath());
    }

    @Test
    void testUnsupportedStorageType() {
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setType(DeltaStoreConfiguration.StorageType.GCS); // Not implemented in resolver
        when(config.getStorage()).thenReturn(storage);
        
        StoragePath result = pathResolver.resolveTablePath("test", Map.of());
        
        // Should handle GCS gracefully
        assertTrue(result.getBasePath().startsWith("gs://"));
    }

    @Test
    void testStoragePathGetProtocol() {
        StoragePath s3Path = StoragePath.builder()
            .fullPath("s3a://bucket/path/table")
            .build();
        
        assertEquals("s3a", s3Path.getProtocol());
        
        StoragePath localPath = StoragePath.builder()
            .fullPath("file:///tmp/path/table")
            .build();
        
        assertEquals("file", localPath.getProtocol());
        
        StoragePath pathWithoutProtocol = StoragePath.builder()
            .fullPath("/tmp/path/table")
            .build();
        
        assertEquals("file", pathWithoutProtocol.getProtocol()); // Default
    }

    @Test
    void testTableConfigSpecificPartitionStrategy() {
        // Global config uses NONE
        DeltaStoreConfiguration.Storage storage = new DeltaStoreConfiguration.Storage();
        storage.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.NONE);
        when(config.getStorage()).thenReturn(storage);
        
        // But table config overrides with DATE_BASED
        DeltaStoreConfiguration.TableConfig tableConfig = new DeltaStoreConfiguration.TableConfig();
        tableConfig.setPartitionStrategy(DeltaStoreConfiguration.PartitionStrategy.DATE_BASED);
        when(config.getTableConfigOrDefault("users")).thenReturn(tableConfig);
        
        Map<String, Object> partitionValues = Map.of("created_date", "2024-08-10");
        
        StoragePath result = pathResolver.resolveTablePath("users", partitionValues);
        
        // Should use table-specific DATE_BASED strategy, not global NONE
        assertTrue(result.isPartitioned());
        assertEquals("/year=2024/month=08/day=10", result.getPartitionPath());
    }

    @Test
    void testMissingBucketNameThrowsException() {
        when(storageProperties.getBucketName()).thenReturn(null);
        
        assertThrows(IllegalArgumentException.class, () -> {
            pathResolver.resolveBaseTablePath("users");
        });
    }
}