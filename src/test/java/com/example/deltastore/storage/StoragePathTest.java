package com.example.deltastore.storage;

import com.example.deltastore.config.DeltaStoreConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class StoragePathTest {

    @Test
    void testCompleteStoragePath() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/users")
            .partitionPath("country=US/signup_date=2024-08-10")
            .fullPath("s3a://bucket/delta-tables/users/country=US/signup_date=2024-08-10")
            .entityType("users")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        assertEquals("s3a://bucket/delta-tables/users", storagePath.getBasePath());
        assertEquals("country=US/signup_date=2024-08-10", storagePath.getPartitionPath());
        assertEquals("s3a://bucket/delta-tables/users/country=US/signup_date=2024-08-10", storagePath.getFullPath());
        assertEquals("users", storagePath.getEntityType());
        assertEquals(DeltaStoreConfiguration.StorageType.S3A, storagePath.getStorageType());
    }

    @Test
    void testStoragePathWithoutPartitions() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/products")
            .fullPath("s3a://bucket/delta-tables/products")
            .entityType("products")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        assertEquals("s3a://bucket/delta-tables/products", storagePath.getBasePath());
        assertNull(storagePath.getPartitionPath());
        assertEquals("s3a://bucket/delta-tables/products", storagePath.getFullPath());
        assertEquals("products", storagePath.getEntityType());
        assertFalse(storagePath.isPartitioned());
    }

    @Test
    void testGetTablePath() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("hdfs://namenode:9000/delta/users")
            .partitionPath("year=2024/month=08")
            .fullPath("hdfs://namenode:9000/delta/users/year=2024/month=08")
            .build();

        assertEquals("hdfs://namenode:9000/delta/users", storagePath.getTablePath());
    }

    @Test
    void testIsPartitionedTrue() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("/tmp/delta-tables/orders")
            .partitionPath("region=US/date=2024-08-10")
            .fullPath("/tmp/delta-tables/orders/region=US/date=2024-08-10")
            .build();

        assertTrue(storagePath.isPartitioned());
    }

    @Test
    void testIsPartitionedFalseWithNull() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("/tmp/delta-tables/orders")
            .partitionPath(null)
            .fullPath("/tmp/delta-tables/orders")
            .build();

        assertFalse(storagePath.isPartitioned());
    }

    @Test
    void testIsPartitionedFalseWithEmpty() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("/tmp/delta-tables/orders")
            .partitionPath("")
            .fullPath("/tmp/delta-tables/orders")
            .build();

        assertFalse(storagePath.isPartitioned());
    }

    @Test
    void testGetProtocolS3A() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("s3a://bucket/delta-tables/users")
            .build();

        assertEquals("s3a", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolHDFS() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("hdfs://namenode:9000/delta/users")
            .build();

        assertEquals("hdfs", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolFile() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("file:///tmp/delta-tables/users")
            .build();

        assertEquals("file", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolAbfs() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("abfs://container@account.dfs.core.windows.net/delta/users")
            .build();

        assertEquals("abfs", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolGcs() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("gs://bucket/delta-tables/users")
            .build();

        assertEquals("gs", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolLocalPath() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("/tmp/delta-tables/users")
            .build();

        assertEquals("file", storagePath.getProtocol()); // Default for local paths
    }

    @Test
    void testGetProtocolWithNullPath() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath(null)
            .build();

        assertEquals("unknown", storagePath.getProtocol());
    }

    @Test
    void testGetProtocolWithInvalidPath() {
        StoragePath storagePath = StoragePath.builder()
            .fullPath("invalid-path-without-protocol")
            .build();

        assertEquals("file", storagePath.getProtocol()); // Default fallback
    }

    @Test
    void testAllStorageTypes() {
        for (DeltaStoreConfiguration.StorageType storageType : DeltaStoreConfiguration.StorageType.values()) {
            StoragePath storagePath = StoragePath.builder()
                .basePath("/test/path")
                .entityType("test")
                .storageType(storageType)
                .build();

            assertEquals(storageType, storagePath.getStorageType());
        }
    }

    @Test
    void testMinimalStoragePath() {
        StoragePath storagePath = StoragePath.builder()
            .build();

        assertNull(storagePath.getBasePath());
        assertNull(storagePath.getPartitionPath());
        assertNull(storagePath.getFullPath());
        assertNull(storagePath.getEntityType());
        assertNull(storagePath.getStorageType());
        assertFalse(storagePath.isPartitioned());
        assertEquals("unknown", storagePath.getProtocol());
    }

    @Test
    void testComplexPartitionPath() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("s3a://data-lake/events")
            .partitionPath("year=2024/month=08/day=10/hour=14")
            .fullPath("s3a://data-lake/events/year=2024/month=08/day=10/hour=14")
            .entityType("events")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        assertTrue(storagePath.isPartitioned());
        assertEquals("year=2024/month=08/day=10/hour=14", storagePath.getPartitionPath());
        assertEquals("s3a", storagePath.getProtocol());
    }

    @Test
    void testToString() {
        StoragePath storagePath = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/users")
            .partitionPath("country=US")
            .fullPath("s3a://bucket/delta-tables/users/country=US")
            .entityType("users")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        String toString = storagePath.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("users"));
        assertTrue(toString.contains("s3a://bucket/delta-tables/users"));
        assertTrue(toString.contains("S3A"));
    }

    @Test
    void testEqualsAndHashCode() {
        StoragePath path1 = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/users")
            .partitionPath("country=US")
            .fullPath("s3a://bucket/delta-tables/users/country=US")
            .entityType("users")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        StoragePath path2 = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/users")
            .partitionPath("country=US")
            .fullPath("s3a://bucket/delta-tables/users/country=US")
            .entityType("users")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        StoragePath path3 = StoragePath.builder()
            .basePath("s3a://bucket/delta-tables/orders")
            .partitionPath("region=EU")
            .fullPath("s3a://bucket/delta-tables/orders/region=EU")
            .entityType("orders")
            .storageType(DeltaStoreConfiguration.StorageType.S3A)
            .build();

        assertEquals(path1, path2);
        assertEquals(path1.hashCode(), path2.hashCode());
        
        assertNotEquals(path1, path3);
        assertNotEquals(path1.hashCode(), path3.hashCode());
        
        assertNotEquals(path1, null);
        assertEquals(path1, path1);
    }
}