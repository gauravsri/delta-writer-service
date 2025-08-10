package com.example.deltastore.storage;

import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.schemas.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OptimizedDeltaTableManagerTest {

    @Mock
    private StorageProperties storageProperties;

    private OptimizedDeltaTableManager optimizedManager;

    @BeforeEach
    void setUp() {
        lenient().when(storageProperties.getEndpoint()).thenReturn("");
        lenient().when(storageProperties.getBucketName()).thenReturn("test-bucket");
        lenient().when(storageProperties.getAccessKey()).thenReturn("test-key");
        lenient().when(storageProperties.getSecretKey()).thenReturn("test-secret");
        optimizedManager = new OptimizedDeltaTableManager(storageProperties);
    }

    @Test
    void testConstructorWithValidProperties() {
        assertNotNull(optimizedManager);
        verify(storageProperties).getEndpoint();
        verify(storageProperties).getBucketName();
    }

    @Test
    void testGetMetricsInitialState() {
        Map<String, Long> metrics = optimizedManager.getMetrics();
        
        assertNotNull(metrics);
        assertEquals(0L, metrics.get("writes"));
        assertEquals(0L, metrics.get("reads"));
        assertEquals(0L, metrics.get("conflicts"));
        assertEquals(0L, metrics.get("cache_hits"));
        assertEquals(0L, metrics.get("cache_misses"));
        assertEquals(0L, metrics.get("queue_size"));
        assertEquals(0L, metrics.get("avg_write_latency_ms"));
        assertEquals(0L, metrics.get("cache_hit_rate_percent"));
    }

    @Test
    void testWriteWithNullTableName() {
        List<GenericRecord> records = createTestRecords();
        Schema schema = User.getClassSchema();
        
        assertThrows(IllegalArgumentException.class, () -> {
            optimizedManager.write(null, records, schema);
        });
    }

    @Test
    void testWriteWithEmptyTableName() {
        List<GenericRecord> records = createTestRecords();
        Schema schema = User.getClassSchema();
        
        assertThrows(IllegalArgumentException.class, () -> {
            optimizedManager.write("", records, schema);
        });
    }

    @Test
    void testWriteWithNullRecords() {
        Schema schema = User.getClassSchema();
        
        // Should not throw exception, just return gracefully
        assertDoesNotThrow(() -> {
            optimizedManager.write("test-table", null, schema);
        });
    }

    @Test
    void testWriteWithEmptyRecords() {
        List<GenericRecord> emptyRecords = new ArrayList<>();
        Schema schema = User.getClassSchema();
        
        // Should not throw exception, just return gracefully
        assertDoesNotThrow(() -> {
            optimizedManager.write("test-table", emptyRecords, schema);
        });
    }

    @Test
    void testReadWithNullParameters() {
        Optional<Map<String, Object>> result1 = optimizedManager.read(null, "user_id", "test");
        assertTrue(result1.isEmpty());

        Optional<Map<String, Object>> result2 = optimizedManager.read("test-table", null, "test");
        assertTrue(result2.isEmpty());

        Optional<Map<String, Object>> result3 = optimizedManager.read("test-table", "user_id", null);
        assertTrue(result3.isEmpty());
    }

    @Test
    void testReadIncreasesReadCount() {
        Map<String, Long> initialMetrics = optimizedManager.getMetrics();
        long initialReads = initialMetrics.get("reads");

        optimizedManager.read("test-table", "user_id", "test-value");

        Map<String, Long> updatedMetrics = optimizedManager.getMetrics();
        long finalReads = updatedMetrics.get("reads");

        assertEquals(initialReads + 1, finalReads);
    }

    @Test
    void testReadByPartitionsWithNullParameters() {
        List<Map<String, Object>> result1 = optimizedManager.readByPartitions(null, Map.of("key", "value"));
        assertTrue(result1.isEmpty());

        List<Map<String, Object>> result2 = optimizedManager.readByPartitions("test-table", null);
        assertTrue(result2.isEmpty());

        List<Map<String, Object>> result3 = optimizedManager.readByPartitions("test-table", Map.of());
        assertTrue(result3.isEmpty());
    }

    @Test
    void testGetTablePathWithEndpoint() {
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getBucketName()).thenReturn("my-bucket");
        
        OptimizedDeltaTableManager manager = new OptimizedDeltaTableManager(storageProperties);
        
        // Using reflection to test private method
        try {
            java.lang.reflect.Method getTablePathMethod = OptimizedDeltaTableManager.class.getDeclaredMethod("getTablePath", String.class);
            getTablePathMethod.setAccessible(true);
            
            String result = (String) getTablePathMethod.invoke(manager, "users");
            assertEquals("s3a://my-bucket/users", result);
        } catch (Exception e) {
            fail("Failed to test getTablePath method: " + e.getMessage());
        }
    }

    @Test
    void testGetTablePathWithoutEndpoint() {
        when(storageProperties.getEndpoint()).thenReturn("");
        when(storageProperties.getBucketName()).thenReturn("my-bucket");
        
        OptimizedDeltaTableManager manager = new OptimizedDeltaTableManager(storageProperties);
        
        // Using reflection to test private method
        try {
            java.lang.reflect.Method getTablePathMethod = OptimizedDeltaTableManager.class.getDeclaredMethod("getTablePath", String.class);
            getTablePathMethod.setAccessible(true);
            
            String result = (String) getTablePathMethod.invoke(manager, "users");
            assertEquals("/tmp/delta-tables/my-bucket/users", result);
        } catch (Exception e) {
            fail("Failed to test getTablePath method: " + e.getMessage());
        }
    }

    @Test
    void testCacheHitRateCalculationWithZeroOperations() {
        Map<String, Long> metrics = optimizedManager.getMetrics();
        assertEquals(0L, metrics.get("cache_hit_rate_percent"));
    }

    @Test
    void testInitAndCleanupMethods() {
        assertDoesNotThrow(() -> {
            optimizedManager.init();
        });

        assertDoesNotThrow(() -> {
            optimizedManager.cleanup();
        });
    }

    @Test
    void testMultipleReadOperationsIncreaseCounter() {
        Map<String, Long> initialMetrics = optimizedManager.getMetrics();
        long initialReads = initialMetrics.get("reads");

        optimizedManager.read("table1", "id", "value1");
        optimizedManager.read("table2", "id", "value2");
        optimizedManager.read("table3", "id", "value3");

        Map<String, Long> finalMetrics = optimizedManager.getMetrics();
        long finalReads = finalMetrics.get("reads");

        assertEquals(initialReads + 3, finalReads);
    }

    @Test
    void testReadByPartitionsLogsOperationRequest() {
        Map<String, String> filters = Map.of("country", "US", "status", "active");
        
        List<Map<String, Object>> result = optimizedManager.readByPartitions("users", filters);
        
        assertNotNull(result);
        assertTrue(result.isEmpty()); // Current implementation returns empty list
    }

    @Test
    void testStoragePropertiesIntegration() {
        when(storageProperties.getAccessKey()).thenReturn("test-access-key");
        when(storageProperties.getSecretKey()).thenReturn("test-secret-key");
        when(storageProperties.getEndpoint()).thenReturn("http://test-endpoint:9000");
        
        OptimizedDeltaTableManager manager = new OptimizedDeltaTableManager(storageProperties);
        
        verify(storageProperties, atLeastOnce()).getEndpoint();
        assertNotNull(manager);
    }

    private List<GenericRecord> createTestRecords() {
        List<GenericRecord> records = new ArrayList<>();
        
        User user1 = new User();
        user1.setUserId("test001");
        user1.setUsername("testuser1");
        user1.setEmail("test1@example.com");
        user1.setCountry("US");
        user1.setSignupDate("2024-08-10");
        records.add(user1);
        
        User user2 = new User();
        user2.setUserId("test002");
        user2.setUsername("testuser2");
        user2.setEmail("test2@example.com");
        user2.setCountry("CA");
        user2.setSignupDate("2024-08-10");
        records.add(user2);
        
        return records;
    }
}