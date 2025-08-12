package com.example.deltastore.storage;

import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.schema.DeltaSchemaManager;
import com.example.deltastore.util.ResourceLeakTracker;
import com.example.deltastore.util.ThreadPoolMonitor;
import io.delta.kernel.types.StructType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.Mockito;

@ExtendWith(MockitoExtension.class)
class OptimizedDeltaTableManagerTest {

    @Mock
    private StorageProperties storageProperties;
    
    @Mock
    private DeltaStoreConfiguration config;
    
    @Mock
    private DeltaStoreConfiguration.Performance performance;
    
    @Mock
    private DeltaSchemaManager schemaManager;
    
    @Mock
    private DeltaStoragePathResolver pathResolver;
    
    private OptimizedDeltaTableManager manager;
    private Schema testSchema;
    private GenericRecord testRecord;

    @BeforeEach
    void setUp() {
        // Mock configuration chain - using lenient to avoid unnecessary stubbing errors
        lenient().when(config.getPerformance()).thenReturn(performance);
        lenient().when(performance.getCommitThreads()).thenReturn(2);
        lenient().when(performance.getBatchTimeoutMs()).thenReturn(1000L);
        lenient().when(performance.getConnectionPoolSize()).thenReturn(10);
        lenient().when(performance.getMaxBatchSize()).thenReturn(100);
        lenient().when(performance.getMaxRetries()).thenReturn(3);
        lenient().when(performance.getCheckpointInterval()).thenReturn(10);
        lenient().when(performance.getWriteTimeoutMs()).thenReturn(30000L);
        
        // Mock storage properties - using lenient to avoid unnecessary stubbing errors
        lenient().when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        lenient().when(storageProperties.getAccessKey()).thenReturn("minio");
        lenient().when(storageProperties.getSecretKey()).thenReturn("minio123");
        
        ResourceLeakTracker resourceTracker = Mockito.mock(ResourceLeakTracker.class);
        ThreadPoolMonitor threadPoolMonitor = Mockito.mock(ThreadPoolMonitor.class);
        manager = new OptimizedDeltaTableManager(storageProperties, config, schemaManager, pathResolver, resourceTracker, threadPoolMonitor);
        
        // Create test schema and record
        testSchema = Schema.parse("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}");
        testRecord = new GenericRecordBuilder(testSchema)
                .set("id", "test123")
                .set("value", 42)
                .build();
    }

    @Test
    void testConstructor() {
        assertNotNull(manager);
        
        // Verify executors are created
        ScheduledExecutorService batchExecutor = (ScheduledExecutorService) ReflectionTestUtils.getField(manager, "batchExecutor");
        ExecutorService commitExecutor = (ExecutorService) ReflectionTestUtils.getField(manager, "commitExecutor");
        
        assertNotNull(batchExecutor);
        assertNotNull(commitExecutor);
        
        verify(config, atLeastOnce()).getPerformance();
        verify(performance).getCommitThreads();
    }

    @Test
    void testCreateOptimizedHadoopConfig() throws Exception {
        // Test is done via constructor - verify the configuration was created
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(manager, "hadoopConf");
        assertNotNull(hadoopConf);
        
        // Test with S3 endpoint configuration
        assertEquals("http://localhost:9000", hadoopConf.get("fs.s3a.endpoint"));
        assertEquals("minio", hadoopConf.get("fs.s3a.access.key"));
        assertEquals("minio123", hadoopConf.get("fs.s3a.secret.key"));
        assertEquals("true", hadoopConf.get("fs.s3a.path.style.access"));
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", hadoopConf.get("fs.s3a.impl"));
        assertEquals("false", hadoopConf.get("fs.s3a.connection.ssl.enabled"));
        
        // Test optimized Parquet settings
        assertEquals("268435456", hadoopConf.get("parquet.block.size"));
        assertEquals("8388608", hadoopConf.get("parquet.page.size"));
        assertEquals("snappy", hadoopConf.get("parquet.compression"));
        
        // Test connection pool settings
        assertEquals("10", hadoopConf.get("fs.s3a.connection.maximum"));
        assertEquals("5", hadoopConf.get("fs.s3a.threads.max"));
        assertEquals("2", hadoopConf.get("fs.s3a.threads.core"));
        
        // Test upload optimization settings
        assertEquals("true", hadoopConf.get("fs.s3a.fast.upload"));
        assertEquals("bytebuffer", hadoopConf.get("fs.s3a.fast.upload.buffer"));
        assertEquals("16", hadoopConf.get("fs.s3a.fast.upload.active.blocks"));
        assertEquals("64M", hadoopConf.get("fs.s3a.multipart.size"));
    }

    @Test
    void testCreateOptimizedHadoopConfigWithoutS3() {
        // Test with no S3 endpoint
        when(storageProperties.getEndpoint()).thenReturn(null);
        
        ResourceLeakTracker localResourceTracker = Mockito.mock(ResourceLeakTracker.class);
        ThreadPoolMonitor localThreadPoolMonitor = Mockito.mock(ThreadPoolMonitor.class);
        OptimizedDeltaTableManager localManager = new OptimizedDeltaTableManager(
                storageProperties, config, schemaManager, pathResolver, localResourceTracker, localThreadPoolMonitor);
        
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(localManager, "hadoopConf");
        assertEquals("file:///", hadoopConf.get("fs.default.name"));
        assertEquals("file:///", hadoopConf.get("fs.defaultFS"));
    }

    @Test
    void testWriteWithNullTableName() {
        assertThrows(IllegalArgumentException.class, () -> 
            manager.write(null, Collections.singletonList(testRecord), testSchema)
        );
    }

    @Test
    void testWriteWithEmptyTableName() {
        assertThrows(IllegalArgumentException.class, () -> 
            manager.write("", Collections.singletonList(testRecord), testSchema)
        );
    }

    @Test
    void testWriteWithNullRecords() {
        // Should return without error
        assertDoesNotThrow(() -> manager.write("test", null, testSchema));
    }

    @Test
    void testWriteWithEmptyRecords() {
        // Should return without error
        assertDoesNotThrow(() -> manager.write("test", Collections.emptyList(), testSchema));
    }

    @Test
    void testCalculateOptimalBatchSize() throws Exception {
        // Test low queue load
        BlockingQueue<?> writeQueue = (BlockingQueue<?>) ReflectionTestUtils.getField(manager, "writeQueue");
        assertTrue(writeQueue.isEmpty());
        
        // Invoke private method via reflection
        Integer optimalSize = (Integer) ReflectionTestUtils.invokeMethod(manager, "calculateOptimalBatchSize");
        
        // With empty queue, should return half of max batch size (minimum 10)
        assertEquals(50, optimalSize); // maxBatchSize (100) / 2
    }

    @Test
    void testShouldCreateCheckpoint() throws Exception {
        // Test checkpoint logic via reflection
        Boolean shouldCreate = (Boolean) ReflectionTestUtils.invokeMethod(manager, "shouldCreateCheckpoint", 10L);
        assertTrue(shouldCreate); // version 10, interval 10 -> true
        
        Boolean shouldNotCreate = (Boolean) ReflectionTestUtils.invokeMethod(manager, "shouldCreateCheckpoint", 5L);
        assertFalse(shouldNotCreate); // version 5, interval 10 -> false
        
        Boolean shouldNotCreateZero = (Boolean) ReflectionTestUtils.invokeMethod(manager, "shouldCreateCheckpoint", 0L);
        assertFalse(shouldNotCreateZero); // version 0 -> false
    }

    @Test
    void testDoesTableExist() throws Exception {
        // Test via reflection since it's a private method
        Boolean existsTrue = (Boolean) ReflectionTestUtils.invokeMethod(manager, "doesTableExist", "/path/to/existing");
        Boolean existsFalse = (Boolean) ReflectionTestUtils.invokeMethod(manager, "doesTableExist", "/path/to/nonexisting");
        
        // Both should be false in test environment (no actual Delta tables)
        assertFalse(existsTrue);
        assertFalse(existsFalse);
    }

    @Test
    void testGetMetrics() {
        Map<String, Object> metrics = manager.getMetrics();
        
        assertNotNull(metrics);
        assertTrue(metrics.containsKey("writes"));
        assertTrue(metrics.containsKey("conflicts"));
        assertTrue(metrics.containsKey("queue_size"));
        assertTrue(metrics.containsKey("avg_write_latency_ms"));
        assertTrue(metrics.containsKey("checkpoints_created"));
        assertTrue(metrics.containsKey("batch_consolidations"));
        assertTrue(metrics.containsKey("optimal_batch_size"));
        assertTrue(metrics.containsKey("configured_batch_timeout_ms"));
        assertTrue(metrics.containsKey("configured_max_batch_size"));
        assertTrue(metrics.containsKey("configured_max_retries"));
        assertTrue(metrics.containsKey("configured_checkpoint_interval"));
        assertTrue(metrics.containsKey("s3a_optimizations_enabled"));
        assertTrue(metrics.containsKey("parquet_block_size_mb"));
        assertTrue(metrics.containsKey("connection_pool_size"));
        
        // Verify initial values
        assertEquals(0L, metrics.get("writes"));
        assertEquals(0L, metrics.get("conflicts"));
        assertEquals(0L, metrics.get("queue_size"));
        assertEquals(0L, metrics.get("avg_write_latency_ms"));
        assertEquals(0L, metrics.get("checkpoints_created"));
        assertEquals(0L, metrics.get("batch_consolidations"));
        assertEquals(1000L, metrics.get("configured_batch_timeout_ms"));
        assertEquals(100, metrics.get("configured_max_batch_size"));
        assertEquals(3, metrics.get("configured_max_retries"));
        assertEquals(10, metrics.get("configured_checkpoint_interval"));
        assertEquals(true, metrics.get("s3a_optimizations_enabled"));
        assertEquals(256, metrics.get("parquet_block_size_mb"));
        assertEquals(10, metrics.get("connection_pool_size"));
    }

    @Test
    void testGetMetricsWithSchemaManagerCacheStats() {
        Map<String, Object> cacheStats = Map.of("hits", 10L, "misses", 2L);
        when(schemaManager.getCacheStats()).thenReturn(cacheStats);
        
        Map<String, Object> metrics = manager.getMetrics();
        
        assertEquals(cacheStats, metrics.get("schema_cache_stats"));
        verify(schemaManager).getCacheStats();
    }

    @Test
    void testInit() {
        // init() is called in @PostConstruct, verify it was called during setup
        ScheduledExecutorService batchExecutor = (ScheduledExecutorService) ReflectionTestUtils.getField(manager, "batchExecutor");
        assertNotNull(batchExecutor);
        assertFalse(batchExecutor.isShutdown());
    }

    @Test
    void testCleanup() throws Exception {
        ScheduledExecutorService batchExecutor = (ScheduledExecutorService) ReflectionTestUtils.getField(manager, "batchExecutor");
        ExecutorService commitExecutor = (ExecutorService) ReflectionTestUtils.getField(manager, "commitExecutor");
        
        // Call cleanup
        ReflectionTestUtils.invokeMethod(manager, "cleanup");
        
        // Executors should be shutdown
        assertTrue(batchExecutor.isShutdown());
        assertTrue(commitExecutor.isShutdown());
    }

    @Test
    void testCreateCloseableIterator() throws Exception {
        List<String> testData = Arrays.asList("item1", "item2", "item3");
        Iterator<String> iterator = testData.iterator();
        
        // Test via reflection
        Object closeableIterator = ReflectionTestUtils.invokeMethod(manager, "createCloseableIterator", iterator);
        assertNotNull(closeableIterator);
        
        // Test the iterator functionality via reflection
        Boolean hasNext = (Boolean) ReflectionTestUtils.invokeMethod(closeableIterator, "hasNext");
        assertTrue(hasNext);
        
        String next = (String) ReflectionTestUtils.invokeMethod(closeableIterator, "next");
        assertEquals("item1", next);
        
        // Test close method doesn't throw
        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(closeableIterator, "close"));
    }

    @Test
    void testWriteBatchClass() throws Exception {
        List<GenericRecord> records = Arrays.asList(testRecord);
        
        // Create WriteBatch via reflection
        Class<?> writeBatchClass = Class.forName("com.example.deltastore.storage.OptimizedDeltaTableManager$WriteBatch");
        Object writeBatch = writeBatchClass.getDeclaredConstructor(String.class, List.class, Schema.class)
                .newInstance("test_table", records, testSchema);
        
        assertNotNull(writeBatch);
        assertEquals("test_table", ReflectionTestUtils.getField(writeBatch, "tableName"));
        assertEquals(records, ReflectionTestUtils.getField(writeBatch, "records"));
        assertEquals(testSchema, ReflectionTestUtils.getField(writeBatch, "schema"));
        assertNotNull(ReflectionTestUtils.getField(writeBatch, "future"));
    }

    @Test
    void testMetricsFieldsExist() {
        // Verify atomic fields exist and are initialized
        assertNotNull(ReflectionTestUtils.getField(manager, "writeCount"));
        assertNotNull(ReflectionTestUtils.getField(manager, "conflictCount"));
        assertNotNull(ReflectionTestUtils.getField(manager, "avgWriteLatency"));
        assertNotNull(ReflectionTestUtils.getField(manager, "checkpointCount"));
        assertNotNull(ReflectionTestUtils.getField(manager, "batchConsolidationCount"));
    }

    @Test
    void testProcessBatchesWithEmptyQueue() throws Exception {
        // Call processBatches when queue is empty
        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(manager, "processBatches"));
        
        // Queue should still be empty
        BlockingQueue<?> writeQueue = (BlockingQueue<?>) ReflectionTestUtils.getField(manager, "writeQueue");
        assertTrue(writeQueue.isEmpty());
    }

    @Test
    void testConfigurationDependencies() {
        verify(storageProperties, atLeastOnce()).getEndpoint();
        verify(storageProperties, atLeastOnce()).getAccessKey();
        verify(storageProperties, atLeastOnce()).getSecretKey();
        verify(performance, atLeastOnce()).getConnectionPoolSize();
        verify(performance, atLeastOnce()).getCommitThreads();
    }

    @Test
    void testOptimalBatchSizeWithHighLoad() throws Exception {
        // Mock high queue load by adding items to queue
        BlockingQueue<Object> writeQueue = (BlockingQueue<Object>) ReflectionTestUtils.getField(manager, "writeQueue");
        
        // Add more than 1000 items to trigger high load path
        for (int i = 0; i < 1001; i++) {
            writeQueue.offer(new Object());
        }
        
        Integer optimalSize = (Integer) ReflectionTestUtils.invokeMethod(manager, "calculateOptimalBatchSize");
        
        // With high load (>1000), should return double the max batch size (capped at 10000)
        assertEquals(200, optimalSize); // maxBatchSize (100) * 2
    }

    @Test
    void testOptimalBatchSizeWithModerateLoad() throws Exception {
        BlockingQueue<Object> writeQueue = (BlockingQueue<Object>) ReflectionTestUtils.getField(manager, "writeQueue");
        
        // Add moderate load (101-1000 items)
        for (int i = 0; i < 150; i++) {
            writeQueue.offer(new Object());
        }
        
        Integer optimalSize = (Integer) ReflectionTestUtils.invokeMethod(manager, "calculateOptimalBatchSize");
        
        // With moderate load, should return configured batch size
        assertEquals(100, optimalSize); // maxBatchSize (100)
    }

    @Test
    void testHadoopConfigurationOptimizations() {
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(manager, "hadoopConf");
        
        // Verify timeout settings
        assertEquals("2000", hadoopConf.get("fs.s3a.connection.establish.timeout"));
        assertEquals("60000", hadoopConf.get("fs.s3a.connection.timeout"));
        assertEquals("60000", hadoopConf.get("fs.s3a.socket.timeout"));
        assertEquals("60000", hadoopConf.get("fs.s3a.request.timeout"));
        
        // Verify retry settings
        assertEquals("5", hadoopConf.get("fs.s3a.attempts.maximum"));
        assertEquals("5", hadoopConf.get("fs.s3a.retry.limit"));
        assertEquals("200ms", hadoopConf.get("fs.s3a.retry.interval"));
        assertEquals("1.5", hadoopConf.get("fs.s3a.retry.exponential.base"));
        
        // Verify buffer and I/O settings
        assertEquals("1048576", hadoopConf.get("fs.s3a.readahead.range"));
        assertEquals("sequential", hadoopConf.get("fs.s3a.input.fadvise"));
        assertEquals("true", hadoopConf.get("fs.s3a.prefetch.enabled"));
        
        // Verify performance optimizations
        assertEquals("none", hadoopConf.get("fs.s3a.server.side.encryption.algorithm"));
        assertEquals("2", hadoopConf.get("fs.s3a.list.version"));
        assertEquals("delete", hadoopConf.get("fs.s3a.directory.marker.retention"));
    }
}