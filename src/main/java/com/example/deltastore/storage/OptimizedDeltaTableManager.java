package com.example.deltastore.storage;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.schema.DeltaSchemaManager;
import com.example.deltastore.exception.TableWriteException;
import com.example.deltastore.util.DeltaKernelBatchOperations;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Write-only Delta Table Manager focused on high-performance write operations using Delta Kernel APIs.
 * All read functionality has been removed to simplify the codebase and focus on what works well.
 */
@Service
@Primary
@Slf4j
@Qualifier("optimized")
public class OptimizedDeltaTableManager implements DeltaTableManager {

    private final StorageProperties storageProperties;
    private final DeltaStoreConfiguration config;
    private final DeltaSchemaManager schemaManager;
    private final DeltaStoragePathResolver pathResolver;
    private final Configuration hadoopConf;
    private final Engine engine;
    
    // Write batching optimization
    private final BlockingQueue<WriteBatch> writeQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService batchExecutor;
    private final ExecutorService commitExecutor;
    
    // Write-only metrics
    private final AtomicLong writeCount = new AtomicLong();
    private final AtomicLong conflictCount = new AtomicLong();
    private final AtomicLong avgWriteLatency = new AtomicLong();
    private final AtomicLong checkpointCount = new AtomicLong();
    private final AtomicLong batchConsolidationCount = new AtomicLong();
    
    private static class WriteBatch {
        final String tableName;
        final List<GenericRecord> records;
        final Schema schema;
        final CompletableFuture<TransactionCommitResult> future;
        
        WriteBatch(String tableName, List<GenericRecord> records, Schema schema) {
            this.tableName = tableName;
            this.records = records;
            this.schema = schema;
            this.future = new CompletableFuture<>();
        }
    }
    
    public OptimizedDeltaTableManager(StorageProperties storageProperties,
                                      DeltaStoreConfiguration config,
                                      DeltaSchemaManager schemaManager,
                                      DeltaStoragePathResolver pathResolver) {
        this.storageProperties = storageProperties;
        this.config = config;
        this.schemaManager = schemaManager;
        this.pathResolver = pathResolver;
        this.hadoopConf = createOptimizedHadoopConfig();
        this.engine = DefaultEngine.create(hadoopConf);
        
        // Initialize executors based on configuration
        this.batchExecutor = Executors.newSingleThreadScheduledExecutor();
        this.commitExecutor = Executors.newFixedThreadPool(config.getPerformance().getCommitThreads());
    }
    
    @PostConstruct
    public void init() {
        // Start batch processor with configured timeout
        long batchTimeoutMs = config.getPerformance().getBatchTimeoutMs();
        batchExecutor.scheduleWithFixedDelay(this::processBatches, 0, batchTimeoutMs, TimeUnit.MILLISECONDS);
        log.info("Write-only Delta Table Manager initialized with batching ({}ms)", batchTimeoutMs);
    }
    
    @PreDestroy
    public void cleanup() {
        batchExecutor.shutdown();
        commitExecutor.shutdown();
        try {
            if (!batchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                batchExecutor.shutdownNow();
            }
            if (!commitExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                commitExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private Configuration createOptimizedHadoopConfig() {
        Configuration conf = new Configuration();
        String endpoint = storageProperties.getEndpoint();
        
        if (endpoint != null && !endpoint.isEmpty()) {
            // Basic S3A configuration
            conf.set("fs.s3a.endpoint", endpoint);
            conf.set("fs.s3a.access.key", storageProperties.getAccessKey());
            conf.set("fs.s3a.secret.key", storageProperties.getSecretKey());
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            conf.set("fs.s3a.connection.ssl.enabled", "false");
            
            // PERFORMANCE OPTIMIZATION: Optimized Parquet settings from knowledgebase
            conf.set("parquet.block.size", "268435456"); // 256MB - optimal for MinIO
            conf.set("parquet.page.size", "8388608");    // 8MB - reduces overhead
            conf.set("parquet.compression", "snappy");   // Fast compression for MinIO
            
            // PERFORMANCE OPTIMIZATION: Enhanced connection pool settings
            int connectionPoolSize = config.getPerformance().getConnectionPoolSize();
            conf.set("fs.s3a.connection.maximum", String.valueOf(connectionPoolSize));
            conf.set("fs.s3a.threads.max", String.valueOf(connectionPoolSize / 2)); // Increased from /4
            conf.set("fs.s3a.threads.core", String.valueOf(connectionPoolSize / 5)); // Increased from /10
            conf.set("fs.s3a.threads.keepalivetime", "60"); // Keep threads alive longer
            
            // PERFORMANCE OPTIMIZATION: Optimized upload settings for large data
            conf.set("fs.s3a.fast.upload", "true");
            conf.set("fs.s3a.fast.upload.buffer", "bytebuffer"); // Direct memory buffers
            conf.set("fs.s3a.fast.upload.active.blocks", "16"); // Increased from 8 for parallel uploads
            conf.set("fs.s3a.multipart.size", "64M");     // Increased from 32M for fewer parts
            conf.set("fs.s3a.multipart.threshold", "32M"); // Increased from 16M
            conf.set("fs.s3a.block.size", "268435456");   // Match Parquet block size
            
            // PERFORMANCE OPTIMIZATION: Aggressive caching for MinIO compatibility
            conf.set("fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore");
            conf.set("fs.s3a.change.detection.mode", "none"); // Skip change detection for performance
            conf.set("fs.s3a.etag.checksum.enabled", "false"); // Skip ETags for MinIO
            
            // PERFORMANCE OPTIMIZATION: Optimized timeouts for MinIO local network
            conf.set("fs.s3a.connection.establish.timeout", "2000");  // Reduced for local MinIO
            conf.set("fs.s3a.connection.timeout", "60000");          // Reduced but sufficient
            conf.set("fs.s3a.socket.timeout", "60000");             // Reduced but sufficient
            conf.set("fs.s3a.request.timeout", "60000");            // Request timeout
            
            // PERFORMANCE OPTIMIZATION: Retry settings optimized for MinIO
            conf.set("fs.s3a.attempts.maximum", "5");     // Reduced from 10
            conf.set("fs.s3a.retry.limit", "5");         // Reduced from 10  
            conf.set("fs.s3a.retry.interval", "200ms");  // Reduced from 500ms
            conf.set("fs.s3a.retry.exponential.base", "1.5"); // Moderate backoff
            
            // PERFORMANCE OPTIMIZATION: Buffer and I/O settings
            conf.set("fs.s3a.readahead.range", "1048576");    // 1MB readahead
            conf.set("fs.s3a.input.fadvise", "sequential");   // Sequential read hint
            conf.set("fs.s3a.prefetch.enabled", "true");      // Enable prefetching
            
            // PERFORMANCE OPTIMIZATION: Disable expensive features for MinIO
            conf.set("fs.s3a.server.side.encryption.algorithm", "none");
            conf.set("fs.s3a.list.version", "2");             // Use V2 list for better performance
            conf.set("fs.s3a.directory.marker.retention", "delete"); // Clean up markers
            
            conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            
            log.info("Configured highly optimized S3A settings for MinIO endpoint: {} (connection pool: {}, upload buffer: 64MB)", 
                    endpoint, connectionPoolSize);
        } else {
            conf.set("fs.default.name", "file:///");
            conf.set("fs.defaultFS", "file:///");
        }
        
        return conf;
    }
    
    @Override
    public void write(String tableName, List<GenericRecord> records, Schema schema) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (records == null || records.isEmpty()) {
            return;
        }
        
        // Add to write queue for batch processing
        WriteBatch batch = new WriteBatch(tableName, records, schema);
        writeQueue.offer(batch);
        
        // Wait for result with configured timeout
        try {
            long writeTimeoutMs = config.getPerformance().getWriteTimeoutMs();
            TransactionCommitResult result = batch.future.get(writeTimeoutMs, TimeUnit.MILLISECONDS);
            log.debug("Write completed at version: {}", result.getVersion());
        } catch (Exception e) {
            throw new TableWriteException("Failed to write to table: " + tableName, e);
        }
    }
    
    private void processBatches() {
        try {
            Map<String, List<WriteBatch>> tableGroups = new HashMap<>();
            List<WriteBatch> currentBatches = new ArrayList<>();
            
            // PERFORMANCE OPTIMIZATION: Use optimal batch size based on knowledgebase recommendations
            // Aim for batches that produce ~256MB Parquet files for best performance
            int optimalBatchSize = calculateOptimalBatchSize();
            writeQueue.drainTo(currentBatches, optimalBatchSize);
            
            if (currentBatches.isEmpty()) {
                return;
            }
            
            // Group by table for transaction consolidation
            for (WriteBatch batch : currentBatches) {
                tableGroups.computeIfAbsent(batch.tableName, k -> new ArrayList<>()).add(batch);
            }
            
            log.debug("Processing {} batches across {} tables with optimal batch size: {}", 
                    currentBatches.size(), tableGroups.size(), optimalBatchSize);
            
            // Process each table group in parallel
            for (Map.Entry<String, List<WriteBatch>> entry : tableGroups.entrySet()) {
                commitExecutor.submit(() -> processTableBatches(entry.getKey(), entry.getValue()));
            }
            
        } catch (Exception e) {
            log.error("Error processing batches", e);
        }
    }
    
    /**
     * Calculates optimal batch size based on schema complexity and knowledgebase recommendations.
     * Aims to produce Parquet files in the 256MB-1GB range for optimal performance.
     */
    private int calculateOptimalBatchSize() {
        int baseBatchSize = config.getPerformance().getMaxBatchSize();
        
        // OPTIMIZATION: Adjust batch size based on system load and queue depth
        int queueDepth = writeQueue.size();
        
        if (queueDepth > 1000) {
            // High load: increase batch size to reduce transaction overhead
            return Math.min(baseBatchSize * 2, 10000);
        } else if (queueDepth > 100) {
            // Moderate load: use configured batch size
            return baseBatchSize;
        } else {
            // Low load: smaller batches for lower latency
            return Math.max(baseBatchSize / 2, 10);
        }
    }
    
    private void processTableBatches(String tableName, List<WriteBatch> batches) {
        try {
            // PERFORMANCE OPTIMIZATION: Batch consolidation
            List<GenericRecord> allRecords = new ArrayList<>();
            Schema schema = batches.get(0).schema;
            
            for (WriteBatch batch : batches) {
                allRecords.addAll(batch.records);
            }
            
            // Track batch consolidation
            if (batches.size() > 1) {
                batchConsolidationCount.incrementAndGet();
                log.debug("Consolidated {} batches into single transaction for table: {}", batches.size(), tableName);
            }
            
            log.info("Processing aggregated batch of {} records for table: {} (consolidated from {} batches)", 
                    allRecords.size(), tableName, batches.size());
            
            // Write with configured retry logic
            int maxRetries = config.getPerformance().getMaxRetries();
            TransactionCommitResult result = writeWithRetry(tableName, allRecords, schema, maxRetries);
            
            // Complete all futures
            for (WriteBatch batch : batches) {
                batch.future.complete(result);
            }
            
            writeCount.addAndGet(allRecords.size());
            
        } catch (Exception e) {
            log.error("Failed to process batches for table: {}", tableName, e);
            for (WriteBatch batch : batches) {
                batch.future.completeExceptionally(e);
            }
        }
    }
    
    private TransactionCommitResult writeWithRetry(String tableName, List<GenericRecord> records, 
                                                   Schema schema, int maxRetries) {
        String tablePath = pathResolver.resolveBaseTablePath(tableName);
        Exception lastException = null;
        long startTime = System.currentTimeMillis();
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Check if table exists
                boolean isNewTable = !doesTableExist(tablePath);
                
                Table table = Table.forPath(engine, tablePath);
                
                // Build transaction with proper operation
                TransactionBuilder txnBuilder = table.createTransactionBuilder(
                    engine,
                    "Write-only Delta Writer v3.0",
                    isNewTable ? Operation.CREATE_TABLE : Operation.WRITE
                );
                
                if (isNewTable) {
                    StructType deltaSchema = schemaManager.getOrCreateDeltaSchema(schema);
                    txnBuilder = txnBuilder.withSchema(engine, deltaSchema);
                }
                
                Transaction txn = txnBuilder.build(engine);
                
                // Use optimized write method
                TransactionCommitResult result = writeOptimizedDeltaRecords(txn, records, schema);
                
                // PERFORMANCE OPTIMIZATION: Create checkpoint at regular intervals
                // This is critical to prevent full transaction log scans that cause 12+ second latencies
                try {
                    if (shouldCreateCheckpoint(result.getVersion())) {
                        table.checkpoint(engine, result.getVersion());
                        checkpointCount.incrementAndGet();
                        log.info("Created checkpoint at version {} for table {} to optimize future reads", 
                                result.getVersion(), tableName);
                    }
                } catch (Exception checkpointError) {
                    // Don't fail the write if checkpoint fails, but log it
                    log.warn("Failed to create checkpoint at version {} for table {}: {}", 
                            result.getVersion(), tableName, checkpointError.getMessage());
                }
                
                // Update latency metrics
                long latency = System.currentTimeMillis() - startTime;
                avgWriteLatency.set((avgWriteLatency.get() + latency) / 2);
                
                log.info("Successfully wrote {} records to {} at version {} (attempt {}, latency: {}ms)", 
                    records.size(), tableName, result.getVersion(), attempt, latency);
                
                return result;
                
            } catch (ConcurrentWriteException e) {
                lastException = e;
                conflictCount.incrementAndGet();
                log.warn("Concurrent write conflict on attempt {} for table {}, retrying...", attempt, tableName);
                
                // Exponential backoff
                try {
                    Thread.sleep((long) Math.pow(2, attempt) * 100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new TableWriteException("Interrupted during retry", ie);
                }
                
            } catch (Exception e) {
                lastException = e;
                log.error("Unexpected error on attempt {} for table {}", attempt, tableName, e);
                break;
            }
        }
        
        throw new TableWriteException("Failed to write after " + maxRetries + " attempts", lastException);
    }
    
    /**
     * Determines if a checkpoint should be created based on configurable intervals.
     * This prevents transaction log from growing too large and causing performance issues.
     */
    private boolean shouldCreateCheckpoint(long version) {
        // Create checkpoint every N versions (configurable, default 10)
        long checkpointInterval = config.getPerformance().getCheckpointInterval();
        return version % checkpointInterval == 0 && version > 0;
    }
    
    private boolean doesTableExist(String tablePath) {
        try {
            Table table = Table.forPath(engine, tablePath);
            table.getLatestSnapshot(engine);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    private TransactionCommitResult writeOptimizedDeltaRecords(Transaction txn, 
                                                               List<GenericRecord> records, 
                                                               Schema schema) throws Exception {
        Row txnState = txn.getTransactionState(engine);
        
        log.debug("Writing {} records with optimized Delta protocol", records.size());
        
        // Get schema using modular schema manager
        StructType deltaSchema = schemaManager.getOrCreateDeltaSchema(schema);
        
        // Create batch from records
        FilteredColumnarBatch recordBatch = DeltaKernelBatchOperations.createBatchFromAvroRecords(records, deltaSchema);
        CloseableIterator<FilteredColumnarBatch> data = createCloseableIterator(
            Collections.singletonList(recordBatch).iterator());
        
        // Empty partition values for non-partitioned table
        Map<String, io.delta.kernel.expressions.Literal> partitionValues = new HashMap<>();
        
        // Transform and write
        CloseableIterator<FilteredColumnarBatch> physicalData = 
            Transaction.transformLogicalData(engine, txnState, data, partitionValues);
        
        DataWriteContext writeContext = 
            Transaction.getWriteContext(engine, txnState, partitionValues);
        
        CloseableIterator<io.delta.kernel.utils.DataFileStatus> dataFiles = 
            engine.getParquetHandler().writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns()
            );
        
        CloseableIterator<Row> dataActions = 
            Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
        
        List<Row> actionsList = new ArrayList<>();
        while (dataActions.hasNext()) {
            actionsList.add(dataActions.next());
        }
        dataActions.close();
        
        io.delta.kernel.utils.CloseableIterable<Row> dataActionsIterable = 
            io.delta.kernel.utils.CloseableIterable.inMemoryIterable(
                createCloseableIterator(actionsList.iterator()));
        
        return txn.commit(engine, dataActionsIterable);
    }
    
    private <T> CloseableIterator<T> createCloseableIterator(Iterator<T> iterator) {
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public T next() {
                return iterator.next();
            }
            
            @Override
            public void close() throws IOException {
                // Nothing to close
            }
        };
    }
    
    // Write-only metrics with performance optimizations tracking
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Core write metrics
        metrics.put("writes", writeCount.get());
        metrics.put("conflicts", conflictCount.get());
        metrics.put("queue_size", (long) writeQueue.size());
        metrics.put("avg_write_latency_ms", avgWriteLatency.get());
        
        // Performance optimization metrics
        metrics.put("checkpoints_created", checkpointCount.get());
        metrics.put("batch_consolidations", batchConsolidationCount.get());
        metrics.put("optimal_batch_size", calculateOptimalBatchSize());
            
        // Configuration metrics
        metrics.put("configured_batch_timeout_ms", config.getPerformance().getBatchTimeoutMs());
        metrics.put("configured_max_batch_size", config.getPerformance().getMaxBatchSize());
        metrics.put("configured_max_retries", config.getPerformance().getMaxRetries());
        metrics.put("configured_checkpoint_interval", config.getPerformance().getCheckpointInterval());
        
        // S3A optimization status
        metrics.put("s3a_optimizations_enabled", true);
        metrics.put("parquet_block_size_mb", 256);
        metrics.put("connection_pool_size", config.getPerformance().getConnectionPoolSize());
        
        // Schema manager metrics
        metrics.put("schema_cache_stats", schemaManager.getCacheStats());
        
        return metrics;
    }
}