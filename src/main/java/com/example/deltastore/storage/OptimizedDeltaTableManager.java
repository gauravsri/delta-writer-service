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
            
            // OPTIMIZATION: Use configured connection pool settings
            int connectionPoolSize = config.getPerformance().getConnectionPoolSize();
            conf.set("fs.s3a.connection.maximum", String.valueOf(connectionPoolSize));
            conf.set("fs.s3a.threads.max", String.valueOf(connectionPoolSize / 4));
            conf.set("fs.s3a.threads.core", String.valueOf(connectionPoolSize / 10));
            
            // OPTIMIZATION: Faster upload settings
            conf.set("fs.s3a.fast.upload", "true");
            conf.set("fs.s3a.fast.upload.buffer", "bytebuffer");
            conf.set("fs.s3a.fast.upload.active.blocks", "8");
            conf.set("fs.s3a.multipart.size", "32M");
            conf.set("fs.s3a.multipart.threshold", "16M");
            
            // OPTIMIZATION: Connection timeouts
            conf.set("fs.s3a.connection.establish.timeout", "5000");
            conf.set("fs.s3a.connection.timeout", "200000");
            conf.set("fs.s3a.socket.timeout", "200000");
            
            // OPTIMIZATION: Request retry settings
            conf.set("fs.s3a.attempts.maximum", "10");
            conf.set("fs.s3a.retry.limit", "10");
            conf.set("fs.s3a.retry.interval", "500ms");
            
            // OPTIMIZATION: Metadata operations
            conf.set("fs.s3a.metadatastore.impl", "org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore");
            conf.set("fs.s3a.change.detection.mode", "none");
            
            conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            
            log.info("Configured optimized S3A settings for endpoint: {}", endpoint);
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
            
            // Drain queue and group by table using configured batch size
            int maxBatchSize = config.getPerformance().getMaxBatchSize();
            writeQueue.drainTo(currentBatches, maxBatchSize);
            
            if (currentBatches.isEmpty()) {
                return;
            }
            
            for (WriteBatch batch : currentBatches) {
                tableGroups.computeIfAbsent(batch.tableName, k -> new ArrayList<>()).add(batch);
            }
            
            // Process each table group in parallel
            for (Map.Entry<String, List<WriteBatch>> entry : tableGroups.entrySet()) {
                commitExecutor.submit(() -> processTableBatches(entry.getKey(), entry.getValue()));
            }
            
        } catch (Exception e) {
            log.error("Error processing batches", e);
        }
    }
    
    private void processTableBatches(String tableName, List<WriteBatch> batches) {
        try {
            // Aggregate all records from batches
            List<GenericRecord> allRecords = new ArrayList<>();
            Schema schema = batches.get(0).schema;
            
            for (WriteBatch batch : batches) {
                allRecords.addAll(batch.records);
            }
            
            log.info("Processing aggregated batch of {} records for table: {}", allRecords.size(), tableName);
            
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
    
    // Write-only metrics
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // Core write metrics
        metrics.put("writes", writeCount.get());
        metrics.put("conflicts", conflictCount.get());
        metrics.put("queue_size", (long) writeQueue.size());
        metrics.put("avg_write_latency_ms", avgWriteLatency.get());
            
        // Configuration metrics
        metrics.put("configured_batch_timeout_ms", config.getPerformance().getBatchTimeoutMs());
        metrics.put("configured_max_batch_size", config.getPerformance().getMaxBatchSize());
        metrics.put("configured_max_retries", config.getPerformance().getMaxRetries());
        
        // Schema manager metrics
        metrics.put("schema_cache_stats", schemaManager.getCacheStats());
        
        return metrics;
    }
}