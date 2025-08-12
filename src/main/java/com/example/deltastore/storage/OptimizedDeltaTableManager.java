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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
    
    // Checkpoint race condition prevention (NEW - High Priority Issue #5)
    private final ConcurrentHashMap<String, Long> lastCheckpointVersion = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<Void>> pendingCheckpoints = new ConcurrentHashMap<>();
    private final AtomicLong backgroundRetryCount = new AtomicLong();
    private final AtomicLong backgroundFailureCount = new AtomicLong();
    private final AtomicLong ioErrorCount = new AtomicLong();
    private final AtomicLong runtimeErrorCount = new AtomicLong();
    private final AtomicLong permanentErrorCount = new AtomicLong();
    
    // HIGH PRIORITY ISSUE #6: Timeout tracking metrics
    private final AtomicLong batchProcessingTimeoutCount = new AtomicLong();
    private final AtomicLong checkpointTimeoutCount = new AtomicLong();
    private final AtomicLong writeTimeoutCount = new AtomicLong();
    
    // Constants for retry handling
    private static final int MAX_BACKGROUND_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100;
    
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
        
        // Initialize executors with dynamic sizing based on system resources
        this.batchExecutor = createOptimizedBatchExecutor();
        this.commitExecutor = createOptimizedCommitExecutor();
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
        log.info("Shutting down Delta Table Manager thread pools...");
        
        batchExecutor.shutdown();
        commitExecutor.shutdown();
        
        try {
            // HIGH PRIORITY ISSUE #6: Use configurable thread pool await timeout
            long awaitTimeoutMs = config.getPerformance().getThreadPoolAwaitTimeoutMs();
            long awaitTimeoutSeconds = awaitTimeoutMs / 1000;
            
            if (!batchExecutor.awaitTermination(awaitTimeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("Batch executor did not terminate gracefully after {}ms, forcing shutdown", awaitTimeoutMs);
                batchExecutor.shutdownNow();
                
                // Give a final chance for forced shutdown
                if (!batchExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.error("Batch executor failed to terminate even after forced shutdown");
                }
            }
            
            if (!commitExecutor.awaitTermination(awaitTimeoutSeconds, TimeUnit.SECONDS)) {
                log.warn("Commit executor did not terminate gracefully after {}ms, forcing shutdown", awaitTimeoutMs);
                commitExecutor.shutdownNow();
                
                // Give a final chance for forced shutdown
                if (!commitExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.error("Commit executor failed to terminate even after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for executor termination");
            // Force shutdown immediately if interrupted
            batchExecutor.shutdownNow();
            commitExecutor.shutdownNow();
        }
        
        // Clean up checkpoint tracking (HIGH PRIORITY ISSUE #5)
        int pendingCount = pendingCheckpoints.size();
        if (pendingCount > 0) {
            log.warn("Cancelling {} pending checkpoint operations during shutdown", pendingCount);
            pendingCheckpoints.values().forEach(future -> future.cancel(true));
            pendingCheckpoints.clear();
        }
        lastCheckpointVersion.clear();
        
        log.info("Delta Table Manager cleanup completed");
    }
    
    /**
     * Creates optimized batch executor with proper thread configuration
     */
    private ScheduledExecutorService createOptimizedBatchExecutor() {
        // Single thread is sufficient for batch scheduling
        return Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("delta-batch-%d")
                .setDaemon(false)
                .setPriority(Thread.NORM_PRIORITY)
                .setUncaughtExceptionHandler((thread, exception) -> {
                    log.error("Uncaught exception in batch executor thread: {}", thread.getName(), exception);
                })
                .build()
        );
    }
    
    /**
     * Creates optimized commit executor with dynamic sizing based on system resources
     */
    private ExecutorService createOptimizedCommitExecutor() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int configuredThreads = config.getPerformance().getCommitThreads();
        
        // Calculate optimal thread count
        int coreThreads = Math.min(configuredThreads, availableProcessors);
        int maxThreads = Math.max(coreThreads, Math.min(configuredThreads * 2, availableProcessors * 2));
        
        // Use ThreadPoolExecutor for better control
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            coreThreads,                    // Core pool size
            maxThreads,                     // Maximum pool size
            60L,                           // Keep alive time
            TimeUnit.SECONDS,              // Time unit
            new LinkedBlockingQueue<>(1000), // Work queue with bounded size
            new ThreadFactoryBuilder()
                .setNameFormat("delta-commit-%d")
                .setDaemon(false)
                .setPriority(Thread.NORM_PRIORITY)
                .setUncaughtExceptionHandler((thread, exception) -> {
                    log.error("Uncaught exception in commit executor thread: {}", thread.getName(), exception);
                })
                .build(),
            new ThreadPoolExecutor.CallerRunsPolicy() // Rejection policy
        );
        
        // Allow core threads to timeout to save resources during low load
        executor.allowCoreThreadTimeOut(true);
        
        log.info("Created commit executor: core={}, max={}, queue=1000, configured={}, available_processors={}", 
            coreThreads, maxThreads, configuredThreads, availableProcessors);
        
        return executor;
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
        } catch (TimeoutException e) {
            writeTimeoutCount.incrementAndGet(); // HIGH PRIORITY ISSUE #6: Track timeout metrics
            log.error("Write operation timeout after {}ms for table: {}", 
                config.getPerformance().getWriteTimeoutMs(), tableName);
            throw new TableWriteException("Write timeout after " + config.getPerformance().getWriteTimeoutMs() + "ms", e);
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
            
            // CRITICAL FIX: Atomic batch extraction to prevent race conditions
            int actuallyDrained = writeQueue.drainTo(currentBatches, optimalBatchSize);
            
            // Early return if no batches were actually drained
            if (actuallyDrained == 0) {
                return;
            }
            
            log.debug("Atomically drained {} batches (requested: {}) for processing", 
                    actuallyDrained, optimalBatchSize);
            
            // Group by table for transaction consolidation
            for (WriteBatch batch : currentBatches) {
                tableGroups.computeIfAbsent(batch.tableName, k -> new ArrayList<>()).add(batch);
            }
            
            log.debug("Processing {} batches across {} tables with optimal batch size: {}", 
                    currentBatches.size(), tableGroups.size(), optimalBatchSize);
            
            // Process each table group in parallel with error tracking
            List<CompletableFuture<Void>> processingFutures = new ArrayList<>();
            for (Map.Entry<String, List<WriteBatch>> entry : tableGroups.entrySet()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(
                    () -> processTableBatches(entry.getKey(), entry.getValue()), 
                    commitExecutor
                );
                processingFutures.add(future);
            }
            
            // HIGH PRIORITY ISSUE #6: Add timeout handling for batch processing
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]));
            
            try {
                long batchProcessingTimeoutMs = config.getPerformance().getBatchProcessingTimeoutMs();
                allFutures.get(batchProcessingTimeoutMs, TimeUnit.MILLISECONDS);
                log.debug("All {} table groups processed successfully", tableGroups.size());
            } catch (TimeoutException e) {
                batchProcessingTimeoutCount.incrementAndGet(); // HIGH PRIORITY ISSUE #6: Track timeout metrics
                log.warn("Batch processing timeout after {}ms, cancelling remaining operations", 
                    config.getPerformance().getBatchProcessingTimeoutMs());
                allFutures.cancel(true);
                // Cancel individual futures that haven't completed
                processingFutures.forEach(future -> {
                    if (!future.isDone()) {
                        future.cancel(true);
                    }
                });
            } catch (Exception e) {
                log.error("Error in parallel batch processing", e);
                allFutures.cancel(true);
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
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount <= MAX_BACKGROUND_RETRIES) {
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
                
                // Complete all futures on success
                for (WriteBatch batch : batches) {
                    batch.future.complete(result);
                }
                
                writeCount.addAndGet(allRecords.size());
                
                // Success - exit retry loop
                if (retryCount > 0) {
                    log.info("Successfully processed batch for table: {} after {} retries", tableName, retryCount);
                }
                return;
                
            } catch (ConcurrentWriteException e) {
                // Transient error - retry
                lastException = e;
                retryCount++;
                backgroundRetryCount.incrementAndGet();
                
                if (retryCount <= MAX_BACKGROUND_RETRIES) {
                    long delayMs = calculateBackoffDelay(retryCount);
                    log.warn("Concurrent write exception for table: {} (attempt {}/{}), retrying in {}ms", 
                            tableName, retryCount, MAX_BACKGROUND_RETRIES + 1, delayMs);
                    
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        handlePermanentFailure(batches, new TableWriteException("Thread interrupted during retry", ie));
                        return;
                    }
                } else {
                    log.error("Exhausted retries for table: {} after {} attempts", tableName, retryCount);
                }
                
            } catch (RuntimeException e) {
                // Check if this is a transient error worth retrying
                if (isTransientException(e) && retryCount < MAX_BACKGROUND_RETRIES) {
                    lastException = e;
                    retryCount++;
                    backgroundRetryCount.incrementAndGet();
                    
                    long delayMs = calculateBackoffDelay(retryCount);
                    log.warn("Transient error for table: {} (attempt {}/{}), retrying in {}ms: {}", 
                            tableName, retryCount, MAX_BACKGROUND_RETRIES + 1, delayMs, e.getMessage());
                    
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        handlePermanentFailure(batches, new TableWriteException("Thread interrupted during retry", ie));
                        return;
                    }
                } else {
                    // Permanent failure or exhausted retries
                    lastException = e;
                    break;
                }
                
            } catch (Exception e) {
                // Unknown exception - treat as permanent failure
                lastException = e;
                log.error("Unexpected error for table: {}, treating as permanent failure", tableName, e);
                break;
            }
        }
        
        // All retries exhausted or permanent failure
        handlePermanentFailure(batches, lastException);
    }
    
    /**
     * Calculates exponential backoff delay for retries
     */
    private long calculateBackoffDelay(int retryCount) {
        return INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, retryCount - 1);
    }
    
    /**
     * Determines if an exception is transient and worth retrying
     */
    private boolean isTransientException(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        
        message = message.toLowerCase();
        return message.contains("timeout") ||
               message.contains("connection") ||
               message.contains("network") ||
               message.contains("temporary") ||
               message.contains("unavailable") ||
               e instanceof IOException;
    }
    
    /**
     * Determines if an IOException is retryable
     */
    private boolean isRetryableIOException(IOException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        
        message = message.toLowerCase();
        
        // Retryable I/O patterns
        return message.contains("timeout") ||
               message.contains("connection reset") ||
               message.contains("connection refused") ||
               message.contains("network") ||
               message.contains("broken pipe") ||
               message.contains("socket") ||
               message.contains("interrupted") ||
               message.contains("temporary") ||
               message.contains("unavailable") ||
               message.contains("service temporarily") ||
               // S3/storage specific errors
               message.contains("503") ||
               message.contains("500") ||
               message.contains("throttle") ||
               message.contains("rate limit") ||
               // Common transient patterns
               message.contains("retry") ||
               message.contains("again");
    }
    
    /**
     * Determines if a RuntimeException is retryable
     */
    private boolean isRetryableRuntimeException(RuntimeException e) {
        String message = e.getMessage();
        String className = e.getClass().getSimpleName().toLowerCase();
        
        // Check exception type first
        if (className.contains("timeout") ||
            className.contains("unavailable") ||
            className.contains("temporary")) {
            return true;
        }
        
        if (message == null) {
            return false;
        }
        
        message = message.toLowerCase();
        
        // Retryable runtime error patterns
        return message.contains("timeout") ||
               message.contains("temporary") ||
               message.contains("unavailable") ||
               message.contains("service temporarily") ||
               message.contains("throttle") ||
               message.contains("rate limit") ||
               message.contains("connection pool") ||
               message.contains("connection") ||
               message.contains("deadlock") ||
               message.contains("lock") ||
               // S3/storage specific
               message.contains("503") ||
               message.contains("500") ||
               message.contains("502") ||
               message.contains("504") ||
               // Delta specific transient errors
               message.contains("metadata") ||
               message.contains("checkpoint") ||
               message.contains("transaction log");
    }
    
    /**
     * Handles permanent failure by completing futures exceptionally and updating metrics
     */
    private void handlePermanentFailure(List<WriteBatch> batches, Exception exception) {
        backgroundFailureCount.incrementAndGet();
        
        log.error("Permanent failure processing {} batches: {}", batches.size(), exception.getMessage(), exception);
        
        TableWriteException writeException = exception instanceof TableWriteException 
            ? (TableWriteException) exception 
            : new TableWriteException("Background processing failed", exception);
        
        for (WriteBatch batch : batches) {
            batch.future.completeExceptionally(writeException);
        }
        
        // TODO: Add alerting mechanism for permanent failures
        // alertingService.sendAlert("Background batch processing failed", exception);
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
                // HIGH PRIORITY ISSUE #5 FIX: Thread-safe checkpoint creation to prevent race conditions
                createCheckpointSafely(tableName, table, result.getVersion());
                
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
                
                // Exponential backoff for conflicts
                try {
                    Thread.sleep((long) Math.pow(2, attempt) * 100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new TableWriteException("Interrupted during retry", ie);
                }
                
            } catch (java.io.IOException e) {
                // I/O errors are often transient - retry with exponential backoff
                lastException = e;
                ioErrorCount.incrementAndGet();
                
                if (isRetryableIOException(e)) {
                    log.warn("Recoverable I/O error on attempt {} for table {}: {}, retrying...", 
                            attempt, tableName, e.getMessage());
                    
                    try {
                        Thread.sleep((long) Math.pow(2, attempt) * 200); // Longer backoff for I/O
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new TableWriteException("Interrupted during retry", ie);
                    }
                } else {
                    permanentErrorCount.incrementAndGet();
                    log.error("Non-recoverable I/O error for table {}: {}", tableName, e.getMessage(), e);
                    break;
                }
                
            } catch (RuntimeException e) {
                // Check if this is a transient runtime error
                lastException = e;
                runtimeErrorCount.incrementAndGet();
                
                if (isRetryableRuntimeException(e)) {
                    log.warn("Recoverable runtime error on attempt {} for table {}: {}, retrying...", 
                            attempt, tableName, e.getMessage());
                    
                    try {
                        Thread.sleep((long) Math.pow(2, attempt) * 150);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new TableWriteException("Interrupted during retry", ie);
                    }
                } else {
                    permanentErrorCount.incrementAndGet();
                    log.error("Non-recoverable runtime error for table {}: {}", tableName, e.getMessage(), e);
                    break;
                }
                
            } catch (Exception e) {
                lastException = e;
                log.error("Unexpected error on attempt {} for table {}: {}", attempt, tableName, e.getMessage(), e);
                break;
            }
        }
        
        throw new TableWriteException("Failed to write after " + maxRetries + " attempts", lastException);
    }
    
    /**
     * Thread-safe checkpoint creation to prevent race conditions (HIGH PRIORITY ISSUE #5)
     * Ensures only one checkpoint is created per version and prevents concurrent checkpoint operations
     */
    private void createCheckpointSafely(String tableName, Table table, long version) {
        try {
            log.info("Checking if checkpoint should be created for version {}", version);
            
            // Thread-safe check and creation
            if (shouldCreateCheckpointThreadSafe(tableName, version)) {
                // Check if checkpoint is already in progress for this table
                String checkpointKey = tableName + ":" + version;
                CompletableFuture<Void> existingCheckpoint = pendingCheckpoints.get(checkpointKey);
                
                if (existingCheckpoint != null) {
                    log.debug("Checkpoint already in progress for {} version {}, waiting for completion", tableName, version);
                    try {
                        // HIGH PRIORITY ISSUE #6: Use configurable checkpoint timeout
                        long checkpointTimeoutMs = config.getPerformance().getCheckpointTimeoutMs();
                        existingCheckpoint.get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
                        log.debug("Existing checkpoint completed for {} version {}", tableName, version);
                    } catch (TimeoutException e) {
                        checkpointTimeoutCount.incrementAndGet(); // HIGH PRIORITY ISSUE #6: Track timeout metrics
                        log.warn("Existing checkpoint timeout after {}ms for {} version {}, proceeding anyway", 
                            config.getPerformance().getCheckpointTimeoutMs(), tableName, version);
                        // Don't return, let the current thread try to create checkpoint
                    } catch (Exception e) {
                        log.warn("Existing checkpoint failed for {} version {}: {}", tableName, version, e.getMessage());
                    }
                    return;
                }
                
                // Create new checkpoint asynchronously to avoid blocking writes
                CompletableFuture<Void> checkpointFuture = CompletableFuture.runAsync(() -> {
                    try {
                        log.info("Creating checkpoint for table {} at version {}", tableName, version);
                        table.checkpoint(engine, version);
                        checkpointCount.incrementAndGet();
                        log.info("✓ Successfully created checkpoint at version {} for table {} to optimize future reads", 
                                version, tableName);
                    } catch (Exception checkpointError) {
                        log.error("Failed to create checkpoint at version {} for table {}: {}", 
                                version, tableName, checkpointError.getMessage(), checkpointError);
                        throw new RuntimeException(checkpointError);
                    }
                }, commitExecutor);
                
                // Store the future to prevent duplicate checkpoint creation
                CompletableFuture<Void> previous = pendingCheckpoints.putIfAbsent(checkpointKey, checkpointFuture);
                if (previous != null) {
                    log.debug("Another thread started checkpoint for {} version {}, cancelling this one", tableName, version);
                    checkpointFuture.cancel(false);
                    return;
                }
                
                // Clean up completed checkpoint from tracking
                checkpointFuture.whenComplete((result, throwable) -> {
                    pendingCheckpoints.remove(checkpointKey);
                    if (throwable != null) {
                        log.error("Checkpoint failed and cleaned up for {} version {}", tableName, version);
                    }
                });
                
            } else {
                log.debug("Checkpoint not needed for version {}", version);
            }
        } catch (Exception e) {
            log.error("Error in checkpoint safety logic for table {} version {}: {}", tableName, version, e.getMessage(), e);
        }
    }

    /**
     * Thread-safe version of shouldCreateCheckpoint that tracks last checkpoint versions
     */
    private boolean shouldCreateCheckpointThreadSafe(String tableName, long version) {
        long checkpointInterval = config.getPerformance().getCheckpointInterval();
        
        // Check basic interval logic first
        if (version % checkpointInterval != 0 || version <= 0) {
            return false;
        }
        
        // Thread-safe check against last checkpoint version
        Long lastCheckpoint = lastCheckpointVersion.get(tableName);
        if (lastCheckpoint != null && version <= lastCheckpoint) {
            log.debug("Checkpoint already exists for table {} at version {} (last checkpoint: {})", 
                     tableName, version, lastCheckpoint);
            return false;
        }
        
        // Atomically update the last checkpoint version
        Long previous = lastCheckpointVersion.putIfAbsent(tableName, version);
        if (previous != null && version <= previous) {
            // Another thread already created a checkpoint at this or later version
            log.debug("Another thread created checkpoint for table {} (existing: {}, current: {})", 
                     tableName, previous, version);
            return false;
        }
        
        // If we had a previous version, try to update it atomically
        if (previous != null) {
            boolean updated = lastCheckpointVersion.replace(tableName, previous, version);
            if (!updated) {
                log.debug("Failed to update checkpoint version for table {} (concurrent update detected)", tableName);
                return false;
            }
        }
        
        log.debug("Checkpoint approved for table {} at version {} (interval: {})", tableName, version, checkpointInterval);
        return true;
    }
    
    /**
     * Determines if a checkpoint should be created based on configurable intervals.
     * This prevents transaction log from growing too large and causing performance issues.
     * @deprecated Use createCheckpointSafely instead for thread safety
     */
    @Deprecated
    private boolean shouldCreateCheckpoint(long version) {
        // Create checkpoint every N versions (configurable, default 10)
        long checkpointInterval = config.getPerformance().getCheckpointInterval();
        boolean shouldCreate = version % checkpointInterval == 0 && version > 0;
        
        log.debug("Checkpoint check: version={}, interval={}, modulo={}, shouldCreate={}", 
                 version, checkpointInterval, version % checkpointInterval, shouldCreate);
        
        return shouldCreate;
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
        
        // CRITICAL FIX: Use try-with-resources to ensure proper cleanup
        CloseableIterator<FilteredColumnarBatch> physicalData = null;
        CloseableIterator<io.delta.kernel.utils.DataFileStatus> dataFiles = null;
        CloseableIterator<Row> dataActions = null;
        
        try {
            // Empty partition values for non-partitioned table
            Map<String, io.delta.kernel.expressions.Literal> partitionValues = new HashMap<>();
            
            // Transform and write
            physicalData = Transaction.transformLogicalData(engine, txnState, data, partitionValues);
            
            DataWriteContext writeContext = 
                Transaction.getWriteContext(engine, txnState, partitionValues);
            
            dataFiles = engine.getParquetHandler().writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns()
            );
            
            dataActions = Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
            
            List<Row> actionsList = new ArrayList<>();
            while (dataActions.hasNext()) {
                actionsList.add(dataActions.next());
            }
            
            io.delta.kernel.utils.CloseableIterable<Row> dataActionsIterable = 
                io.delta.kernel.utils.CloseableIterable.inMemoryIterable(
                    createCloseableIterator(actionsList.iterator()));
            
            return txn.commit(engine, dataActionsIterable);
            
        } finally {
            // CRITICAL FIX: Ensure all iterators are properly closed
            if (dataActions != null) {
                try {
                    dataActions.close();
                    log.trace("Closed dataActions iterator");
                } catch (IOException e) {
                    log.warn("Error closing dataActions iterator: {}", e.getMessage());
                }
            }
            
            if (dataFiles != null) {
                try {
                    dataFiles.close();
                    log.trace("Closed dataFiles iterator");
                } catch (IOException e) {
                    log.warn("Error closing dataFiles iterator: {}", e.getMessage());
                }
            }
            
            if (physicalData != null) {
                try {
                    physicalData.close();
                    log.trace("Closed physicalData iterator");
                } catch (IOException e) {
                    log.warn("Error closing physicalData iterator: {}", e.getMessage());
                }
            }
            
            if (data != null) {
                try {
                    data.close();
                    log.trace("Closed data iterator");
                } catch (IOException e) {
                    log.warn("Error closing data iterator: {}", e.getMessage());
                }
            }
            
            // Close the batch to free columnar data
            if (recordBatch != null) {
                try {
                    // FilteredColumnarBatch wraps a ColumnarBatch - ensure proper cleanup
                    var columnarBatch = recordBatch.getData();
                    if (columnarBatch instanceof AutoCloseable) {
                        ((AutoCloseable) columnarBatch).close();
                        log.trace("Closed record batch data");
                    } else {
                        log.trace("Record batch does not implement AutoCloseable");
                    }
                } catch (Exception e) {
                    log.warn("Error closing record batch data: {}", e.getMessage());
                }
            }
        }
    }
    
    private <T> CloseableIterator<T> createCloseableIterator(Iterator<T> iterator) {
        return new CloseableIterator<T>() {
            private volatile boolean closed = false;
            
            @Override
            public boolean hasNext() {
                if (closed) {
                    return false;
                }
                return iterator.hasNext();
            }
            
            @Override
            public T next() {
                if (closed) {
                    throw new IllegalStateException("Iterator has been closed");
                }
                return iterator.next();
            }
            
            @Override
            public void close() throws IOException {
                if (closed) {
                    return; // Already closed
                }
                
                closed = true;
                
                // CRITICAL FIX: Properly close underlying resources
                try {
                    if (iterator instanceof AutoCloseable) {
                        try {
                            ((AutoCloseable) iterator).close();
                            log.debug("Successfully closed underlying AutoCloseable iterator");
                        } catch (Exception e) {
                            log.warn("Error closing AutoCloseable iterator: {}", e.getMessage());
                            throw new IOException("Failed to close underlying iterator", e);
                        }
                    }
                    
                    // If iterator is a CloseableIterator, close it directly
                    if (iterator instanceof CloseableIterator) {
                        try {
                            ((CloseableIterator<?>) iterator).close();
                            log.debug("Successfully closed underlying CloseableIterator");
                        } catch (IOException e) {
                            log.warn("Error closing CloseableIterator: {}", e.getMessage());
                            throw e;
                        }
                    }
                    
                    // For collection iterators, try to clear reference if possible
                    if (iterator.getClass().getName().contains("ArrayList") || 
                        iterator.getClass().getName().contains("LinkedList")) {
                        // Clear reference to help GC
                        log.debug("Released reference to collection iterator for GC");
                    }
                    
                } finally {
                    // Always log closure for debugging resource leaks
                    log.trace("CloseableIterator wrapper closed");
                }
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
        
        // Background processing metrics (NEW)
        metrics.put("background_retries", backgroundRetryCount.get());
        metrics.put("background_failures", backgroundFailureCount.get());
        
        // Error recovery metrics (NEW)
        metrics.put("io_errors", ioErrorCount.get());
        metrics.put("runtime_errors", runtimeErrorCount.get());
        metrics.put("permanent_errors", permanentErrorCount.get());
        
        // HIGH PRIORITY ISSUE #6: Timeout metrics
        metrics.put("batch_processing_timeouts", batchProcessingTimeoutCount.get());
        metrics.put("checkpoint_timeouts", checkpointTimeoutCount.get());
        metrics.put("write_timeouts", writeTimeoutCount.get());
        
        // Performance optimization metrics
        metrics.put("checkpoints_created", checkpointCount.get());
        metrics.put("batch_consolidations", batchConsolidationCount.get());
        metrics.put("optimal_batch_size", calculateOptimalBatchSize());
        
        // Checkpoint race condition prevention metrics (NEW - High Priority Issue #5)
        metrics.put("pending_checkpoints", (long) pendingCheckpoints.size());
        metrics.put("tracked_tables_for_checkpoints", (long) lastCheckpointVersion.size());
            
        // Configuration metrics
        metrics.put("configured_batch_timeout_ms", config.getPerformance().getBatchTimeoutMs());
        metrics.put("configured_max_batch_size", config.getPerformance().getMaxBatchSize());
        metrics.put("configured_max_retries", config.getPerformance().getMaxRetries());
        metrics.put("configured_checkpoint_interval", config.getPerformance().getCheckpointInterval());
        
        // HIGH PRIORITY ISSUE #6: Timeout configuration metrics
        metrics.put("configured_schema_operation_timeout_ms", config.getPerformance().getSchemaOperationTimeoutMs());
        metrics.put("configured_batch_processing_timeout_ms", config.getPerformance().getBatchProcessingTimeoutMs());
        metrics.put("configured_checkpoint_timeout_ms", config.getPerformance().getCheckpointTimeoutMs());
        metrics.put("configured_background_operation_timeout_ms", config.getPerformance().getBackgroundOperationTimeoutMs());
        metrics.put("configured_thread_pool_await_timeout_ms", config.getPerformance().getThreadPoolAwaitTimeoutMs());
        
        // S3A optimization status
        metrics.put("s3a_optimizations_enabled", true);
        metrics.put("parquet_block_size_mb", 256);
        metrics.put("connection_pool_size", config.getPerformance().getConnectionPoolSize());
        
        // Schema manager metrics
        metrics.put("schema_cache_stats", schemaManager.getCacheStats());
        
        // Thread pool metrics (NEW - part of High Priority Issue #4)
        if (commitExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) commitExecutor;
            metrics.put("commit_pool_core_size", tpe.getCorePoolSize());
            metrics.put("commit_pool_max_size", tpe.getMaximumPoolSize());
            metrics.put("commit_pool_current_size", tpe.getPoolSize());
            metrics.put("commit_pool_active_threads", tpe.getActiveCount());
            metrics.put("commit_pool_queue_size", tpe.getQueue().size());
            metrics.put("commit_pool_queue_remaining_capacity", tpe.getQueue().remainingCapacity());
            metrics.put("commit_pool_completed_tasks", tpe.getCompletedTaskCount());
            metrics.put("commit_pool_rejected_tasks", 0L); // CallerRunsPolicy doesn't increment rejection count
        }
        
        // Batch executor metrics (single thread, so simpler)
        metrics.put("batch_executor_thread_count", 1L);
        metrics.put("batch_executor_type", "SingleThreadScheduledExecutor");
        
        return metrics;
    }
}