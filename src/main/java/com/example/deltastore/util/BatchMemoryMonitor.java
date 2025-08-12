package com.example.deltastore.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors memory usage during batch processing operations.
 * Provides adaptive batch size limits and alerts for memory pressure.
 */
@Component
@Slf4j
public class BatchMemoryMonitor {
    
    private final MeterRegistry meterRegistry;
    private final MemoryMXBean memoryMXBean;
    
    // Memory tracking
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchMemoryUsed = new AtomicLong(0);
    private final AtomicReference<Long> peakBatchMemoryUsage = new AtomicReference<>(0L);
    private final Map<String, BatchMetrics> activeBatches = new ConcurrentHashMap<>();
    
    // Alert thresholds (configurable)
    private static final double MEMORY_WARNING_THRESHOLD = 0.8; // 80% of heap
    private static final double MEMORY_CRITICAL_THRESHOLD = 0.9; // 90% of heap
    private static final long MAX_BATCH_MEMORY_MB = 500; // 500MB per batch
    
    // Adaptive batch sizing
    private volatile int currentOptimalBatchSize = 100; // Start with default
    private final AtomicLong memoryPressureCount = new AtomicLong(0);
    
    public BatchMemoryMonitor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        
        // Register metrics
        registerMetrics();
        
        log.info("BatchMemoryMonitor initialized with warning threshold: {}%, critical threshold: {}%", 
            (int)(MEMORY_WARNING_THRESHOLD * 100), (int)(MEMORY_CRITICAL_THRESHOLD * 100));
    }
    
    /**
     * Starts monitoring a new batch operation
     */
    public BatchSession startBatch(String batchId, int entityCount, String entityType) {
        long memoryBefore = getCurrentMemoryUsage();
        
        BatchMetrics metrics = new BatchMetrics(batchId, entityCount, entityType, memoryBefore);
        activeBatches.put(batchId, metrics);
        
        log.debug("Started monitoring batch '{}' with {} entities of type '{}'", 
            batchId, entityCount, entityType);
        
        return new BatchSession(batchId, this);
    }
    
    /**
     * Completes monitoring for a batch operation
     */
    public void completeBatch(String batchId, boolean success) {
        BatchMetrics metrics = activeBatches.remove(batchId);
        if (metrics == null) {
            log.warn("Attempted to complete unknown batch: {}", batchId);
            return;
        }
        
        long memoryAfter = getCurrentMemoryUsage();
        long batchMemoryUsed = Math.max(0, memoryAfter - metrics.memoryBefore);
        
        // Update statistics
        totalBatchesProcessed.incrementAndGet();
        totalBatchMemoryUsed.addAndGet(batchMemoryUsed);
        updatePeakMemoryUsage(batchMemoryUsed);
        
        // Update metrics
        if (success) {
            meterRegistry.counter("deltastore.batch.completed", "result", "success", "entity_type", metrics.entityType)
                .increment();
        } else {
            meterRegistry.counter("deltastore.batch.completed", "result", "failure", "entity_type", metrics.entityType)
                .increment();
        }
        
        meterRegistry.gauge("deltastore.batch.memory_usage_mb", batchMemoryUsed / (1024 * 1024));
        
        // Check for memory pressure and adjust batch size
        checkMemoryPressure(batchMemoryUsed, metrics.entityCount);
        
        log.debug("Completed batch '{}' (success: {}) - Memory used: {} MB, Entities: {}", 
            batchId, success, batchMemoryUsed / (1024 * 1024), metrics.entityCount);
    }
    
    /**
     * Gets the current optimal batch size based on memory analysis
     */
    public int getOptimalBatchSize() {
        return currentOptimalBatchSize;
    }
    
    /**
     * Checks if a batch of given size would cause memory pressure
     */
    public boolean wouldCauseMemoryPressure(int batchSize) {
        long estimatedMemoryUsage = estimateMemoryUsage(batchSize);
        long availableMemory = getAvailableMemory();
        
        return estimatedMemoryUsage > availableMemory * MEMORY_WARNING_THRESHOLD;
    }
    
    /**
     * Gets comprehensive memory statistics
     */
    public Map<String, Object> getMemoryStatistics() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage();
        
        // Current memory state
        stats.put("heap_used_mb", heapUsage.getUsed() / (1024 * 1024));
        stats.put("heap_max_mb", heapUsage.getMax() / (1024 * 1024));
        stats.put("heap_usage_ratio", (double) heapUsage.getUsed() / heapUsage.getMax());
        stats.put("non_heap_used_mb", nonHeapUsage.getUsed() / (1024 * 1024));
        
        // Batch-specific metrics
        stats.put("total_batches_processed", totalBatchesProcessed.get());
        stats.put("average_batch_memory_mb", getAverageBatchMemoryUsage() / (1024 * 1024));
        stats.put("peak_batch_memory_mb", peakBatchMemoryUsage.get() / (1024 * 1024));
        stats.put("current_optimal_batch_size", currentOptimalBatchSize);
        stats.put("active_batches", activeBatches.size());
        stats.put("memory_pressure_events", memoryPressureCount.get());
        
        return stats;
    }
    
    /**
     * Forces garbage collection and returns memory freed
     */
    public long forceGarbageCollection() {
        long memoryBefore = getCurrentMemoryUsage();
        System.gc();
        
        // Wait a bit for GC to complete
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long memoryAfter = getCurrentMemoryUsage();
        long memoryFreed = memoryBefore - memoryAfter;
        
        log.info("Forced garbage collection - Memory freed: {} MB", memoryFreed / (1024 * 1024));
        
        return memoryFreed;
    }
    
    private void registerMetrics() {
        // Current memory usage
        Gauge.builder("deltastore.memory.heap_usage_ratio", this, monitor -> {
            MemoryUsage heapUsage = monitor.memoryMXBean.getHeapMemoryUsage();
            return (double) heapUsage.getUsed() / heapUsage.getMax();
        })
        .description("Current heap memory usage ratio")
        .register(meterRegistry);
        
        // Optimal batch size
        Gauge.builder("deltastore.batch.optimal_size", this, monitor -> (double) monitor.currentOptimalBatchSize)
            .description("Current optimal batch size based on memory analysis")
            .register(meterRegistry);
            
        // Active batches
        Gauge.builder("deltastore.batch.active_count", this, monitor -> (double) monitor.activeBatches.size())
            .description("Number of currently active batches")
            .register(meterRegistry);
            
        // Peak batch memory
        Gauge.builder("deltastore.batch.peak_memory_mb", this, monitor -> (double) monitor.peakBatchMemoryUsage.get() / (1024 * 1024))
            .description("Peak memory usage by a single batch in MB")
            .register(meterRegistry);
    }
    
    private void checkMemoryPressure(long batchMemoryUsed, int entityCount) {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        double memoryUsageRatio = (double) heapUsage.getUsed() / heapUsage.getMax();
        
        // Check for memory pressure
        if (memoryUsageRatio >= MEMORY_CRITICAL_THRESHOLD) {
            memoryPressureCount.incrementAndGet();
            log.error("CRITICAL: Memory usage at {:.1f}% of heap - reducing batch size", memoryUsageRatio * 100);
            adaptBatchSize(entityCount, false); // Reduce batch size
            
        } else if (memoryUsageRatio >= MEMORY_WARNING_THRESHOLD) {
            memoryPressureCount.incrementAndGet();
            log.warn("WARNING: Memory usage at {:.1f}% of heap - batch size optimization may be needed", memoryUsageRatio * 100);
            adaptBatchSize(entityCount, false); // Reduce batch size slightly
            
        } else if (memoryUsageRatio < 0.6 && batchMemoryUsed < MAX_BATCH_MEMORY_MB * 1024 * 1024) {
            // Memory usage is low, we can potentially increase batch size
            adaptBatchSize(entityCount, true); // Increase batch size
        }
    }
    
    private void adaptBatchSize(int currentEntityCount, boolean increase) {
        int adjustment = increase ? Math.max(1, currentEntityCount / 10) : -Math.max(1, currentEntityCount / 10);
        int newBatchSize = Math.max(10, Math.min(1000, currentOptimalBatchSize + adjustment));
        
        if (newBatchSize != currentOptimalBatchSize) {
            log.info("Adapting batch size from {} to {} based on memory analysis", 
                currentOptimalBatchSize, newBatchSize);
            currentOptimalBatchSize = newBatchSize;
        }
    }
    
    private long getCurrentMemoryUsage() {
        return memoryMXBean.getHeapMemoryUsage().getUsed();
    }
    
    private long getAvailableMemory() {
        MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
        return heapUsage.getMax() - heapUsage.getUsed();
    }
    
    private long estimateMemoryUsage(int batchSize) {
        // Rough estimation based on average memory per entity
        long avgBatchMemory = getAverageBatchMemoryUsage();
        long avgBatchSize = totalBatchesProcessed.get() > 0 ? 
            totalBatchMemoryUsed.get() / totalBatchesProcessed.get() : 100; // Default assumption
            
        return avgBatchSize > 0 ? (batchSize * avgBatchMemory) / avgBatchSize : batchSize * 1024; // 1KB per entity estimate
    }
    
    private long getAverageBatchMemoryUsage() {
        return totalBatchesProcessed.get() > 0 ? 
            totalBatchMemoryUsed.get() / totalBatchesProcessed.get() : 0;
    }
    
    private void updatePeakMemoryUsage(long batchMemoryUsed) {
        peakBatchMemoryUsage.updateAndGet(current -> Math.max(current, batchMemoryUsed));
    }
    
    /**
     * Session object for tracking individual batch operations
     */
    public static class BatchSession implements AutoCloseable {
        private final String batchId;
        private final BatchMemoryMonitor monitor;
        private boolean completed = false;
        
        public BatchSession(String batchId, BatchMemoryMonitor monitor) {
            this.batchId = batchId;
            this.monitor = monitor;
        }
        
        public void complete(boolean success) {
            if (!completed) {
                monitor.completeBatch(batchId, success);
                completed = true;
            }
        }
        
        @Override
        public void close() {
            if (!completed) {
                complete(false); // Assume failure if not explicitly completed
            }
        }
    }
    
    private static class BatchMetrics {
        final String batchId;
        final int entityCount;
        final String entityType;
        final long memoryBefore;
        final long startTime;
        
        BatchMetrics(String batchId, int entityCount, String entityType, long memoryBefore) {
            this.batchId = batchId;
            this.entityCount = entityCount;
            this.entityType = entityType;
            this.memoryBefore = memoryBefore;
            this.startTime = System.currentTimeMillis();
        }
    }
}