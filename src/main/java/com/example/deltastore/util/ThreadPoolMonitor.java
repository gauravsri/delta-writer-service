package com.example.deltastore.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors thread pool health and provides alerting for queue depth and rejection rates.
 * Tracks performance metrics and implements backpressure mechanisms.
 */
@Component
@Slf4j
public class ThreadPoolMonitor {
    
    private final MeterRegistry meterRegistry;
    private final Map<String, ThreadPoolMetrics> monitoredPools = new ConcurrentHashMap<>();
    private final AtomicLong totalRejections = new AtomicLong(0);
    
    // Alert thresholds
    private static final double QUEUE_DEPTH_WARNING_THRESHOLD = 0.7; // 70% of max queue size
    private static final double QUEUE_DEPTH_CRITICAL_THRESHOLD = 0.9; // 90% of max queue size
    private static final double REJECTION_RATE_WARNING_THRESHOLD = 0.05; // 5% rejection rate
    
    public ThreadPoolMonitor(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Global rejection counter
        Gauge.builder("deltastore.threadpool.total_rejections", this, monitor -> (double) monitor.totalRejections.get())
            .description("Total number of task rejections across all thread pools")
            .register(meterRegistry);
    }
    
    /**
     * Registers a thread pool for monitoring
     */
    public void registerThreadPool(String poolName, ThreadPoolExecutor executor) {
        ThreadPoolMetrics metrics = new ThreadPoolMetrics(poolName, executor);
        monitoredPools.put(poolName, metrics);
        
        // Register Micrometer gauges for this pool
        registerPoolGauges(poolName, executor);
        
        log.info("Registered thread pool '{}' for monitoring (core={}, max={}, queue={})", 
            poolName, executor.getCorePoolSize(), executor.getMaximumPoolSize(), 
            executor.getQueue().remainingCapacity() + executor.getQueue().size());
    }
    
    /**
     * Monitors all registered thread pools and triggers alerts if needed
     */
    public void monitorPools() {
        for (ThreadPoolMetrics metrics : monitoredPools.values()) {
            updateMetrics(metrics);
            checkAlerts(metrics);
        }
    }
    
    /**
     * Gets comprehensive statistics for all monitored pools
     */
    public Map<String, Object> getPoolStatistics() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        
        for (ThreadPoolMetrics metrics : monitoredPools.values()) {
            updateMetrics(metrics);
            stats.put(metrics.poolName + "_active_threads", metrics.executor.getActiveCount());
            stats.put(metrics.poolName + "_pool_size", metrics.executor.getPoolSize());
            stats.put(metrics.poolName + "_queue_size", metrics.executor.getQueue().size());
            stats.put(metrics.poolName + "_queue_depth_ratio", metrics.getQueueDepthRatio());
            stats.put(metrics.poolName + "_completed_tasks", metrics.executor.getCompletedTaskCount());
            stats.put(metrics.poolName + "_total_tasks", metrics.executor.getTaskCount());
            stats.put(metrics.poolName + "_rejection_count", metrics.rejectionCount.get());
            stats.put(metrics.poolName + "_rejection_rate", metrics.getRejectionRate());
        }
        
        stats.put("total_rejections", totalRejections.get());
        stats.put("monitored_pools", monitoredPools.size());
        
        return stats;
    }
    
    /**
     * Checks if backpressure should be applied based on pool metrics
     */
    public boolean shouldApplyBackpressure(String poolName) {
        ThreadPoolMetrics metrics = monitoredPools.get(poolName);
        if (metrics == null) {
            return false;
        }
        
        updateMetrics(metrics);
        
        // Apply backpressure if queue is near capacity or rejection rate is high
        return metrics.getQueueDepthRatio() > QUEUE_DEPTH_WARNING_THRESHOLD || 
               metrics.getRejectionRate() > REJECTION_RATE_WARNING_THRESHOLD;
    }
    
    /**
     * Manually triggers an alert check for a specific pool
     */
    public void checkPoolHealth(String poolName) {
        ThreadPoolMetrics metrics = monitoredPools.get(poolName);
        if (metrics != null) {
            updateMetrics(metrics);
            checkAlerts(metrics);
        }
    }
    
    /**
     * Records a task rejection for monitoring
     */
    public void recordRejection(String poolName) {
        ThreadPoolMetrics metrics = monitoredPools.get(poolName);
        if (metrics != null) {
            metrics.rejectionCount.incrementAndGet();
            totalRejections.incrementAndGet();
            
            // Update rejection rate counter
            meterRegistry.counter("deltastore.threadpool.rejections", "pool", poolName).increment();
            
            log.warn("Task rejected by thread pool '{}' (total rejections: {})", 
                poolName, metrics.rejectionCount.get());
        }
    }
    
    private void registerPoolGauges(String poolName, ThreadPoolExecutor executor) {
        Gauge.builder("deltastore.threadpool.active_threads", executor, ThreadPoolExecutor::getActiveCount)
            .description("Number of active threads in the pool")
            .tags("pool", poolName)
            .register(meterRegistry);
            
        Gauge.builder("deltastore.threadpool.pool_size", executor, ThreadPoolExecutor::getPoolSize)
            .description("Current size of the thread pool")
            .tags("pool", poolName)
            .register(meterRegistry);
            
        Gauge.builder("deltastore.threadpool.queue_size", executor, e -> (double) e.getQueue().size())
            .description("Number of tasks in the queue")
            .tags("pool", poolName)
            .register(meterRegistry);
            
        Gauge.builder("deltastore.threadpool.completed_tasks", executor, ThreadPoolExecutor::getCompletedTaskCount)
            .description("Number of completed tasks")
            .tags("pool", poolName)
            .register(meterRegistry);
    }
    
    private void updateMetrics(ThreadPoolMetrics metrics) {
        // Update rejection rate calculation
        long currentTime = System.currentTimeMillis();
        if (currentTime - metrics.lastUpdateTime > 60000) { // Update every minute
            metrics.calculateRejectionRate();
            metrics.lastUpdateTime = currentTime;
        }
    }
    
    private void checkAlerts(ThreadPoolMetrics metrics) {
        double queueDepthRatio = metrics.getQueueDepthRatio();
        double rejectionRate = metrics.getRejectionRate();
        
        // Queue depth alerts
        if (queueDepthRatio >= QUEUE_DEPTH_CRITICAL_THRESHOLD) {
            log.error("CRITICAL: Thread pool '{}' queue depth at {:.1f}% capacity ({})", 
                metrics.poolName, queueDepthRatio * 100, metrics.executor.getQueue().size());
        } else if (queueDepthRatio >= QUEUE_DEPTH_WARNING_THRESHOLD) {
            log.warn("WARNING: Thread pool '{}' queue depth at {:.1f}% capacity ({})", 
                metrics.poolName, queueDepthRatio * 100, metrics.executor.getQueue().size());
        }
        
        // Rejection rate alerts
        if (rejectionRate >= REJECTION_RATE_WARNING_THRESHOLD) {
            log.warn("WARNING: Thread pool '{}' rejection rate at {:.2f}% ({})", 
                metrics.poolName, rejectionRate * 100, metrics.rejectionCount.get());
        }
        
        // Thread utilization alerts
        if (metrics.executor.getActiveCount() == metrics.executor.getMaximumPoolSize()) {
            log.warn("WARNING: Thread pool '{}' at maximum capacity ({} active threads)", 
                metrics.poolName, metrics.executor.getActiveCount());
        }
    }
    
    private static class ThreadPoolMetrics {
        final String poolName;
        final ThreadPoolExecutor executor;
        final AtomicLong rejectionCount = new AtomicLong(0);
        
        private long lastRejectionCount = 0;
        private long lastUpdateTime = System.currentTimeMillis();
        private double currentRejectionRate = 0.0;
        
        ThreadPoolMetrics(String poolName, ThreadPoolExecutor executor) {
            this.poolName = poolName;
            this.executor = executor;
        }
        
        double getQueueDepthRatio() {
            int queueSize = executor.getQueue().size();
            int queueCapacity = queueSize + executor.getQueue().remainingCapacity();
            return queueCapacity > 0 ? (double) queueSize / queueCapacity : 0.0;
        }
        
        double getRejectionRate() {
            return currentRejectionRate;
        }
        
        void calculateRejectionRate() {
            long currentRejections = rejectionCount.get();
            long newRejections = currentRejections - lastRejectionCount;
            long totalTasks = executor.getTaskCount();
            
            if (totalTasks > 0) {
                currentRejectionRate = (double) newRejections / totalTasks;
            } else {
                currentRejectionRate = 0.0;
            }
            
            lastRejectionCount = currentRejections;
        }
    }
}