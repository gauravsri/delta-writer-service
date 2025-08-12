package com.example.deltastore.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks resource allocations and deallocations to detect potential memory leaks.
 * Provides metrics and alerting for resource cleanup failures.
 */
@Component
@Slf4j
public class ResourceLeakTracker {
    
    private final ConcurrentMap<String, ResourceInfo> activeResources = new ConcurrentHashMap<>();
    private final AtomicLong totalResourcesAllocated = new AtomicLong(0);
    private final AtomicLong totalResourcesReleased = new AtomicLong(0);
    private final AtomicLong totalLeaksDetected = new AtomicLong(0);
    
    private final Counter resourceAllocatedCounter;
    private final Counter resourceReleasedCounter;
    private final Counter resourceLeakCounter;
    
    public ResourceLeakTracker(MeterRegistry meterRegistry) {
        this.resourceAllocatedCounter = Counter.builder("deltastore.resources.allocated")
            .description("Number of resources allocated")
            .register(meterRegistry);
            
        this.resourceReleasedCounter = Counter.builder("deltastore.resources.released")
            .description("Number of resources properly released")
            .register(meterRegistry);
            
        this.resourceLeakCounter = Counter.builder("deltastore.resources.leaked")
            .description("Number of resource leaks detected")
            .register(meterRegistry);
            
        Gauge.builder("deltastore.resources.active", this, tracker -> (double) tracker.getActiveResourceCount())
            .description("Number of currently active resources")
            .register(meterRegistry);
    }
    
    /**
     * Tracks a new resource allocation
     */
    public void trackResource(String resourceId, String resourceType, String location) {
        ResourceInfo info = new ResourceInfo(resourceType, location, Instant.now());
        activeResources.put(resourceId, info);
        totalResourcesAllocated.incrementAndGet();
        resourceAllocatedCounter.increment();
        
        log.trace("Tracked resource: {} of type {} at {}", resourceId, resourceType, location);
    }
    
    /**
     * Marks a resource as properly released
     */
    public void releaseResource(String resourceId) {
        ResourceInfo removed = activeResources.remove(resourceId);
        if (removed != null) {
            totalResourcesReleased.incrementAndGet();
            resourceReleasedCounter.increment();
            log.trace("Released resource: {}", resourceId);
        } else {
            log.warn("Attempted to release unknown resource: {}", resourceId);
        }
    }
    
    /**
     * Detects and reports resource leaks
     */
    public void detectLeaks(long thresholdMinutes) {
        Instant threshold = Instant.now().minusSeconds(thresholdMinutes * 60);
        
        activeResources.entrySet().removeIf(entry -> {
            ResourceInfo info = entry.getValue();
            if (info.getAllocatedAt().isBefore(threshold)) {
                totalLeaksDetected.incrementAndGet();
                resourceLeakCounter.increment();
                log.warn("Resource leak detected: {} of type {} allocated at {} (location: {})", 
                    entry.getKey(), info.getResourceType(), info.getAllocatedAt(), info.getLocation());
                return true;
            }
            return false;
        });
    }
    
    /**
     * Gets statistics about resource usage
     */
    public Map<String, Object> getResourceStats() {
        return Map.of(
            "active_resources", activeResources.size(),
            "total_allocated", totalResourcesAllocated.get(),
            "total_released", totalResourcesReleased.get(),
            "total_leaks_detected", totalLeaksDetected.get(),
            "leak_rate", calculateLeakRate()
        );
    }
    
    public long getActiveResourceCount() {
        return activeResources.size();
    }
    
    private double calculateLeakRate() {
        long total = totalResourcesAllocated.get();
        return total > 0 ? (double) totalLeaksDetected.get() / total : 0.0;
    }
    
    /**
     * Clears all tracked resources (for testing)
     */
    public void clearAll() {
        activeResources.clear();
        log.debug("Cleared all tracked resources");
    }
    
    private static class ResourceInfo {
        private final String resourceType;
        private final String location;
        private final Instant allocatedAt;
        
        public ResourceInfo(String resourceType, String location, Instant allocatedAt) {
            this.resourceType = resourceType;
            this.location = location;
            this.allocatedAt = allocatedAt;
        }
        
        public String getResourceType() {
            return resourceType;
        }
        
        public String getLocation() {
            return location;
        }
        
        public Instant getAllocatedAt() {
            return allocatedAt;
        }
    }
}