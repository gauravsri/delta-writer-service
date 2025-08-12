package com.example.deltastore.schema;

import io.delta.kernel.types.*;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import com.example.deltastore.config.DeltaStoreConfiguration;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.time.Duration;

/**
 * Dynamic schema management system that converts Avro schemas to Delta Lake schemas automatically.
 * Replaces hard-coded schema mapping with intelligent dynamic conversion.
 */
@Component
@Slf4j
public class DeltaSchemaManager {
    
    private final Cache<String, StructType> schemaCache;
    private final AvroToDeltaSchemaConverter schemaConverter;
    private final SchemaCompatibilityChecker compatibilityChecker;
    private final DeltaStoreConfiguration config;
    
    public DeltaSchemaManager(DeltaStoreConfiguration config) {
        this.config = config;
        this.schemaConverter = new AvroToDeltaSchemaConverter();
        this.compatibilityChecker = new SchemaCompatibilityChecker();
        
        // Initialize bounded cache with TTL to prevent memory leaks
        this.schemaCache = Caffeine.newBuilder()
            .maximumSize(1000) // Limit to 1000 schemas
            .expireAfterAccess(Duration.ofMillis(config.getSchema().getSchemaCacheTtlMs()))
            .recordStats() // Enable metrics
            .removalListener((key, value, cause) -> {
                log.debug("Evicted schema from cache: {} (cause: {})", key, cause);
            })
            .build();
        
        log.info("Initialized schema cache with TTL: {}ms, max size: 1000", 
            config.getSchema().getSchemaCacheTtlMs());
    }
    
    /**
     * Gets or creates a Delta schema from an Avro schema with caching
     */
    public StructType getOrCreateDeltaSchema(Schema avroSchema) {
        String schemaKey = generateSchemaKey(avroSchema);
        
        return schemaCache.get(schemaKey, k -> {
            log.info("Converting Avro schema to Delta schema for: {}", avroSchema.getFullName());
            StructType deltaSchema = schemaConverter.convertSchema(avroSchema);
            log.debug("Successfully converted schema with {} fields", deltaSchema.fields().size());
            return deltaSchema;
        });
    }
    
    /**
     * Validates schema compatibility for evolution
     */
    public boolean isSchemaCompatible(Schema oldSchema, Schema newSchema) {
        return compatibilityChecker.isCompatible(oldSchema, newSchema);
    }
    
    /**
     * Invalidates cached schema to force refresh
     */
    public void invalidateSchema(Schema avroSchema) {
        String schemaKey = generateSchemaKey(avroSchema);
        schemaCache.invalidate(schemaKey);
        log.debug("Invalidated cached schema for: {}", avroSchema.getFullName());
    }
    
    /**
     * Gets current cache statistics including hit rates and evictions
     */
    public Map<String, Object> getCacheStats() {
        var stats = schemaCache.stats();
        return Map.of(
            "cached_schemas", schemaCache.estimatedSize(),
            "cache_hit_rate", stats.hitRate(),
            "cache_miss_rate", stats.missRate(),
            "eviction_count", stats.evictionCount(),
            "hit_count", stats.hitCount(),
            "miss_count", stats.missCount(),
            "load_count", stats.loadCount(),
            "average_load_penalty_ms", stats.averageLoadPenalty() / 1_000_000.0 // Convert to ms
        );
    }
    
    /**
     * Manually triggers cache cleanup and returns statistics
     */
    public void cleanupCache() {
        schemaCache.cleanUp();
        log.debug("Manual cache cleanup completed. Current size: {}", schemaCache.estimatedSize());
    }
    
    private String generateSchemaKey(Schema avroSchema) {
        // Use full name + fingerprint for robust caching
        return avroSchema.getFullName() + "_" + Math.abs(avroSchema.toString().hashCode());
    }
}