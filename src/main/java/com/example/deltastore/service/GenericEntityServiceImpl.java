package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.entity.EntityOperationResult;
import com.example.deltastore.entity.GenericEntityService;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.storage.DeltaTableManager;
import com.example.deltastore.util.BatchMemoryMonitor;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Generic implementation of EntityService that handles any entity type.
 * This replaces entity-specific service implementations to reduce code duplication
 * and provide consistency across all entity types.
 * 
 * @param <T> The entity type that extends GenericRecord
 */
@Slf4j
public class GenericEntityServiceImpl<T extends GenericRecord> implements EntityService<T> {

    private final String entityType;
    private final DeltaTableManager deltaTableManager;
    private final DeltaStoreMetrics metrics;
    private final GenericEntityService genericEntityService;
    private final BatchMemoryMonitor batchMemoryMonitor;

    public GenericEntityServiceImpl(
            String entityType,
            @Qualifier("optimized") DeltaTableManager deltaTableManager,
            DeltaStoreMetrics metrics,
            GenericEntityService genericEntityService,
            BatchMemoryMonitor batchMemoryMonitor) {
        this.entityType = entityType;
        this.deltaTableManager = deltaTableManager;
        this.metrics = metrics;
        this.batchMemoryMonitor = batchMemoryMonitor;
        this.genericEntityService = genericEntityService;
    }

    @Override
    public void save(T entity) {
        log.info("Saving entity of type: {}", entityType);
        Timer.Sample sample = metrics.startWriteTimer();
        
        try {
            deltaTableManager.write(entityType, Collections.singletonList(entity), entity.getSchema());
            metrics.recordWriteSuccess(entityType, 1L);
            log.info("Successfully saved entity of type: {}", entityType);
        } catch (Exception e) {
            metrics.recordWriteFailure(entityType, e.getClass().getSimpleName());
            log.error("Failed to save entity of type: {}", entityType, e);
            throw e;
        } finally {
            metrics.stopWriteTimer(sample, entityType);
        }
    }

    @Override
    public EntityOperationResult<T> saveWithResult(T entity) {
        return genericEntityService.save(entityType, entity);
    }

    @Override
    public BatchCreateResponse saveBatch(List<T> entities) {
        log.info("Starting batch save operation for {} entities of type: {}", entities.size(), entityType);
        long startTime = System.currentTimeMillis();
        Timer.Sample sample = metrics.startWriteTimer();
        
        List<String> successfulEntityIds = new ArrayList<>();
        List<BatchCreateResponse.FailureDetail> failures = new ArrayList<>();
        int totalBatches = 0;
        long totalDeltaTransactionTime = 0;
        
        try {
            // Get optimal chunk size from memory monitor
            final int CHUNK_SIZE = batchMemoryMonitor.getOptimalBatchSize();
            List<List<T>> entityChunks = partitionEntities(entities, CHUNK_SIZE);
            totalBatches = entityChunks.size();
            
            log.debug("Processing {} entities in {} batches of up to {} entities each", 
                     entities.size(), totalBatches, CHUNK_SIZE);
            
            for (int chunkIndex = 0; chunkIndex < entityChunks.size(); chunkIndex++) {
                List<T> chunk = entityChunks.get(chunkIndex);
                log.debug("Processing batch {}/{} with {} entities", 
                         chunkIndex + 1, totalBatches, chunk.size());
                
                // Check for memory pressure before processing
                if (batchMemoryMonitor.wouldCauseMemoryPressure(chunk.size())) {
                    log.warn("Memory pressure detected - forcing garbage collection before batch {}/{}", 
                            chunkIndex + 1, totalBatches);
                    batchMemoryMonitor.forceGarbageCollection();
                }
                
                // Start memory monitoring for this batch
                String batchId = entityType + "_batch_" + System.currentTimeMillis() + "_" + chunkIndex;
                BatchMemoryMonitor.BatchSession memorySession = batchMemoryMonitor.startBatch(
                    batchId, chunk.size(), entityType);
                
                long chunkStartTime = System.currentTimeMillis();
                
                try {
                    // Cast List<T> to List<GenericRecord>
                    List<GenericRecord> genericRecords = new ArrayList<>();
                    genericRecords.addAll(chunk);
                    deltaTableManager.write(entityType, genericRecords, chunk.get(0).getSchema());
                    
                    // All entities in this chunk succeeded
                    for (int i = 0; i < chunk.size(); i++) {
                        T entity = chunk.get(i);
                        // Extract ID if available, otherwise use index
                        String entityId = extractEntityId(entity, chunkIndex * CHUNK_SIZE + i);
                        successfulEntityIds.add(entityId);
                    }
                    
                    long chunkTime = System.currentTimeMillis() - chunkStartTime;
                    totalDeltaTransactionTime += chunkTime;
                    
                    log.debug("Successfully processed batch {}/{} in {}ms", 
                             chunkIndex + 1, totalBatches, chunkTime);
                    
                    // Mark batch as successful
                    memorySession.complete(true);
                    
                } catch (Exception e) {
                    log.warn("Failed to process batch {}/{}: {}", 
                            chunkIndex + 1, totalBatches, e.getMessage());
                    
                    // Add all entities in this chunk as failures
                    for (int entityIndex = 0; entityIndex < chunk.size(); entityIndex++) {
                        T entity = chunk.get(entityIndex);
                        String entityId = extractEntityId(entity, chunkIndex * CHUNK_SIZE + entityIndex);
                        failures.add(BatchCreateResponse.FailureDetail.builder()
                            .userId(entityId)  // Note: keeping userId field name for compatibility
                            .index(chunkIndex * CHUNK_SIZE + entityIndex)
                            .error(e.getMessage())
                            .errorType(e.getClass().getSimpleName())
                            .build());
                    }
                    
                    // Mark batch as failed
                    memorySession.complete(false);
                }
            }
            
            long processingTime = System.currentTimeMillis() - startTime;
            
            // Record metrics
            metrics.recordWriteSuccess(entityType, (long) successfulEntityIds.size());
            if (!failures.isEmpty()) {
                metrics.recordWriteFailure(entityType, "BatchPartialFailure");
            }
            
            // Build statistics
            BatchCreateResponse.BatchStatistics statistics = BatchCreateResponse.BatchStatistics.builder()
                .totalBatches(totalBatches)
                .avgBatchSize(totalBatches > 0 ? entities.size() / totalBatches : 0)
                .avgProcessingTimePerBatch(totalBatches > 0 ? totalDeltaTransactionTime / totalBatches : 0)
                .totalDeltaTransactionTime(totalDeltaTransactionTime)
                .deltaTransactionCount(totalBatches)
                .additionalMetrics(Map.of(
                    "chunksProcessed", totalBatches,
                    "averageChunkSize", CHUNK_SIZE,
                    "totalProcessingTimeMs", processingTime,
                    "entityType", entityType
                ))
                .build();
            
            BatchCreateResponse response = BatchCreateResponse.builder()
                .totalRequested(entities.size())
                .successCount(successfulEntityIds.size())
                .failureCount(failures.size())
                .successfulUserIds(successfulEntityIds)  // Note: keeping field name for compatibility
                .failures(failures)
                .processedAt(LocalDateTime.now())
                .processingTimeMs(processingTime)
                .statistics(statistics)
                .build();
            
            log.info("Batch save completed for {}: {}/{} successful, {} failed, {}ms total", 
                    entityType, successfulEntityIds.size(), entities.size(), failures.size(), processingTime);
            
            return response;
            
        } catch (Exception e) {
            metrics.recordWriteFailure(entityType, e.getClass().getSimpleName());
            log.error("Batch save operation failed completely for type: {}", entityType, e);
            throw e;
        } finally {
            metrics.stopWriteTimer(sample, entityType);
        }
    }

    @Override
    public EntityOperationResult<T> saveAllWithResult(List<T> entities) {
        return genericEntityService.saveAll(entityType, entities);
    }

    @Override
    public String getEntityType() {
        return entityType;
    }

    private List<List<T>> partitionEntities(List<T> entities, int chunkSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < entities.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, entities.size());
            partitions.add(entities.subList(i, end));
        }
        return partitions;
    }

    /**
     * Extracts entity ID from GenericRecord if available, otherwise uses fallback
     */
    private String extractEntityId(T entity, int fallbackIndex) {
        try {
            // Try common ID field names
            String[] idFieldNames = {"id", "userId", "user_id", "entityId", "entity_id"};
            
            for (String fieldName : idFieldNames) {
                Object idValue = entity.get(fieldName);
                if (idValue != null) {
                    return idValue.toString();
                }
            }
            
            // Fallback to index-based ID
            return entityType + "_" + fallbackIndex;
            
        } catch (Exception e) {
            // Fallback to index-based ID if extraction fails
            return entityType + "_" + fallbackIndex;
        }
    }
}