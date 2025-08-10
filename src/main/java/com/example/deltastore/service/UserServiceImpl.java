package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.schemas.User;
import com.example.deltastore.storage.DeltaTableManager;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserServiceImpl implements UserService {

    private static final String TABLE_NAME = "users";
    private static final String PRIMARY_KEY_COLUMN = "user_id";

    private final DeltaTableManager deltaTableManager;
    private final DeltaStoreMetrics metrics;

    @Override
    public void save(User user) {
        log.info("Saving user: {}", user.getUserId());
        Timer.Sample sample = metrics.startWriteTimer();
        
        try {
            // The Avro-generated User class is already a GenericRecord.
            deltaTableManager.write(TABLE_NAME, Collections.singletonList(user), user.getSchema());
            metrics.recordWriteSuccess(TABLE_NAME, 1);
            log.info("Successfully saved user: {}", user.getUserId());
        } catch (Exception e) {
            metrics.recordWriteFailure(TABLE_NAME, e.getClass().getSimpleName());
            log.error("Failed to save user: {}", user.getUserId(), e);
            throw e;
        } finally {
            metrics.stopWriteTimer(sample, TABLE_NAME);
        }
    }

    @Override
    public Optional<User> findById(String userId) {
        log.debug("Finding user by ID: {}", userId);
        Timer.Sample sample = metrics.startReadTimer();
        
        try {
            Optional<User> result = deltaTableManager.read(TABLE_NAME, PRIMARY_KEY_COLUMN, userId)
                    .map(this::mapToUser);
            metrics.recordReadSuccess(TABLE_NAME);
            log.debug("User {} found: {}", userId, result.isPresent());
            return result;
        } catch (Exception e) {
            metrics.recordReadFailure(TABLE_NAME, e.getClass().getSimpleName());
            log.error("Failed to find user: {}", userId, e);
            throw e;
        } finally {
            metrics.stopReadTimer(sample, TABLE_NAME);
        }
    }

    @Override
    public List<User> findByPartitions(Map<String, String> partitionFilters) {
        log.debug("Finding users by partitions: {}", partitionFilters);
        Timer.Sample sample = metrics.startPartitionReadTimer();
        
        try {
            List<User> results = deltaTableManager.readByPartitions(TABLE_NAME, partitionFilters).stream()
                    .map(this::mapToUser)
                    .collect(Collectors.toList());
            metrics.recordPartitionReadSuccess(TABLE_NAME, results.size());
            log.debug("Found {} users matching partition filters: {}", results.size(), partitionFilters);
            return results;
        } catch (Exception e) {
            metrics.recordPartitionReadFailure(TABLE_NAME, e.getClass().getSimpleName());
            log.error("Failed to find users by partitions: {}", partitionFilters, e);
            throw e;
        } finally {
            metrics.stopPartitionReadTimer(sample, TABLE_NAME);
        }
    }

    @Override
    public BatchCreateResponse saveBatch(List<User> users) {
        log.info("Starting batch save operation for {} users", users.size());
        long startTime = System.currentTimeMillis();
        Timer.Sample sample = metrics.startWriteTimer();
        
        List<String> successfulUserIds = new ArrayList<>();
        List<BatchCreateResponse.FailureDetail> failures = new ArrayList<>();
        int totalBatches = 0;
        long totalDeltaTransactionTime = 0;
        
        try {
            // Process in smaller chunks for better performance and memory management
            final int CHUNK_SIZE = 100;
            List<List<User>> userChunks = partitionUsers(users, CHUNK_SIZE);
            totalBatches = userChunks.size();
            
            log.debug("Processing {} users in {} batches of up to {} users each", 
                     users.size(), totalBatches, CHUNK_SIZE);
            
            for (int chunkIndex = 0; chunkIndex < userChunks.size(); chunkIndex++) {
                List<User> chunk = userChunks.get(chunkIndex);
                log.debug("Processing batch {}/{} with {} users", 
                         chunkIndex + 1, totalBatches, chunk.size());
                
                long chunkStartTime = System.currentTimeMillis();
                
                try {
                    // Cast List<User> to List<GenericRecord> since User implements GenericRecord
                    List<org.apache.avro.generic.GenericRecord> genericRecords = new ArrayList<>(chunk);
                    deltaTableManager.write(TABLE_NAME, genericRecords, chunk.get(0).getSchema());
                    
                    // All users in this chunk succeeded
                    chunk.forEach(user -> successfulUserIds.add(user.getUserId().toString()));
                    
                    long chunkTime = System.currentTimeMillis() - chunkStartTime;
                    totalDeltaTransactionTime += chunkTime;
                    
                    log.debug("Successfully processed batch {}/{} in {}ms", 
                             chunkIndex + 1, totalBatches, chunkTime);
                    
                } catch (Exception e) {
                    log.warn("Failed to process batch {}/{}: {}", 
                            chunkIndex + 1, totalBatches, e.getMessage());
                    
                    // Add all users in this chunk as failures
                    for (int userIndex = 0; userIndex < chunk.size(); userIndex++) {
                        User user = chunk.get(userIndex);
                        failures.add(BatchCreateResponse.FailureDetail.builder()
                            .userId(user.getUserId().toString())
                            .index(chunkIndex * CHUNK_SIZE + userIndex)
                            .error(e.getMessage())
                            .errorType(e.getClass().getSimpleName())
                            .build());
                    }
                }
            }
            
            long processingTime = System.currentTimeMillis() - startTime;
            
            // Record metrics
            metrics.recordWriteSuccess(TABLE_NAME, successfulUserIds.size());
            if (!failures.isEmpty()) {
                metrics.recordWriteFailure(TABLE_NAME, "BatchPartialFailure");
            }
            
            // Build statistics
            BatchCreateResponse.BatchStatistics statistics = BatchCreateResponse.BatchStatistics.builder()
                .totalBatches(totalBatches)
                .avgBatchSize(users.size() / totalBatches)
                .avgProcessingTimePerBatch(totalBatches > 0 ? totalDeltaTransactionTime / totalBatches : 0)
                .totalDeltaTransactionTime(totalDeltaTransactionTime)
                .deltaTransactionCount(totalBatches)
                .additionalMetrics(Map.of(
                    "chunksProcessed", totalBatches,
                    "averageChunkSize", CHUNK_SIZE,
                    "totalProcessingTimeMs", processingTime
                ))
                .build();
            
            BatchCreateResponse response = BatchCreateResponse.builder()
                .totalRequested(users.size())
                .successCount(successfulUserIds.size())
                .failureCount(failures.size())
                .successfulUserIds(successfulUserIds)
                .failures(failures)
                .processedAt(LocalDateTime.now())
                .processingTimeMs(processingTime)
                .statistics(statistics)
                .build();
            
            log.info("Batch save completed: {}/{} successful, {} failed, {}ms total", 
                    successfulUserIds.size(), users.size(), failures.size(), processingTime);
            
            return response;
            
        } catch (Exception e) {
            metrics.recordWriteFailure(TABLE_NAME, e.getClass().getSimpleName());
            log.error("Batch save operation failed completely", e);
            throw e;
        } finally {
            metrics.stopWriteTimer(sample, TABLE_NAME);
        }
    }
    
    private List<List<User>> partitionUsers(List<User> users, int chunkSize) {
        List<List<User>> partitions = new ArrayList<>();
        for (int i = 0; i < users.size(); i += chunkSize) {
            int end = Math.min(i + chunkSize, users.size());
            partitions.add(users.subList(i, end));
        }
        return partitions;
    }

    private User mapToUser(Map<String, Object> map) {
        // This conversion is safe because we control the source data.
        // In a more complex system, more robust mapping (e.g., MapStruct) would be better.
        return User.newBuilder()
                .setUserId((String) map.get("user_id"))
                .setUsername((String) map.get("username"))
                .setEmail((String) map.get("email"))
                .setCountry((String) map.get("country"))
                .setSignupDate((String) map.get("signup_date"))
                .build();
    }
}
