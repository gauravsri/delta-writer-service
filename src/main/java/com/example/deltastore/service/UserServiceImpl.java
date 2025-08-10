package com.example.deltastore.service;

import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.schemas.User;
import com.example.deltastore.storage.DeltaTableManager;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
