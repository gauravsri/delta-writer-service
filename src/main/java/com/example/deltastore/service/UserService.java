package com.example.deltastore.service;

import com.example.deltastore.schemas.User;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service layer for handling business logic related to User data.
 */
public interface UserService {

    /**
     * Saves a user record.
     * @param user The user object to save.
     */
    void save(User user);

    /**
     * Finds a user by their primary key (ID).
     * @param userId The ID of the user to find.
     * @return An Optional containing the User, or empty if not found.
     */
    Optional<User> findById(String userId);

    /**
     * Finds all users that match the given partition filters.
     * @param partitionFilters A map of partition column names to their desired values.
     * @return A list of users that match the partition filters.
     */
    List<User> findByPartitions(Map<String, String> partitionFilters);
}
