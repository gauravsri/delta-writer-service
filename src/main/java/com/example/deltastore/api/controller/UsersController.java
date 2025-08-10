package com.example.deltastore.api.controller;

import com.example.deltastore.schemas.User;
import com.example.deltastore.service.UserService;
import com.example.deltastore.validation.UserValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
@Slf4j
public class UsersController {

    private final UserService userService;
    private final UserValidator userValidator;

    @PostMapping
    public ResponseEntity<?> createUser(@Valid @RequestBody User user) {
        log.info("Creating user with ID: {}", user != null ? user.getUserId() : "null");
        
        // Validate user
        List<String> validationErrors = userValidator.validate(user);
        if (!validationErrors.isEmpty()) {
            log.warn("User validation failed for ID: {} with errors: {}", 
                    user != null ? user.getUserId() : "null", validationErrors);
            return ResponseEntity.badRequest().body(Map.of("errors", validationErrors));
        }
        
        try {
            userService.save(user);
            log.info("Successfully created user: {}", user.getUserId());
            return ResponseEntity.status(HttpStatus.CREATED).build();
        } catch (Exception e) {
            log.error("Failed to create user: {}", user.getUserId(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to create user"));
        }
    }

    @GetMapping("/{userId}")
    public ResponseEntity<?> getUserById(@PathVariable String userId) {
        if (!StringUtils.hasText(userId)) {
            log.warn("Get user request with empty userId");
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "User ID cannot be empty"));
        }
        
        if (userId.length() > 50) {
            log.warn("Get user request with userId too long: {}", userId.substring(0, 50));
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "User ID must be 50 characters or less"));
        }
        
        log.debug("Retrieving user: {}", userId);
        
        try {
            return userService.findById(userId)
                    .map(user -> {
                        log.debug("Found user: {}", userId);
                        return ResponseEntity.ok(user);
                    })
                    .orElseGet(() -> {
                        log.debug("User not found: {}", userId);
                        return ResponseEntity.notFound().build();
                    });
        } catch (Exception e) {
            log.error("Failed to retrieve user: {}", userId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to retrieve user"));
        }
    }

    @GetMapping
    public ResponseEntity<?> findUsersByPartition(
            @RequestParam Map<String, String> partitionFilters) {
        
        if (partitionFilters == null) {
            log.warn("Find users request with null partition filters");
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Partition filters cannot be null"));
        }
        
        // Validate partition filter values
        for (Map.Entry<String, String> entry : partitionFilters.entrySet()) {
            if (!StringUtils.hasText(entry.getKey())) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Partition filter keys cannot be empty"));
            }
            if (entry.getValue() == null) {
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Partition filter values cannot be null"));
            }
        }
        
        log.debug("Finding users with partition filters: {}", partitionFilters);
        
        try {
            List<User> users = userService.findByPartitions(partitionFilters);
            log.debug("Found {} users matching partition filters", users.size());
            return ResponseEntity.ok(users);
        } catch (Exception e) {
            log.error("Failed to find users with partition filters: {}", partitionFilters, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to retrieve users"));
        }
    }
}
