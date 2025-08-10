package com.example.deltastore.validation;

import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class UserValidatorTest {

    private UserValidator userValidator;

    @BeforeEach
    void setUp() {
        userValidator = new UserValidator();
    }

    @Test
    void testValidateValidUser() {
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertTrue(errors.isEmpty());
    }

    @Test
    void testValidateNullUser() {
        List<String> errors = userValidator.validate(null);
        assertEquals(1, errors.size());
        assertEquals("User cannot be null", errors.get(0));
    }

    @Test
    void testValidateUserWithEmptyUserId() {
        User user = User.newBuilder()
            .setUserId("")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("User ID is required")));
    }

    @Test
    void testValidateUserWithInvalidUserId() {
        User user = User.newBuilder()
            .setUserId("user@#$%")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("User ID must be 1-50 alphanumeric characters")));
    }

    @Test
    void testValidateUserWithInvalidEmail() {
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail("invalid-email")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("Invalid email format")));
    }

    @Test
    void testValidateUserWithNullEmail() {
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail(null)
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        // Null email should be allowed as it's optional
        List<String> errors = userValidator.validate(user);
        assertFalse(errors.stream().anyMatch(e -> e.contains("email")));
    }

    @Test
    void testValidateUserWithLongUserId() {
        String longUserId = "a".repeat(51);
        User user = User.newBuilder()
            .setUserId(longUserId)
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("User ID must be 1-50 alphanumeric characters")));
    }

    @Test
    void testValidateUserWithLongUsername() {
        String longUsername = "a".repeat(101);
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername(longUsername)
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-08-09")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("Username must be 100 characters or less")));
    }

    @Test
    void testValidateUserWithInvalidDate() {
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("invalid-date")
            .build();

        List<String> errors = userValidator.validate(user);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("Signup date must be in YYYY-MM-DD format")));
    }
}