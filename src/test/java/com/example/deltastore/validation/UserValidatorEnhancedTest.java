package com.example.deltastore.validation;

import com.example.deltastore.schemas.User;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

class UserValidatorEnhancedTest {

    private UserValidator userValidator;

    @BeforeEach
    void setUp() {
        userValidator = new UserValidator();
    }

    @Test
    @DisplayName("Should validate valid user with all fields")
    void testValidUserWithAllFields() {
        // Given
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail("test@example.com")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertTrue(errors.isEmpty());
    }

    @Test
    @DisplayName("Should validate valid user without optional email")
    void testValidUserWithoutEmail() {
        // Given
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail(null)
            .setCountry("CA")
            .setSignupDate("2024-02-15")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertTrue(errors.isEmpty());
    }

    @Test
    @DisplayName("Should reject null user")
    void testNullUser() {
        // When
        List<String> errors = userValidator.validate(null);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User cannot be null"));
    }

    @Test
    @DisplayName("Should reject user with null user_id")
    void testNullUserId() {
        // Given - create user with direct field access since builder doesn't allow nulls
        User user = new User();
        user.setUserId(null);
        user.setUsername("testuser");
        user.setCountry("US");
        user.setSignupDate("2024-01-01");

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User ID is required"));
    }

    @Test
    @DisplayName("Should reject user with empty user_id")
    void testEmptyUserId() {
        // Given
        User user = User.newBuilder()
            .setUserId("")
            .setUsername("testuser")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User ID is required"));
    }

    @Test
    @DisplayName("Should reject user_id with invalid characters")
    void testInvalidUserIdCharacters() {
        // Given
        User user = User.newBuilder()
            .setUserId("user@123") // @ is not allowed
            .setUsername("testuser")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User ID must be 1-50 alphanumeric characters, underscores, or hyphens"));
    }

    @Test
    @DisplayName("Should reject user_id that is too long")
    void testUserIdTooLong() {
        // Given
        String longUserId = "a".repeat(51); // 51 characters
        User user = User.newBuilder()
            .setUserId(longUserId)
            .setUsername("testuser")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User ID must be 1-50 alphanumeric characters, underscores, or hyphens"));
    }

    @Test
    @DisplayName("Should accept valid user_id with underscores and hyphens")
    void testValidUserIdWithSpecialChars() {
        // Given
        User user = User.newBuilder()
            .setUserId("user_123-test")
            .setUsername("testuser")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertTrue(errors.isEmpty());
    }

    @Test
    @DisplayName("Should reject user with null username")
    void testNullUsername() {
        // Given - create user with direct field access since builder doesn't allow nulls
        User user = new User();
        user.setUserId("user123");
        user.setUsername(null);
        user.setCountry("US");
        user.setSignupDate("2024-01-01");

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Username is required"));
    }

    @Test
    @DisplayName("Should reject username that is too long")
    void testUsernameTooLong() {
        // Given
        String longUsername = "a".repeat(101); // 101 characters
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername(longUsername)
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Username must be 100 characters or less"));
    }

    @Test
    @DisplayName("Should accept username at maximum length")
    void testUsernameAtMaxLength() {
        // Given
        String maxLengthUsername = "a".repeat(100); // Exactly 100 characters
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername(maxLengthUsername)
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertTrue(errors.isEmpty());
    }

    @Test
    @DisplayName("Should reject invalid email format")
    void testInvalidEmailFormat() {
        // Given
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setEmail("invalid-email")
            .setCountry("US")
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Invalid email format"));
    }

    @Test
    @DisplayName("Should accept valid email formats")
    void testValidEmailFormats() {
        String[] validEmails = {
            "test@example.com",
            "user.name@domain.co.uk",
            "user+tag@example.org",
            "firstname_lastname@company.com",
            "user123@test123.co"
        };

        for (String email : validEmails) {
            // Given
            User user = User.newBuilder()
                .setUserId("user123")
                .setUsername("testuser")
                .setEmail(email)
                .setCountry("US")
                .setSignupDate("2024-01-01")
                .build();

            // When
            List<String> errors = userValidator.validate(user);

            // Then
            assertTrue(errors.isEmpty(), "Email should be valid: " + email);
        }
    }

    @Test
    @DisplayName("Should reject null country")
    void testNullCountry() {
        // Given - create user with direct field access since builder doesn't allow nulls
        User user = new User();
        user.setUserId("user123");
        user.setUsername("testuser");
        user.setCountry(null);
        user.setSignupDate("2024-01-01");

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Country is required"));
    }

    @Test
    @DisplayName("Should reject country that is too long")
    void testCountryTooLong() {
        // Given
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setCountry("verylongcountryname") // More than 10 characters
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Country must be 10 characters or less"));
    }

    @Test
    @DisplayName("Should accept country at maximum length")
    void testCountryAtMaxLength() {
        // Given
        User user = User.newBuilder()
            .setUserId("user123")
            .setUsername("testuser")
            .setCountry("1234567890") // Exactly 10 characters
            .setSignupDate("2024-01-01")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertTrue(errors.isEmpty());
    }

    @Test
    @DisplayName("Should reject null signup date")
    void testNullSignupDate() {
        // Given - create user with direct field access since builder doesn't allow nulls
        User user = new User();
        user.setUserId("user123");
        user.setUsername("testuser");
        user.setCountry("US");
        user.setSignupDate(null);

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("Signup date is required"));
    }

    @Test
    @DisplayName("Should reject invalid signup date format")
    void testInvalidSignupDateFormat() {
        String[] invalidDates = {
            "2024/01/01",    // Wrong separator
            "01-01-2024",    // Wrong order
            "2024-1-1",      // Single digits
            "24-01-01",      // Two-digit year
            "invalid-date"   // Not a date
            // Note: The current validator only checks format YYYY-MM-DD, not logical validity
        };

        for (String invalidDate : invalidDates) {
            // Given
            User user = User.newBuilder()
                .setUserId("user123")
                .setUsername("testuser")
                .setCountry("US")
                .setSignupDate(invalidDate)
                .build();

            // When
            List<String> errors = userValidator.validate(user);

            // Then
            assertFalse(errors.isEmpty(), "Date should be invalid: " + invalidDate);
            assertTrue(errors.contains("Signup date must be in YYYY-MM-DD format"), 
                "Should reject date format: " + invalidDate);
        }
    }

    @Test
    @DisplayName("Should accept valid signup date formats")
    void testValidSignupDateFormats() {
        String[] validDates = {
            "2024-01-01",
            "2023-12-31",
            "2024-02-29", // Leap year
            "1990-06-15"
        };

        for (String validDate : validDates) {
            // Given
            User user = User.newBuilder()
                .setUserId("user123")
                .setUsername("testuser")
                .setCountry("US")
                .setSignupDate(validDate)
                .build();

            // When
            List<String> errors = userValidator.validate(user);

            // Then
            assertTrue(errors.isEmpty(), "Date should be valid: " + validDate);
        }
    }

    @Test
    @DisplayName("Should collect multiple validation errors")
    void testMultipleValidationErrors() {
        // Given - user with multiple invalid fields using direct field access
        User user = new User();
        user.setUserId(""); // Empty user ID
        user.setUsername(null); // Null username
        user.setEmail("invalid-email"); // Invalid email
        user.setCountry(""); // Empty country
        user.setSignupDate("invalid-date"); // Invalid date

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertEquals(5, errors.size());
        assertTrue(errors.contains("User ID is required"));
        assertTrue(errors.contains("Username is required"));
        assertTrue(errors.contains("Invalid email format"));
        assertTrue(errors.contains("Country is required"));
        assertTrue(errors.contains("Signup date must be in YYYY-MM-DD format"));
    }

    @Test
    @DisplayName("Should handle empty string fields")
    void testEmptyStringFields() {
        // Given
        User user = User.newBuilder()
            .setUserId("")
            .setUsername("")
            .setEmail("")
            .setCountry("")
            .setSignupDate("")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        // Empty email should be treated as null (no validation error for empty email)
        assertTrue(errors.contains("User ID is required"));
        assertTrue(errors.contains("Username is required"));
        assertTrue(errors.contains("Country is required"));
        assertTrue(errors.contains("Signup date is required"));
    }

    @Test
    @DisplayName("Should handle whitespace-only fields")
    void testWhitespaceOnlyFields() {
        // Given
        User user = User.newBuilder()
            .setUserId("   ")
            .setUsername("   ")
            .setEmail("   ")
            .setCountry("   ")
            .setSignupDate("   ")
            .build();

        // When
        List<String> errors = userValidator.validate(user);

        // Then
        assertFalse(errors.isEmpty());
        assertTrue(errors.contains("User ID is required"));
        assertTrue(errors.contains("Username is required"));
        assertTrue(errors.contains("Country is required"));
        assertTrue(errors.contains("Signup date is required"));
    }
}