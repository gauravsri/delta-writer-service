package com.example.deltastore.validation;

import com.example.deltastore.schemas.User;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class UserValidator {
    
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
        "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
    );
    
    private static final Pattern USER_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{1,50}$");
    
    public List<String> validate(User user) {
        List<String> errors = new ArrayList<>();
        
        if (user == null) {
            errors.add("User cannot be null");
            return errors;
        }
        
        // Validate user_id
        if (!StringUtils.hasText(user.getUserId())) {
            errors.add("User ID is required");
        } else if (!USER_ID_PATTERN.matcher(user.getUserId()).matches()) {
            errors.add("User ID must be 1-50 alphanumeric characters, underscores, or hyphens");
        }
        
        // Validate username
        if (!StringUtils.hasText(user.getUsername())) {
            errors.add("Username is required");
        } else if (user.getUsername().length() > 100) {
            errors.add("Username must be 100 characters or less");
        }
        
        // Validate email (optional but must be valid if present)
        if (StringUtils.hasText(user.getEmail())) {
            if (!EMAIL_PATTERN.matcher(user.getEmail()).matches()) {
                errors.add("Invalid email format");
            }
        }
        
        // Validate country
        if (!StringUtils.hasText(user.getCountry())) {
            errors.add("Country is required");
        } else if (user.getCountry().length() > 10) {
            errors.add("Country must be 10 characters or less");
        }
        
        // Validate signup_date
        if (!StringUtils.hasText(user.getSignupDate())) {
            errors.add("Signup date is required");
        } else {
            try {
                // Basic date format validation (YYYY-MM-DD)
                if (!user.getSignupDate().matches("\\d{4}-\\d{2}-\\d{2}")) {
                    errors.add("Signup date must be in YYYY-MM-DD format");
                }
            } catch (Exception e) {
                errors.add("Invalid signup date format");
            }
        }
        
        return errors;
    }
}