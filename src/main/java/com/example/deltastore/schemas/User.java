package com.example.deltastore.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * User schema for Delta Lake operations
 * This class represents the structure of user data stored in Delta Lake
 */
@Data
public class User {
    
    @NotBlank(message = "User ID cannot be blank")
    @Size(max = 100, message = "User ID must be less than 100 characters")
    @JsonProperty("user_id")
    private String userId;
    
    @NotBlank(message = "Username cannot be blank")
    @Size(max = 50, message = "Username must be less than 50 characters")
    @JsonProperty("username")
    private String username;
    
    @Email(message = "Email should be valid")
    @Size(max = 100, message = "Email must be less than 100 characters")
    @JsonProperty("email")
    private String email;
    
    @NotBlank(message = "Country cannot be blank")
    @Size(max = 50, message = "Country must be less than 50 characters")
    @JsonProperty("country")
    private String country;
    
    @NotNull(message = "Signup date cannot be null")
    @JsonProperty("signup_date")
    private String signupDate;
}