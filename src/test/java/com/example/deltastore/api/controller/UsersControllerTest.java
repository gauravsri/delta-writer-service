package com.example.deltastore.api.controller;

import com.example.deltastore.schemas.User;
import com.example.deltastore.service.UserService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(UsersController.class)
class UsersControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserService userService;
    
    @MockBean 
    private com.example.deltastore.validation.UserValidator userValidator;

    @Test
    void whenCreateUser_thenReturnsCreated() throws Exception {
        User newUser = User.newBuilder()
                .setUserId("u1")
                .setUsername("test")
                .setCountry("US")
                .setSignupDate("2024-01-01")
                .setEmail("test@example.com")
                .build();
        when(userValidator.validate(any(User.class))).thenReturn(Collections.emptyList());
        doNothing().when(userService).save(any(User.class));

        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(newUser)))
                .andExpect(status().isCreated());
    }

    @Test
    void whenGetUserById_andUserExists_thenReturnsUser() throws Exception {
        User testUser = User.newBuilder()
                .setUserId("u1")
                .setUsername("test")
                .setCountry("US")
                .setSignupDate("2024-01-01")
                .setEmail("test@example.com")
                .build();
        when(userService.findById("u1")).thenReturn(Optional.of(testUser));

        mockMvc.perform(get("/api/v1/users/u1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.userId").value("u1"));
    }

    @Test
    void whenGetUserById_andUserDoesNotExist_thenReturnsNotFound() throws Exception {
        when(userService.findById("u1")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/v1/users/u1"))
                .andExpect(status().isNotFound());
    }

    @Test
    void whenFindUsersByPartition_thenReturnsUserList() throws Exception {
        User testUser = User.newBuilder()
                .setUserId("u1")
                .setUsername("test")
                .setCountry("US")
                .setSignupDate("2024-01-01")
                .setEmail("test@example.com")
                .build();
        when(userService.findByPartitions(any())).thenReturn(Collections.singletonList(testUser));

        mockMvc.perform(get("/api/v1/users").param("country", "US"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].userId").value("u1"));
    }
}
