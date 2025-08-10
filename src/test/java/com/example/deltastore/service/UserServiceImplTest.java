package com.example.deltastore.service;

import com.example.deltastore.schemas.User;
import com.example.deltastore.storage.DeltaTableManager;
import io.micrometer.core.instrument.Timer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class UserServiceImplTest {

    @Mock
    private DeltaTableManager deltaTableManager;
    
    @Mock
    private com.example.deltastore.metrics.DeltaStoreMetrics metrics;

    @InjectMocks
    private UserServiceImpl userService;

    private User testUser;
    private Map<String, Object> testUserMap;

    @BeforeEach
    void setUp() {
        testUser = User.newBuilder()
                .setUserId("u1")
                .setUsername("test-user")
                .setCountry("US")
                .setSignupDate("2024-01-01")
                .setEmail("test@example.com")
                .build();

        testUserMap = Map.of(
                "user_id", "u1",
                "username", "test-user",
                "country", "US",
                "signup_date", "2024-01-01",
                "email", "test@example.com"
        );
        
        // Setup metrics mocks - using lenient to avoid unnecessary stubbing warnings
        lenient().when(metrics.startWriteTimer()).thenReturn(Timer.start());
        lenient().when(metrics.startReadTimer()).thenReturn(Timer.start());
        lenient().when(metrics.startPartitionReadTimer()).thenReturn(Timer.start());
    }

    @Test
    void whenSaveUser_thenCallsManagerWrite() {
        // When
        userService.save(testUser);

        // Then
        ArgumentCaptor<List<GenericRecord>> recordCaptor = ArgumentCaptor.forClass(List.class);
        verify(deltaTableManager).write(
                eq("users"),
                recordCaptor.capture(),
                eq(testUser.getSchema())
        );

        assertThat(recordCaptor.getValue()).hasSize(1);
        assertThat(recordCaptor.getValue().get(0)).isEqualTo(testUser);
    }

    @Test
    void whenFindById_andUserExists_thenReturnsUser() {
        // Given
        when(deltaTableManager.read("users", "user_id", "u1")).thenReturn(Optional.of(testUserMap));

        // When
        Optional<User> foundUser = userService.findById("u1");

        // Then
        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getUserId()).isEqualTo("u1");
        assertThat(foundUser.get().getUsername()).isEqualTo("test-user");
    }

    @Test
    void whenFindById_andUserDoesNotExist_thenReturnsEmpty() {
        // Given
        when(deltaTableManager.read("users", "user_id", "u1")).thenReturn(Optional.empty());

        // When
        Optional<User> foundUser = userService.findById("u1");

        // Then
        assertThat(foundUser).isEmpty();
    }

    @Test
    void whenFindByPartitions_andUsersExist_returnsUserList() {
        // Given
        when(deltaTableManager.readByPartitions(eq("users"), any(Map.class)))
                .thenReturn(Collections.singletonList(testUserMap));

        // When
        List<User> foundUsers = userService.findByPartitions(Map.of("country", "US"));

        // Then
        assertThat(foundUsers).hasSize(1);
        assertThat(foundUsers.get(0).getUserId()).isEqualTo("u1");
    }

    @Test
    void whenFindByPartitions_andNoUsersExist_returnsEmptyList() {
        // Given
        when(deltaTableManager.readByPartitions(eq("users"), any(Map.class)))
                .thenReturn(Collections.emptyList());

        // When
        List<User> foundUsers = userService.findByPartitions(Map.of("country", "US"));

        // Then
        assertThat(foundUsers).isEmpty();
    }
}
