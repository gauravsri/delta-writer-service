package com.example.deltastore.service;

import com.example.deltastore.schemas.User;
import com.example.deltastore.storage.DeltaTableManager;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private static final String TABLE_NAME = "users";
    private static final String PRIMARY_KEY_COLUMN = "user_id";

    private final DeltaTableManager deltaTableManager;

    @Override
    public void save(User user) {
        // The Avro-generated User class is already a GenericRecord.
        deltaTableManager.write(TABLE_NAME, Collections.singletonList(user), user.getSchema());
    }

    @Override
    public Optional<User> findById(String userId) {
        return deltaTableManager.read(TABLE_NAME, PRIMARY_KEY_COLUMN, userId)
                .map(this::mapToUser);
    }

    @Override
    public List<User> findByPartitions(Map<String, String> partitionFilters) {
        return deltaTableManager.readByPartitions(TABLE_NAME, partitionFilters).stream()
                .map(this::mapToUser)
                .collect(Collectors.toList());
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
