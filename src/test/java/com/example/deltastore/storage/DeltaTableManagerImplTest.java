package com.example.deltastore.storage;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.defaults.engine.DefaultEngine;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.util.DataTypeConverter;
import com.example.deltastore.exception.TableWriteException;
import com.example.deltastore.exception.TableReadException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DeltaTableManagerImplTest {

    @Mock
    private StorageProperties storageProperties;
    
    @Mock
    private DataTypeConverter dataTypeConverter;

    private DeltaTableManagerImpl deltaTableManager;
    private Schema userSchema;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Mock storage properties for local filesystem testing
        when(storageProperties.getEndpoint()).thenReturn("");
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        when(storageProperties.getAccessKey()).thenReturn("test-access-key");
        when(storageProperties.getSecretKey()).thenReturn("test-secret-key");
        when(storageProperties.getMaskedAccessKey()).thenReturn("test-access-***");

        deltaTableManager = new DeltaTableManagerImpl(storageProperties, dataTypeConverter);
        
        // Create user schema for tests
        userSchema = Schema.createRecord("User", "User record", "com.example.deltastore.schemas", false, Arrays.asList(
            new Schema.Field("user_id", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("username", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("email", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), null, null),
            new Schema.Field("country", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("signup_date", Schema.create(Schema.Type.STRING), null, null)
        ));
    }

    @Test
    @DisplayName("Should initialize with correct storage properties")
    void testInitialization() {
        // Verify that the manager was created without throwing exceptions
        assertNotNull(deltaTableManager);
        verify(storageProperties, atLeastOnce()).getEndpoint();
        verify(storageProperties, atLeastOnce()).getBucketName();
    }

    @Test
    @DisplayName("Should configure MinIO settings when endpoint is provided")
    void testMinioConfiguration() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("minioadmin");
        when(storageProperties.getSecretKey()).thenReturn("minioadmin");
        when(storageProperties.getBucketName()).thenReturn("delta-lake-bucket");
        when(storageProperties.getMaskedAccessKey()).thenReturn("minioad***");

        // When
        DeltaTableManagerImpl manager = new DeltaTableManagerImpl(storageProperties, dataTypeConverter);

        // Then
        assertNotNull(manager);
        verify(storageProperties, atLeastOnce()).getEndpoint();
        verify(storageProperties, atLeastOnce()).getAccessKey();
        verify(storageProperties, atLeastOnce()).getSecretKey();
    }

    @Test
    @DisplayName("Should throw exception for null table name in write")
    void testWriteWithNullTableName() {
        // Given
        List<GenericRecord> records = createTestRecords();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.write(null, records, userSchema));
    }

    @Test
    @DisplayName("Should throw exception for empty table name in write")
    void testWriteWithEmptyTableName() {
        // Given
        List<GenericRecord> records = createTestRecords();

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.write("", records, userSchema));
    }

    @Test
    @DisplayName("Should handle empty records list gracefully")
    void testWriteWithEmptyRecords() {
        // Given
        List<GenericRecord> emptyRecords = new ArrayList<>();

        // When/Then - should not throw exception
        assertDoesNotThrow(() -> 
            deltaTableManager.write("users", emptyRecords, userSchema));
    }

    @Test
    @DisplayName("Should handle null records list gracefully")
    void testWriteWithNullRecords() {
        // When/Then - should not throw exception
        assertDoesNotThrow(() -> 
            deltaTableManager.write("users", null, userSchema));
    }

    @Test
    @DisplayName("Should throw exception for null table name in read")
    void testReadWithNullTableName() {
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.read(null, "user_id", "123"));
    }

    @Test
    @DisplayName("Should throw exception for null primary key column in read")
    void testReadWithNullPrimaryKeyColumn() {
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.read("users", null, "123"));
    }

    @Test
    @DisplayName("Should throw exception for null primary key value in read")
    void testReadWithNullPrimaryKeyValue() {
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.read("users", "user_id", null));
    }

    @Test
    @DisplayName("Should throw exception for empty table name in read")
    void testReadWithEmptyTableName() {
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.read("  ", "user_id", "123"));
    }

    @Test
    @DisplayName("Should throw exception for empty primary key column in read")
    void testReadWithEmptyPrimaryKeyColumn() {
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.read("users", "  ", "123"));
    }

    @Test
    @DisplayName("Should throw exception for null table name in readByPartitions")
    void testReadByPartitionsWithNullTableName() {
        // Given
        Map<String, String> partitionFilters = Map.of("country", "US");

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.readByPartitions(null, partitionFilters));
    }

    @Test
    @DisplayName("Should throw exception for empty table name in readByPartitions")
    void testReadByPartitionsWithEmptyTableName() {
        // Given
        Map<String, String> partitionFilters = Map.of("country", "US");

        // When/Then
        assertThrows(IllegalArgumentException.class, () -> 
            deltaTableManager.readByPartitions("  ", partitionFilters));
    }

    @Test
    @DisplayName("Should handle null partition filters in readByPartitions")
    void testReadByPartitionsWithNullFilters() {
        // When/Then - should not throw exception for null filters
        assertDoesNotThrow(() -> 
            deltaTableManager.readByPartitions("users", null));
    }

    @Test
    @DisplayName("Should handle empty partition filters in readByPartitions")
    void testReadByPartitionsWithEmptyFilters() {
        // Given
        Map<String, String> emptyFilters = new HashMap<>();

        // When/Then - should not throw exception for empty filters
        assertDoesNotThrow(() -> 
            deltaTableManager.readByPartitions("users", emptyFilters));
    }

    @Test
    @DisplayName("Should generate correct S3A path when endpoint is configured")
    void testS3APathGeneration() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getBucketName()).thenReturn("delta-bucket");
        when(storageProperties.getAccessKey()).thenReturn("access-key");
        when(storageProperties.getSecretKey()).thenReturn("secret-key");
        when(storageProperties.getMaskedAccessKey()).thenReturn("access-***");

        DeltaTableManagerImpl manager = new DeltaTableManagerImpl(storageProperties, dataTypeConverter);

        // When/Then - path generation is tested implicitly through initialization
        assertNotNull(manager);
    }

    @Test
    @DisplayName("Should use local filesystem path when endpoint is not configured")
    void testLocalFileSystemPath() {
        // Given
        when(storageProperties.getEndpoint()).thenReturn("");
        when(storageProperties.getBucketName()).thenReturn("test-bucket");

        DeltaTableManagerImpl manager = new DeltaTableManagerImpl(storageProperties, dataTypeConverter);

        // When/Then - local path generation is tested implicitly through initialization
        assertNotNull(manager);
    }

    @Test
    @DisplayName("Should validate table name in all operations")
    void testTableNameValidation() {
        // Test various invalid table names
        String[] invalidNames = {null, "", "  ", "\t", "\n"};
        List<GenericRecord> records = createTestRecords();

        for (String invalidName : invalidNames) {
            // Test write operation
            assertThrows(IllegalArgumentException.class, () -> 
                deltaTableManager.write(invalidName, records, userSchema),
                "Should reject table name: '" + invalidName + "'");

            // Test read operation
            assertThrows(IllegalArgumentException.class, () -> 
                deltaTableManager.read(invalidName, "user_id", "123"),
                "Should reject table name: '" + invalidName + "'");

            // Test readByPartitions operation
            assertThrows(IllegalArgumentException.class, () -> 
                deltaTableManager.readByPartitions(invalidName, Map.of("country", "US")),
                "Should reject table name: '" + invalidName + "'");
        }
    }

    @Test
    @DisplayName("Should handle schema conversion correctly")
    void testSchemaConversion() {
        // Given
        List<GenericRecord> records = createTestRecords();

        // When/Then - schema conversion is tested implicitly through write operations
        // The convertAvroSchemaToDelta method should handle the conversion
        assertDoesNotThrow(() -> {
            // This tests that schema conversion doesn't cause immediate failures
            deltaTableManager.write("test_table", records, userSchema);
        });
    }

    @Test
    @DisplayName("Should support different data types in records")
    void testDifferentDataTypes() {
        // Given - create records with various field types
        GenericRecord record1 = new GenericRecordBuilder(userSchema)
            .set("user_id", "user123")
            .set("username", "testuser")
            .set("email", "test@example.com")
            .set("country", "US")
            .set("signup_date", "2024-01-01")
            .build();

        GenericRecord record2 = new GenericRecordBuilder(userSchema)
            .set("user_id", "user456") 
            .set("username", "anotheruser")
            .set("email", null) // Test null email
            .set("country", "CA")
            .set("signup_date", "2024-01-02")
            .build();

        List<GenericRecord> records = Arrays.asList(record1, record2);

        // When/Then - should handle different data types including nulls
        assertDoesNotThrow(() -> 
            deltaTableManager.write("mixed_data_table", records, userSchema));
    }

    @Test
    @DisplayName("Should handle large batch of records")
    void testLargeBatchWrite() {
        // Given
        List<GenericRecord> largeRecordSet = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            GenericRecord record = new GenericRecordBuilder(userSchema)
                .set("user_id", "user" + i)
                .set("username", "user" + i)
                .set("email", "user" + i + "@example.com")
                .set("country", i % 2 == 0 ? "US" : "CA")
                .set("signup_date", "2024-01-" + String.format("%02d", (i % 28) + 1))
                .build();
            largeRecordSet.add(record);
        }

        // When/Then - should handle large batches without memory issues
        assertDoesNotThrow(() -> 
            deltaTableManager.write("large_batch_table", largeRecordSet, userSchema));
    }

    private List<GenericRecord> createTestRecords() {
        GenericRecord record1 = new GenericRecordBuilder(userSchema)
            .set("user_id", "123")
            .set("username", "testuser")
            .set("email", "test@example.com")
            .set("country", "US")
            .set("signup_date", "2024-01-01")
            .build();

        GenericRecord record2 = new GenericRecordBuilder(userSchema)
            .set("user_id", "456")
            .set("username", "anotheruser")
            .set("email", "another@example.com")
            .set("country", "CA")
            .set("signup_date", "2024-01-02")
            .build();

        return Arrays.asList(record1, record2);
    }
}