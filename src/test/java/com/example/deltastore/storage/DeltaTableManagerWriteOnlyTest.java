package com.example.deltastore.storage;

import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.config.DeltaStoreConfiguration;
import com.example.deltastore.schema.DeltaSchemaManager;
import com.example.deltastore.storage.DeltaStoragePathResolver;
import com.example.deltastore.exception.TableWriteException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DeltaTableManagerWriteOnlyTest {

    @Mock
    private StorageProperties storageProperties;

    @Mock
    private DeltaStoreConfiguration config;

    @Mock
    private DeltaSchemaManager schemaManager;

    @Mock
    private DeltaStoragePathResolver pathResolver;

    @Mock
    private GenericRecord record1;

    @Mock 
    private GenericRecord record2;

    @Mock
    private Schema schema;

    private OptimizedDeltaTableManager deltaTableManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Mock the performance configuration
        DeltaStoreConfiguration.Performance performanceConfig = mock(DeltaStoreConfiguration.Performance.class);
        when(performanceConfig.getCommitThreads()).thenReturn(4);
        when(config.getPerformance()).thenReturn(performanceConfig);
        
        deltaTableManager = new OptimizedDeltaTableManager(storageProperties, config, schemaManager, pathResolver);
    }

    @Test
    @DisplayName("Should write records successfully")
    void testWriteRecordsSuccess() {
        // Given
        String tableName = "test_table";
        List<GenericRecord> records = Arrays.asList(record1, record2);
        
        when(storageProperties.getBucketName()).thenReturn("test-bucket");

        // When & Then - Should not throw exception
        assertDoesNotThrow(() -> {
            deltaTableManager.write(tableName, records, schema);
        });
    }

    @Test
    @DisplayName("Should handle null table name")
    void testWriteWithNullTableName() {
        // Given
        List<GenericRecord> records = Arrays.asList(record1);

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            deltaTableManager.write(null, records, schema);
        });
    }

    @Test
    @DisplayName("Should handle empty table name")
    void testWriteWithEmptyTableName() {
        // Given
        List<GenericRecord> records = Arrays.asList(record1);

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            deltaTableManager.write("", records, schema);
        });
    }

    @Test
    @DisplayName("Should handle null records list")
    void testWriteWithNullRecords() {
        // Given
        String tableName = "test_table";

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            deltaTableManager.write(tableName, null, schema);
        });
    }

    @Test
    @DisplayName("Should handle empty records list")
    void testWriteWithEmptyRecords() {
        // Given
        String tableName = "test_table";
        List<GenericRecord> emptyRecords = Arrays.asList();

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            deltaTableManager.write(tableName, emptyRecords, schema);
        });
    }

    @Test
    @DisplayName("Should handle null schema")
    void testWriteWithNullSchema() {
        // Given
        String tableName = "test_table";
        List<GenericRecord> records = Arrays.asList(record1);

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            deltaTableManager.write(tableName, records, null);
        });
    }
}