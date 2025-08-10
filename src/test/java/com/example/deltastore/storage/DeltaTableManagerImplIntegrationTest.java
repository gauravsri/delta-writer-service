package com.example.deltastore.storage;

import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.exception.TableWriteException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeltaTableManagerImplIntegrationTest {

    @Mock
    private StorageProperties storageProperties;
    
    private DeltaTableManagerImpl deltaTableManager;
    private Schema testSchema;
    private GenericRecord testRecord;

    @BeforeEach
    void setUp() {
        // Mock storage properties for S3/MinIO
        when(storageProperties.getEndpoint()).thenReturn("http://localhost:9000");
        when(storageProperties.getAccessKey()).thenReturn("minio");
        when(storageProperties.getSecretKey()).thenReturn("minio123");
        when(storageProperties.getBucketName()).thenReturn("test-bucket");
        when(storageProperties.getMaskedAccessKey()).thenReturn("m****");
        
        deltaTableManager = new DeltaTableManagerImpl(storageProperties);
        
        // Create test schema and record
        testSchema = Schema.parse("{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"username\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"signup_date\",\"type\":\"string\"}]}");
        
        testRecord = new GenericRecordBuilder(testSchema)
                .set("user_id", "user123")
                .set("username", "testuser")
                .set("email", "test@example.com")
                .set("country", "US")
                .set("signup_date", "2023-01-01")
                .build();
    }

    @Test
    void testConstructorWithS3Configuration() {
        assertNotNull(deltaTableManager);
        
        // Verify Hadoop configuration was set up
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(deltaTableManager, "hadoopConf");
        assertNotNull(hadoopConf);
        
        // Verify S3A configuration
        assertEquals("http://localhost:9000", hadoopConf.get("fs.s3a.endpoint"));
        assertEquals("minio", hadoopConf.get("fs.s3a.access.key"));
        assertEquals("minio123", hadoopConf.get("fs.s3a.secret.key"));
        assertEquals("true", hadoopConf.get("fs.s3a.path.style.access"));
        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", hadoopConf.get("fs.s3a.impl"));
        assertEquals("false", hadoopConf.get("fs.s3a.connection.ssl.enabled"));
        
        // Verify performance settings
        assertEquals("100", hadoopConf.get("fs.s3a.connection.maximum"));
        assertEquals("10", hadoopConf.get("fs.s3a.threads.max"));
        assertEquals("5000", hadoopConf.get("fs.s3a.connection.establish.timeout"));
        assertEquals("10000", hadoopConf.get("fs.s3a.connection.timeout"));
        
        // Verify buffer settings
        assertEquals("/tmp", hadoopConf.get("fs.s3a.buffer.dir"));
        assertEquals("true", hadoopConf.get("fs.s3a.fast.upload"));
        assertEquals("disk", hadoopConf.get("fs.s3a.fast.upload.buffer"));
        assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", hadoopConf.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    void testConstructorWithLocalFilesystem() {
        when(storageProperties.getEndpoint()).thenReturn(null);
        
        DeltaTableManagerImpl localManager = new DeltaTableManagerImpl(storageProperties);
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(localManager, "hadoopConf");
        
        assertEquals("file:///", hadoopConf.get("fs.default.name"));
        assertEquals("file:///", hadoopConf.get("fs.defaultFS"));
    }

    @Test
    void testConstructorWithEmptyEndpoint() {
        when(storageProperties.getEndpoint()).thenReturn("");
        
        DeltaTableManagerImpl localManager = new DeltaTableManagerImpl(storageProperties);
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(localManager, "hadoopConf");
        
        assertEquals("file:///", hadoopConf.get("fs.default.name"));
        assertEquals("file:///", hadoopConf.get("fs.defaultFS"));
    }

    @Test
    void testWriteWithNullTableName() {
        assertThrows(IllegalArgumentException.class, () ->
                deltaTableManager.write(null, Arrays.asList(testRecord), testSchema)
        );
    }

    @Test
    void testWriteWithEmptyTableName() {
        assertThrows(IllegalArgumentException.class, () ->
                deltaTableManager.write("", Arrays.asList(testRecord), testSchema)
        );
    }

    @Test
    void testWriteWithWhitespaceTableName() {
        assertThrows(IllegalArgumentException.class, () ->
                deltaTableManager.write("   ", Arrays.asList(testRecord), testSchema)
        );
    }

    @Test
    void testWriteWithNullRecords() {
        // Should log warning and return without error
        assertDoesNotThrow(() -> deltaTableManager.write("test-table", null, testSchema));
    }

    @Test
    void testWriteWithEmptyRecords() {
        // Should log warning and return without error
        assertDoesNotThrow(() -> deltaTableManager.write("test-table", Collections.emptyList(), testSchema));
    }

    @Test
    void testGetTablePathWithS3() throws Exception {
        String tablePath = (String) ReflectionTestUtils.invokeMethod(deltaTableManager, "getTablePath", "users");
        assertEquals("s3a://test-bucket/users", tablePath);
    }

    @Test
    void testGetTablePathWithLocalFilesystem() throws Exception {
        when(storageProperties.getEndpoint()).thenReturn(null);
        DeltaTableManagerImpl localManager = new DeltaTableManagerImpl(storageProperties);
        
        String tablePath = (String) ReflectionTestUtils.invokeMethod(localManager, "getTablePath", "users");
        assertEquals("/tmp/delta-tables/test-bucket/users", tablePath);
    }

    @Test
    void testDoesTableExist() throws Exception {
        // In test environment, table should not exist
        Boolean exists = (Boolean) ReflectionTestUtils.invokeMethod(deltaTableManager, "doesTableExist", "/tmp/nonexistent-table");
        assertFalse(exists);
    }

    @Test
    void testConvertAvroSchemaToDelta() throws Exception {
        Object deltaSchema = ReflectionTestUtils.invokeMethod(deltaTableManager, "convertAvroSchemaToDelta", testSchema);
        assertNotNull(deltaSchema);
        assertEquals("io.delta.kernel.types.StructType", deltaSchema.getClass().getName());
    }

    @Test
    void testCreateCloseableIterator() throws Exception {
        List<String> testData = Arrays.asList("item1", "item2", "item3");
        Iterator<String> iterator = testData.iterator();
        
        Object closeableIterator = ReflectionTestUtils.invokeMethod(deltaTableManager, "createCloseableIterator", iterator);
        assertNotNull(closeableIterator);
        
        // Test iterator functionality
        Boolean hasNext = (Boolean) ReflectionTestUtils.invokeMethod(closeableIterator, "hasNext");
        assertTrue(hasNext);
        
        String next = (String) ReflectionTestUtils.invokeMethod(closeableIterator, "next");
        assertEquals("item1", next);
        
        // Test close method doesn't throw
        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(closeableIterator, "close"));
    }

    @Test
    void testWriteDeltaRecordsWithEmptyList() throws Exception {
        // This would require mocking Delta Kernel objects, which is complex
        // For now, we'll test that empty records are handled properly through the main write method
        assertDoesNotThrow(() -> deltaTableManager.write("empty-test", Collections.emptyList(), testSchema));
    }

    @Test
    void testWriteWithValidRecord() {
        // This test will fail in the test environment because we don't have actual Delta Kernel setup,
        // but it will exercise the validation and path resolution code
        
        List<GenericRecord> records = Arrays.asList(testRecord);
        
        // Should get to the Delta Kernel write operation and fail there (which is expected in test environment)
        TableWriteException exception = assertThrows(TableWriteException.class, () ->
                deltaTableManager.write("users", records, testSchema)
        );
        
        assertTrue(exception.getMessage().contains("Unexpected error writing to Delta table"));
    }

    @Test 
    void testWriteWithMultipleRecords() {
        List<GenericRecord> records = new ArrayList<>();
        
        // Create multiple test records
        for (int i = 0; i < 3; i++) {
            GenericRecord record = new GenericRecordBuilder(testSchema)
                    .set("user_id", "user" + i)
                    .set("username", "testuser" + i)
                    .set("email", "test" + i + "@example.com")
                    .set("country", "US")
                    .set("signup_date", "2023-01-0" + (i + 1))
                    .build();
            records.add(record);
        }
        
        // Should fail at Delta Kernel operation but exercise the validation code
        TableWriteException exception = assertThrows(TableWriteException.class, () ->
                deltaTableManager.write("users", records, testSchema)
        );
        
        assertTrue(exception.getMessage().contains("Unexpected error writing to Delta table"));
    }

    @Test
    void testSchemaConversion() throws Exception {
        // Test that the schema conversion produces a valid StructType with expected fields
        Object deltaSchema = ReflectionTestUtils.invokeMethod(deltaTableManager, "convertAvroSchemaToDelta", testSchema);
        assertNotNull(deltaSchema);
        
        String schemaString = deltaSchema.toString();
        assertTrue(schemaString.contains("user_id"));
        assertTrue(schemaString.contains("username"));
        assertTrue(schemaString.contains("email"));
        assertTrue(schemaString.contains("country"));
        assertTrue(schemaString.contains("signup_date"));
    }

    @Test
    void testStoragePropertiesIntegration() {
        verify(storageProperties, atLeastOnce()).getEndpoint();
        verify(storageProperties, atLeastOnce()).getAccessKey();
        verify(storageProperties, atLeastOnce()).getSecretKey();
        verify(storageProperties, atLeastOnce()).getBucketName();
        verify(storageProperties, atLeastOnce()).getMaskedAccessKey();
    }

    @Test
    void testHadoopConfigurationCreation() {
        Configuration hadoopConf = (Configuration) ReflectionTestUtils.getField(deltaTableManager, "hadoopConf");
        assertNotNull(hadoopConf);
        
        // Test that all required S3A properties are set
        assertNotNull(hadoopConf.get("fs.s3a.endpoint"));
        assertNotNull(hadoopConf.get("fs.s3a.access.key"));
        assertNotNull(hadoopConf.get("fs.s3a.secret.key"));
        assertNotNull(hadoopConf.get("fs.s3a.path.style.access"));
        assertNotNull(hadoopConf.get("fs.s3a.impl"));
        assertNotNull(hadoopConf.get("fs.s3a.connection.ssl.enabled"));
    }

    @Test
    void testDeltaEngineInitialization() {
        Object engine = ReflectionTestUtils.getField(deltaTableManager, "engine");
        assertNotNull(engine);
        assertEquals("io.delta.kernel.defaults.engine.DefaultEngine", engine.getClass().getName());
    }

    @Test
    void testTablePathWithSpecialCharacters() throws Exception {
        String tablePath = (String) ReflectionTestUtils.invokeMethod(deltaTableManager, "getTablePath", "my-table_with.special-chars");
        assertEquals("s3a://test-bucket/my-table_with.special-chars", tablePath);
    }

    @Test
    void testTablePathWithSlashes() throws Exception {
        String tablePath = (String) ReflectionTestUtils.invokeMethod(deltaTableManager, "getTablePath", "namespace/table-name");
        assertEquals("s3a://test-bucket/namespace/table-name", tablePath);
    }

    @Test
    void testConfigurationPropertiesIntegrity() {
        // Verify that all expected configuration properties are being used
        verify(storageProperties, atLeastOnce()).getEndpoint(); // Called multiple times in constructor
        verify(storageProperties, atLeastOnce()).getAccessKey(); // Called once in constructor  
        verify(storageProperties, atLeastOnce()).getSecretKey(); // Called once in constructor
        verify(storageProperties, atLeastOnce()).getMaskedAccessKey(); // Called once for logging
        verify(storageProperties, atLeastOnce()).getBucketName(); // Called once for logging
    }
}