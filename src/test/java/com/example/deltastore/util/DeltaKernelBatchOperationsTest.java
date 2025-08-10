package com.example.deltastore.util;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.IntegerType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class DeltaKernelBatchOperationsTest {

    private List<GenericRecord> testRecords;
    private StructType testSchema;

    @BeforeEach
    void setUp() {
        // Create Avro schema
        String avroSchemaJson = """
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
            """;
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
        
        // Create test records
        testRecords = new ArrayList<>();
        
        GenericRecord record1 = new GenericData.Record(avroSchema);
        record1.put("id", 1);
        record1.put("name", "Alice");
        testRecords.add(record1);
        
        GenericRecord record2 = new GenericData.Record(avroSchema);
        record2.put("id", 2);
        record2.put("name", "Bob");
        testRecords.add(record2);
        
        // Create Delta schema
        testSchema = new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING);
    }

    @Test
    void testCreateBatchFromAvroRecords() {
        FilteredColumnarBatch batch = DeltaKernelBatchOperations.createBatchFromAvroRecords(testRecords, testSchema);
        
        assertNotNull(batch);
        
        // Access underlying ColumnarBatch
        ColumnarBatch data = batch.getData();
        assertNotNull(data);
        assertEquals(2, data.getSize());
        assertEquals(testSchema, data.getSchema());
    }

    @Test
    void testCreateBatchFromAvroRecordsWithNulls() {
        // Add a record with null values
        GenericRecord recordWithNull = new GenericData.Record(testRecords.get(0).getSchema());
        recordWithNull.put("id", 3);
        recordWithNull.put("name", null);
        
        List<GenericRecord> recordsWithNull = new ArrayList<>(testRecords);
        recordsWithNull.add(recordWithNull);
        
        FilteredColumnarBatch batch = DeltaKernelBatchOperations.createBatchFromAvroRecords(recordsWithNull, testSchema);
        
        assertNotNull(batch);
        
        ColumnarBatch data = batch.getData();
        assertEquals(3, data.getSize());
        
        // Verify null handling
        assertTrue(data.getColumnVector(1).isNullAt(2)); // name column, 3rd record
    }

    @Test
    void testCreateEmptyBatch() {
        FilteredColumnarBatch emptyBatch = DeltaKernelBatchOperations.createEmptyBatch(testSchema);
        
        assertNotNull(emptyBatch);
        
        ColumnarBatch data = emptyBatch.getData();
        assertEquals(0, data.getSize());
        assertEquals(testSchema, data.getSchema());
        assertEquals(2, data.getSchema().length()); // 2 columns
    }

    @Test
    void testCreateBatchFromEmptyRecords() {
        FilteredColumnarBatch batch = DeltaKernelBatchOperations.createBatchFromAvroRecords(List.of(), testSchema);
        
        assertNotNull(batch);
        
        ColumnarBatch data = batch.getData();
        assertEquals(0, data.getSize());
        assertEquals(testSchema, data.getSchema());
    }

    @Test
    void testInferSchemaFromAvroRecords() {
        StructType inferredSchema = DeltaKernelBatchOperations.inferSchemaFromAvroRecords(testRecords);
        
        assertNotNull(inferredSchema);
        assertEquals(2, inferredSchema.length());
        
        // Check field names and types
        assertEquals("id", inferredSchema.at(0).getName());
        assertEquals(IntegerType.INTEGER, inferredSchema.at(0).getDataType());
        
        assertEquals("name", inferredSchema.at(1).getName());
        assertEquals(StringType.STRING, inferredSchema.at(1).getDataType());
    }

    @Test
    void testInferSchemaFromEmptyRecords() {
        assertThrows(IllegalArgumentException.class, () -> 
            DeltaKernelBatchOperations.inferSchemaFromAvroRecords(List.of()));
    }

    @Test
    void testCreateBatchWithNullRecordsList() {
        assertThrows(RuntimeException.class, () -> 
            DeltaKernelBatchOperations.createBatchFromAvroRecords(null, testSchema));
    }

    @Test
    void testCreateBatchWithNullSchema() {
        assertThrows(RuntimeException.class, () -> 
            DeltaKernelBatchOperations.createBatchFromAvroRecords(testRecords, null));
    }

    @Test
    void testCreateEmptyBatchWithNullSchema() {
        assertThrows(RuntimeException.class, () -> 
            DeltaKernelBatchOperations.createEmptyBatch(null));
    }

    @Test
    void testBatchDataAccess() {
        FilteredColumnarBatch batch = DeltaKernelBatchOperations.createBatchFromAvroRecords(testRecords, testSchema);
        
        ColumnarBatch data = batch.getData();
        
        // Access data through column vectors
        assertEquals(1, data.getColumnVector(0).getInt(0));
        assertEquals(2, data.getColumnVector(0).getInt(1));
        
        assertEquals("Alice", data.getColumnVector(1).getString(0));
        assertEquals("Bob", data.getColumnVector(1).getString(1));
    }
}