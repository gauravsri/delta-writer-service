package com.example.deltastore.util;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class DefaultColumnarBatchTest {

    private StructType schema;
    private List<ColumnVector> vectors;
    private DefaultColumnarBatch batch;

    @BeforeEach
    void setUp() {
        schema = new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING)
            .add("active", BooleanType.BOOLEAN);

        DefaultColumnVector idVector = new DefaultColumnVector(
            IntegerType.INTEGER, 
            List.of(1, 2, 3), 
            new boolean[]{false, false, false}
        );
        DefaultColumnVector nameVector = new DefaultColumnVector(
            StringType.STRING, 
            List.of("Alice", "Bob", "Charlie"), 
            new boolean[]{false, false, false}
        );
        DefaultColumnVector activeVector = new DefaultColumnVector(
            BooleanType.BOOLEAN, 
            List.of(true, false, true), 
            new boolean[]{false, false, false}
        );

        vectors = List.of(idVector, nameVector, activeVector);
        batch = new DefaultColumnarBatch(schema, vectors);
    }

    @Test
    void testGetSize() {
        assertEquals(3, batch.getSize());
    }

    @Test
    void testGetSchema() {
        assertEquals(schema, batch.getSchema());
        assertEquals(3, batch.getSchema().length());
    }

    @Test
    void testGetColumnVector() {
        ColumnVector idVector = batch.getColumnVector(0);
        assertNotNull(idVector);
        assertEquals(IntegerType.INTEGER, idVector.getDataType());
        assertEquals(3, idVector.getSize());

        ColumnVector nameVector = batch.getColumnVector(1);
        assertNotNull(nameVector);
        assertEquals(StringType.STRING, nameVector.getDataType());

        ColumnVector activeVector = batch.getColumnVector(2);
        assertNotNull(activeVector);
        assertEquals(BooleanType.BOOLEAN, activeVector.getDataType());
    }

    @Test
    void testGetColumnVectorOutOfBounds() {
        assertThrows(IndexOutOfBoundsException.class, () -> batch.getColumnVector(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.getColumnVector(3));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.getColumnVector(10));
    }

    @Test
    void testEmptyBatch() {
        StructType emptySchema = new StructType();
        List<ColumnVector> emptyVectors = List.of();
        DefaultColumnarBatch emptyBatch = new DefaultColumnarBatch(emptySchema, emptyVectors);

        assertEquals(0, emptyBatch.getSize());
        assertEquals(0, emptyBatch.getSchema().length());
        assertThrows(IndexOutOfBoundsException.class, () -> emptyBatch.getColumnVector(0));
    }

    @Test
    void testSingleColumnBatch() {
        StructType singleSchema = new StructType().add("count", IntegerType.INTEGER);
        DefaultColumnVector singleVector = new DefaultColumnVector(
            IntegerType.INTEGER, 
            List.of(42, 84, 126), 
            new boolean[]{false, false, false}
        );
        List<ColumnVector> singleVectorList = List.of(singleVector);
        
        DefaultColumnarBatch singleBatch = new DefaultColumnarBatch(singleSchema, singleVectorList);

        assertEquals(3, singleBatch.getSize());
        assertEquals(1, singleBatch.getSchema().length());
        
        ColumnVector vector = singleBatch.getColumnVector(0);
        assertEquals(IntegerType.INTEGER, vector.getDataType());
        assertEquals(3, vector.getSize());
    }

    @Test
    void testBatchWithNullValues() {
        StructType nullSchema = new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING);

        DefaultColumnVector idVectorWithNulls = new DefaultColumnVector(
            IntegerType.INTEGER, 
            List.of(1, null, 3), 
            new boolean[]{false, true, false}
        );
        DefaultColumnVector nameVectorWithNulls = new DefaultColumnVector(
            StringType.STRING, 
            List.of("Alice", null, "Charlie"), 
            new boolean[]{false, true, false}
        );

        List<ColumnVector> nullVectors = List.of(idVectorWithNulls, nameVectorWithNulls);
        DefaultColumnarBatch nullBatch = new DefaultColumnarBatch(nullSchema, nullVectors);

        assertEquals(3, nullBatch.getSize());
        
        ColumnVector idVector = nullBatch.getColumnVector(0);
        assertFalse(idVector.isNullAt(0));
        assertTrue(idVector.isNullAt(1));
        assertFalse(idVector.isNullAt(2));
        
        ColumnVector nameVector = nullBatch.getColumnVector(1);
        assertFalse(nameVector.isNullAt(0));
        assertTrue(nameVector.isNullAt(1));
        assertFalse(nameVector.isNullAt(2));
    }

    @Test
    void testConstructorWithNullSchema() {
        assertThrows(IllegalArgumentException.class, () -> 
            new DefaultColumnarBatch(null, vectors));
    }

    @Test
    void testConstructorWithNullVectors() {
        assertThrows(IllegalArgumentException.class, () -> 
            new DefaultColumnarBatch(schema, null));
    }

    @Test
    void testConstructorWithMismatchedSchemaAndVectors() {
        StructType mismatchedSchema = new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING); // Only 2 fields but vectors has 3

        assertThrows(IllegalArgumentException.class, () -> 
            new DefaultColumnarBatch(mismatchedSchema, vectors));
    }

    @Test
    void testBatchConsistency() {
        // Verify all vectors have the same size
        for (int i = 0; i < batch.getSchema().length(); i++) {
            ColumnVector vector = batch.getColumnVector(i);
            assertEquals(batch.getSize(), vector.getSize());
        }
    }

    @Test
    void testSchemaFieldMapping() {
        assertEquals("id", schema.at(0).getName());
        assertEquals("name", schema.at(1).getName());
        assertEquals("active", schema.at(2).getName());
        
        assertEquals(IntegerType.INTEGER, schema.at(0).getDataType());
        assertEquals(StringType.STRING, schema.at(1).getDataType());
        assertEquals(BooleanType.BOOLEAN, schema.at(2).getDataType());
    }

    @Test
    void testVectorDataAccess() {
        ColumnVector idVector = batch.getColumnVector(0);
        assertEquals(1, idVector.getInt(0));
        assertEquals(2, idVector.getInt(1));
        assertEquals(3, idVector.getInt(2));

        ColumnVector nameVector = batch.getColumnVector(1);
        assertEquals("Alice", nameVector.getString(0));
        assertEquals("Bob", nameVector.getString(1));
        assertEquals("Charlie", nameVector.getString(2));

        ColumnVector activeVector = batch.getColumnVector(2);
        assertTrue(activeVector.getBoolean(0));
        assertFalse(activeVector.getBoolean(1));
        assertTrue(activeVector.getBoolean(2));
    }
}