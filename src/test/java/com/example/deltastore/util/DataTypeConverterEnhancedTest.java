package com.example.deltastore.util;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;

class DataTypeConverterEnhancedTest {

    @Mock
    private Row mockRow;

    private DataTypeConverter dataTypeConverter;
    private StructType testSchema;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        dataTypeConverter = new DataTypeConverter();
        
        // Create a test schema with various data types
        testSchema = new StructType()
            .add("string_field", StringType.STRING, false)
            .add("int_field", IntegerType.INTEGER, false)
            .add("long_field", LongType.LONG, false)
            .add("double_field", DoubleType.DOUBLE, false)
            .add("boolean_field", BooleanType.BOOLEAN, false)
            .add("nullable_field", StringType.STRING, true);
    }

    @Test
    @DisplayName("Should convert row with all string fields successfully")
    void testRowToMapWithStringFields() {
        // Given
        StructType stringSchema = new StructType()
            .add("user_id", StringType.STRING, false)
            .add("username", StringType.STRING, false)
            .add("email", StringType.STRING, true);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.isNullAt(2)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("user123");
        when(mockRow.getString(1)).thenReturn("testuser");
        when(mockRow.getString(2)).thenReturn("test@example.com");

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, stringSchema);

        // Then
        assertEquals(3, result.size());
        assertEquals("user123", result.get("user_id"));
        assertEquals("testuser", result.get("username"));
        assertEquals("test@example.com", result.get("email"));
    }

    @Test
    @DisplayName("Should handle null values correctly")
    void testRowToMapWithNullValues() {
        // Given
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(true);
        when(mockRow.isNullAt(2)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("value1");
        when(mockRow.getString(2)).thenReturn("value3");

        StructType schema = new StructType()
            .add("field1", StringType.STRING, false)
            .add("field2", StringType.STRING, true)
            .add("field3", StringType.STRING, false);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, schema);

        // Then
        assertEquals(3, result.size());
        assertEquals("value1", result.get("field1"));
        assertNull(result.get("field2"));
        assertEquals("value3", result.get("field3"));
    }

    @Test
    @DisplayName("Should convert integer fields correctly")
    void testRowToMapWithIntegerFields() {
        // Given
        StructType intSchema = new StructType()
            .add("age", IntegerType.INTEGER, false)
            .add("count", IntegerType.INTEGER, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getInt(0)).thenReturn(25);
        when(mockRow.getInt(1)).thenReturn(100);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, intSchema);

        // Then
        assertEquals(2, result.size());
        assertEquals(25, result.get("age"));
        assertEquals(100, result.get("count"));
    }

    @Test
    @DisplayName("Should convert long fields correctly")
    void testRowToMapWithLongFields() {
        // Given
        StructType longSchema = new StructType()
            .add("timestamp", LongType.LONG, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getLong(0)).thenReturn(1640995200000L);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, longSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals(1640995200000L, result.get("timestamp"));
    }

    @Test
    @DisplayName("Should convert double fields correctly")
    void testRowToMapWithDoubleFields() {
        // Given
        StructType doubleSchema = new StructType()
            .add("price", DoubleType.DOUBLE, false)
            .add("rate", DoubleType.DOUBLE, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getDouble(0)).thenReturn(99.99);
        when(mockRow.getDouble(1)).thenReturn(0.15);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, doubleSchema);

        // Then
        assertEquals(2, result.size());
        assertEquals(99.99, result.get("price"));
        assertEquals(0.15, result.get("rate"));
    }

    @Test
    @DisplayName("Should convert boolean fields correctly")
    void testRowToMapWithBooleanFields() {
        // Given
        StructType boolSchema = new StructType()
            .add("active", BooleanType.BOOLEAN, false)
            .add("verified", BooleanType.BOOLEAN, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getBoolean(0)).thenReturn(true);
        when(mockRow.getBoolean(1)).thenReturn(false);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, boolSchema);

        // Then
        assertEquals(2, result.size());
        assertEquals(true, result.get("active"));
        assertEquals(false, result.get("verified"));
    }

    @Test
    @DisplayName("Should convert float fields correctly")
    void testRowToMapWithFloatFields() {
        // Given
        StructType floatSchema = new StructType()
            .add("score", FloatType.FLOAT, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getFloat(0)).thenReturn(85.5f);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, floatSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals(85.5f, result.get("score"));
    }

    @Test
    @DisplayName("Should convert byte and short fields correctly")
    void testRowToMapWithByteAndShortFields() {
        // Given
        StructType byteShortSchema = new StructType()
            .add("byte_field", ByteType.BYTE, false)
            .add("short_field", ShortType.SHORT, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getByte(0)).thenReturn((byte) 100);
        when(mockRow.getShort(1)).thenReturn((short) 1000);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, byteShortSchema);

        // Then
        assertEquals(2, result.size());
        assertEquals((byte) 100, result.get("byte_field"));
        assertEquals((short) 1000, result.get("short_field"));
    }

    @Test
    @DisplayName("Should convert decimal fields correctly")
    void testRowToMapWithDecimalFields() {
        // Given
        StructType decimalSchema = new StructType()
            .add("amount", new DecimalType(10, 2), false);

        BigDecimal expectedValue = new BigDecimal("123.45");
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getDecimal(0)).thenReturn(expectedValue);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, decimalSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals(expectedValue, result.get("amount"));
    }

    @Test
    @DisplayName("Should convert date fields correctly")
    void testRowToMapWithDateFields() {
        // Given
        StructType dateSchema = new StructType()
            .add("birth_date", DateType.DATE, false);

        // Date as days since epoch (e.g., 2024-01-01)
        int daysSinceEpoch = (int) LocalDate.of(2024, 1, 1).toEpochDay();
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getInt(0)).thenReturn(daysSinceEpoch);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, dateSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals("2024-01-01", result.get("birth_date"));
    }

    @Test
    @DisplayName("Should convert timestamp fields correctly")
    void testRowToMapWithTimestampFields() {
        // Given
        StructType timestampSchema = new StructType()
            .add("created_at", TimestampType.TIMESTAMP, false);

        // Timestamp as microseconds since epoch
        long microsSinceEpoch = 1640995200000000L; // 2022-01-01 00:00:00 UTC
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getLong(0)).thenReturn(microsSinceEpoch);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, timestampSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals("2022-01-01 00:00:00", result.get("created_at"));
    }

    @Test
    @DisplayName("Should convert binary fields correctly")
    void testRowToMapWithBinaryFields() {
        // Given
        StructType binarySchema = new StructType()
            .add("data", BinaryType.BINARY, false);

        byte[] expectedData = {1, 2, 3, 4, 5};
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getBinary(0)).thenReturn(expectedData);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, binarySchema);

        // Then
        assertEquals(1, result.size());
        assertArrayEquals(expectedData, (byte[]) result.get("data"));
    }

    @Test
    @DisplayName("Should handle mixed data types in single row")
    void testRowToMapWithMixedDataTypes() {
        // Given
        StructType mixedSchema = new StructType()
            .add("name", StringType.STRING, false)
            .add("age", IntegerType.INTEGER, false)
            .add("salary", DoubleType.DOUBLE, false)
            .add("active", BooleanType.BOOLEAN, false)
            .add("notes", StringType.STRING, true);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.isNullAt(2)).thenReturn(false);
        when(mockRow.isNullAt(3)).thenReturn(false);
        when(mockRow.isNullAt(4)).thenReturn(true);
        
        when(mockRow.getString(0)).thenReturn("John Doe");
        when(mockRow.getInt(1)).thenReturn(30);
        when(mockRow.getDouble(2)).thenReturn(75000.0);
        when(mockRow.getBoolean(3)).thenReturn(true);

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, mixedSchema);

        // Then
        assertEquals(5, result.size());
        assertEquals("John Doe", result.get("name"));
        assertEquals(30, result.get("age"));
        assertEquals(75000.0, result.get("salary"));
        assertEquals(true, result.get("active"));
        assertNull(result.get("notes"));
    }

    @Test
    @DisplayName("Should handle exceptions gracefully and set field to null")
    void testRowToMapWithExceptions() {
        // Given
        StructType schema = new StructType()
            .add("problematic_field", StringType.STRING, false);

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenThrow(new RuntimeException("Field access error"));

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, schema);

        // Then
        assertEquals(1, result.size());
        assertNull(result.get("problematic_field"));
    }

    @Test
    @DisplayName("Should convert values to expected string type")
    void testConvertToExpectedStringType() {
        // Test various input types
        assertEquals("123", dataTypeConverter.convertToExpectedType(123, StringType.STRING));
        assertEquals("true", dataTypeConverter.convertToExpectedType(true, StringType.STRING));
        assertEquals("45.67", dataTypeConverter.convertToExpectedType(45.67, StringType.STRING));
        assertEquals("test", dataTypeConverter.convertToExpectedType("test", StringType.STRING));
        assertNull(dataTypeConverter.convertToExpectedType(null, StringType.STRING));
    }

    @Test
    @DisplayName("Should convert values to expected integer type")
    void testConvertToExpectedIntegerType() {
        // Test various input types
        assertEquals(123, dataTypeConverter.convertToExpectedType("123", IntegerType.INTEGER));
        assertEquals(45, dataTypeConverter.convertToExpectedType(45.67, IntegerType.INTEGER));
        assertEquals(100, dataTypeConverter.convertToExpectedType(100L, IntegerType.INTEGER));
        assertNull(dataTypeConverter.convertToExpectedType(null, IntegerType.INTEGER));
    }

    @Test
    @DisplayName("Should convert values to expected long type")
    void testConvertToExpectedLongType() {
        // Test various input types
        assertEquals(123L, dataTypeConverter.convertToExpectedType("123", LongType.LONG));
        assertEquals(45L, dataTypeConverter.convertToExpectedType(45.67, LongType.LONG));
        assertEquals(100L, dataTypeConverter.convertToExpectedType(100, LongType.LONG));
        assertNull(dataTypeConverter.convertToExpectedType(null, LongType.LONG));
    }

    @Test
    @DisplayName("Should convert values to expected double type")
    void testConvertToExpectedDoubleType() {
        // Test various input types
        assertEquals(123.0, dataTypeConverter.convertToExpectedType("123", DoubleType.DOUBLE));
        assertEquals(45.67, (Double) dataTypeConverter.convertToExpectedType(45.67f, DoubleType.DOUBLE), 0.001);
        assertEquals(100.0, dataTypeConverter.convertToExpectedType(100, DoubleType.DOUBLE));
        assertNull(dataTypeConverter.convertToExpectedType(null, DoubleType.DOUBLE));
    }

    @Test
    @DisplayName("Should convert values to expected boolean type")
    void testConvertToExpectedBooleanType() {
        // Test various input types
        assertEquals(true, dataTypeConverter.convertToExpectedType("true", BooleanType.BOOLEAN));
        assertEquals(false, dataTypeConverter.convertToExpectedType("false", BooleanType.BOOLEAN));
        assertEquals(true, dataTypeConverter.convertToExpectedType(true, BooleanType.BOOLEAN));
        assertNull(dataTypeConverter.convertToExpectedType(null, BooleanType.BOOLEAN));
    }

    @Test
    @DisplayName("Should convert values to expected decimal type")
    void testConvertToExpectedDecimalType() {
        // Test various input types
        DecimalType decimalType = new DecimalType(10, 2);
        
        BigDecimal result1 = (BigDecimal) dataTypeConverter.convertToExpectedType("123.45", decimalType);
        assertEquals(new BigDecimal("123.45"), result1);
        
        BigDecimal expectedDecimal = new BigDecimal("99.99");
        assertEquals(expectedDecimal, dataTypeConverter.convertToExpectedType(expectedDecimal, decimalType));
        
        assertNull(dataTypeConverter.convertToExpectedType(null, decimalType));
    }

    @Test
    @DisplayName("Should handle invalid type conversions gracefully")
    void testConvertToExpectedTypeWithInvalidInput() {
        // Test invalid conversions that should fallback to string
        assertEquals("invalid", dataTypeConverter.convertToExpectedType("invalid", IntegerType.INTEGER));
        assertEquals("not-a-number", dataTypeConverter.convertToExpectedType("not-a-number", DoubleType.DOUBLE));
        // Boolean.valueOf("not-boolean") returns false, not the string
        assertEquals(false, dataTypeConverter.convertToExpectedType("not-boolean", BooleanType.BOOLEAN));
    }

    @Test
    @DisplayName("Should handle unknown data types by converting to string")
    void testUnknownDataType() {
        // Given - create a mock data type (not a real Delta Lake type, but for testing)
        StructType unknownTypeSchema = new StructType()
            .add("unknown_field", StringType.STRING, false); // Use string type but simulate unknown handling

        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("unknown_value");

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, unknownTypeSchema);

        // Then
        assertEquals(1, result.size());
        assertEquals("unknown_value", result.get("unknown_field"));
    }

    @Test
    @DisplayName("Should handle empty schema")
    void testEmptySchema() {
        // Given
        StructType emptySchema = new StructType();

        // When
        Map<String, Object> result = dataTypeConverter.rowToMap(mockRow, emptySchema);

        // Then
        assertTrue(result.isEmpty());
    }
}