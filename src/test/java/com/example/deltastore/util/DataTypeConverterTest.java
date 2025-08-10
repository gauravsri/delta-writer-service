package com.example.deltastore.util;

import io.delta.kernel.types.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import io.delta.kernel.data.Row;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataTypeConverterTest {

    private DataTypeConverter converter;

    @Mock
    private Row mockRow;

    @Mock 
    private StructType mockSchema;

    @Mock
    private StructField mockField;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        converter = new DataTypeConverter();
    }

    @Test
    void testRowToMapWithStringField() {
        when(mockSchema.length()).thenReturn(1);
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("name");
        when(mockField.getDataType()).thenReturn(StringType.STRING);
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("test");

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertEquals("test", result.get("name"));
    }

    @Test
    void testRowToMapWithNullField() {
        when(mockSchema.length()).thenReturn(1);
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("name");
        when(mockField.getDataType()).thenReturn(StringType.STRING);
        when(mockRow.isNullAt(0)).thenReturn(true);

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertNull(result.get("name"));
    }

    @Test
    void testRowToMapWithIntegerField() {
        when(mockSchema.length()).thenReturn(1);
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("count");
        when(mockField.getDataType()).thenReturn(IntegerType.INTEGER);
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getInt(0)).thenReturn(42);

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertEquals(42, result.get("count"));
    }

    @Test
    void testConvertToExpectedTypeString() {
        Object result = converter.convertToExpectedType(123, StringType.STRING);
        assertEquals("123", result);
    }

    @Test
    void testConvertToExpectedTypeInteger() {
        Object result = converter.convertToExpectedType("456", IntegerType.INTEGER);
        assertEquals(456, result);
    }

    @Test
    void testConvertToExpectedTypeNull() {
        Object result = converter.convertToExpectedType(null, StringType.STRING);
        assertNull(result);
    }

    @Test
    void testConvertToExpectedTypeLong() {
        Object result = converter.convertToExpectedType(123, LongType.LONG);
        assertEquals(123L, result);
    }

    @Test
    void testConvertToExpectedTypeDouble() {
        Object result = converter.convertToExpectedType(123, DoubleType.DOUBLE);
        assertEquals(123.0, result);
    }

    @Test
    void testConvertToExpectedTypeBoolean() {
        Object result = converter.convertToExpectedType("true", BooleanType.BOOLEAN);
        assertEquals(true, result);
    }

    @Test
    void testRowToMapWithAllDataTypes() {
        // Test with various data types
        when(mockSchema.length()).thenReturn(8);
        
        // String field
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("stringField");
        when(mockField.getDataType()).thenReturn(StringType.STRING);
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("test");
        
        // Float field (index 1)
        StructField floatField = mock(StructField.class);
        when(mockSchema.at(1)).thenReturn(floatField);
        when(floatField.getName()).thenReturn("floatField");
        when(floatField.getDataType()).thenReturn(FloatType.FLOAT);
        when(mockRow.isNullAt(1)).thenReturn(false);
        when(mockRow.getFloat(1)).thenReturn(3.14f);
        
        // Byte field (index 2)
        StructField byteField = mock(StructField.class);
        when(mockSchema.at(2)).thenReturn(byteField);
        when(byteField.getName()).thenReturn("byteField");
        when(byteField.getDataType()).thenReturn(ByteType.BYTE);
        when(mockRow.isNullAt(2)).thenReturn(false);
        when(mockRow.getByte(2)).thenReturn((byte)127);
        
        // Short field (index 3)
        StructField shortField = mock(StructField.class);
        when(mockSchema.at(3)).thenReturn(shortField);
        when(shortField.getName()).thenReturn("shortField");
        when(shortField.getDataType()).thenReturn(ShortType.SHORT);
        when(mockRow.isNullAt(3)).thenReturn(false);
        when(mockRow.getShort(3)).thenReturn((short)1000);
        
        // Date field (index 4)
        StructField dateField = mock(StructField.class);
        when(mockSchema.at(4)).thenReturn(dateField);
        when(dateField.getName()).thenReturn("dateField");
        when(dateField.getDataType()).thenReturn(DateType.DATE);
        when(mockRow.isNullAt(4)).thenReturn(false);
        when(mockRow.getInt(4)).thenReturn(19000); // Days since epoch
        
        // Timestamp field (index 5)
        StructField timestampField = mock(StructField.class);
        when(mockSchema.at(5)).thenReturn(timestampField);
        when(timestampField.getName()).thenReturn("timestampField");
        when(timestampField.getDataType()).thenReturn(TimestampType.TIMESTAMP);
        when(mockRow.isNullAt(5)).thenReturn(false);
        when(mockRow.getLong(5)).thenReturn(1640995200000000L); // Microseconds since epoch
        
        // Binary field (index 6)
        StructField binaryField = mock(StructField.class);
        when(mockSchema.at(6)).thenReturn(binaryField);
        when(binaryField.getName()).thenReturn("binaryField");
        when(binaryField.getDataType()).thenReturn(BinaryType.BINARY);
        when(mockRow.isNullAt(6)).thenReturn(false);
        byte[] binaryData = {1, 2, 3, 4, 5};
        when(mockRow.getBinary(6)).thenReturn(binaryData);
        
        // Unknown type field (index 7) - will fall back to string
        StructField unknownField = mock(StructField.class);
        when(mockSchema.at(7)).thenReturn(unknownField);
        when(unknownField.getName()).thenReturn("unknownField");
        when(unknownField.getDataType()).thenReturn(mock(DataType.class)); // Unknown type
        when(mockRow.isNullAt(7)).thenReturn(false);
        when(mockRow.getString(7)).thenReturn("fallback");

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertEquals("test", result.get("stringField"));
        assertEquals(3.14f, result.get("floatField"));
        assertEquals((byte)127, result.get("byteField"));
        assertEquals((short)1000, result.get("shortField"));
        assertNotNull(result.get("dateField")); // Date formatted as string
        assertNotNull(result.get("timestampField")); // Timestamp formatted as string
        assertArrayEquals(binaryData, (byte[])result.get("binaryField"));
        assertEquals("fallback", result.get("unknownField"));
    }
    
    @Test
    void testRowToMapWithSchemaDataMismatch() {
        // Schema has more fields than row data
        when(mockSchema.length()).thenReturn(2);
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("field1");
        when(mockField.getDataType()).thenReturn(StringType.STRING);
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenReturn("value1");
        
        StructField field2 = mock(StructField.class);
        when(mockSchema.at(1)).thenReturn(field2);
        when(field2.getName()).thenReturn("field2");
        when(field2.getDataType()).thenReturn(StringType.STRING);
        
        // Simulate row.isNullAt(1) throwing exception (column doesn't exist)
        when(mockRow.isNullAt(1)).thenThrow(new IllegalArgumentException("Column 1 does not exist"));

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertEquals("value1", result.get("field1"));
        assertNull(result.get("field2")); // Should be null due to mismatch
    }
    
    @Test
    void testRowToMapWithFieldConversionError() {
        when(mockSchema.length()).thenReturn(1);
        when(mockSchema.at(0)).thenReturn(mockField);
        when(mockField.getName()).thenReturn("errorField");
        when(mockField.getDataType()).thenReturn(StringType.STRING);
        when(mockRow.isNullAt(0)).thenReturn(false);
        when(mockRow.getString(0)).thenThrow(new RuntimeException("Field access error"));

        Map<String, Object> result = converter.rowToMap(mockRow, mockSchema);

        assertNull(result.get("errorField")); // Should be null due to error
    }
    
    @Test
    void testConvertToExpectedTypeWithNumbers() {
        // Test Integer conversion with Number input
        Object result = converter.convertToExpectedType(123L, IntegerType.INTEGER);
        assertEquals(123, result);
        
        // Test Long conversion with Number input
        result = converter.convertToExpectedType(456, LongType.LONG);
        assertEquals(456L, result);
        
        // Test Double conversion with Number input
        result = converter.convertToExpectedType(789, DoubleType.DOUBLE);
        assertEquals(789.0, result);
    }
    
    @Test
    void testConvertToExpectedTypeWithBooleanInput() {
        // Test Boolean with Boolean input
        Object result = converter.convertToExpectedType(true, BooleanType.BOOLEAN);
        assertEquals(true, result);
        
        Object result2 = converter.convertToExpectedType(false, BooleanType.BOOLEAN);
        assertEquals(false, result2);
    }
    
    @Test
    void testConvertToExpectedTypeDecimal() {
        // Test with BigDecimal input
        java.math.BigDecimal decimal = new java.math.BigDecimal("123.456");
        Object result = converter.convertToExpectedType(decimal, new DecimalType(10, 3));
        assertEquals(decimal, result);
        
        // Test with string input
        result = converter.convertToExpectedType("789.123", new DecimalType(10, 3));
        assertEquals(new java.math.BigDecimal("789.123"), result);
    }
    
    @Test
    void testConvertToExpectedTypeUnknownType() {
        // Unknown type should return toString
        Object result = converter.convertToExpectedType(123, mock(DataType.class));
        assertEquals("123", result);
    }
    
    @Test
    void testConvertToExpectedTypeWithInvalidInput() {
        // Invalid integer conversion should return as string
        Object result = converter.convertToExpectedType("invalid", IntegerType.INTEGER);
        assertEquals("invalid", result);
        
        // Invalid double conversion should return as string
        result = converter.convertToExpectedType("not-a-number", DoubleType.DOUBLE);
        assertEquals("not-a-number", result);
    }
}