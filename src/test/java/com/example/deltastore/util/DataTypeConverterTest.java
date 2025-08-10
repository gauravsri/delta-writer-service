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
}