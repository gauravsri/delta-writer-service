package com.example.deltastore.util;

import io.delta.kernel.types.StringType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.BooleanType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.List;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class DefaultColumnVectorTest {

    private DefaultColumnVector stringVector;
    private DefaultColumnVector intVector;
    private DefaultColumnVector boolVector;

    @BeforeEach
    void setUp() {
        List<Object> stringData = new ArrayList<>();
        stringData.add("value1");
        stringData.add("value2");
        stringData.add(null);
        stringData.add("value4");
        stringVector = new DefaultColumnVector(StringType.STRING, stringData, new boolean[]{false, false, true, false});

        List<Object> intData = new ArrayList<>();
        intData.add(1);
        intData.add(2);
        intData.add(null);
        intData.add(4);
        intData.add(5);
        intVector = new DefaultColumnVector(IntegerType.INTEGER, intData, new boolean[]{false, false, true, false, false});

        List<Object> boolData = new ArrayList<>();
        boolData.add(true);
        boolData.add(false);
        boolData.add(null);
        boolVector = new DefaultColumnVector(BooleanType.BOOLEAN, boolData, new boolean[]{false, false, true});
    }

    @Test
    void testGetSize() {
        assertEquals(4, stringVector.getSize());
        assertEquals(5, intVector.getSize());
        assertEquals(3, boolVector.getSize());
    }

    @Test
    void testGetDataType() {
        assertEquals(StringType.STRING, stringVector.getDataType());
        assertEquals(IntegerType.INTEGER, intVector.getDataType());
        assertEquals(BooleanType.BOOLEAN, boolVector.getDataType());
    }

    @Test
    void testIsNullAt() {
        assertFalse(stringVector.isNullAt(0)); // "value1"
        assertFalse(stringVector.isNullAt(1)); // "value2"
        assertTrue(stringVector.isNullAt(2));  // null
        assertFalse(stringVector.isNullAt(3)); // "value4"

        assertTrue(intVector.isNullAt(2));     // null
        assertFalse(intVector.isNullAt(0));    // 1
        
        assertTrue(boolVector.isNullAt(2));    // null
        assertFalse(boolVector.isNullAt(0));   // true
    }

    @Test
    void testGetString() {
        assertEquals("value1", stringVector.getString(0));
        assertEquals("value2", stringVector.getString(1));
        assertEquals("value4", stringVector.getString(3));
        
        assertThrows(NullPointerException.class, () -> stringVector.getString(2));
    }

    @Test
    void testGetInt() {
        assertEquals(1, intVector.getInt(0));
        assertEquals(2, intVector.getInt(1));
        assertEquals(4, intVector.getInt(3));
        assertEquals(5, intVector.getInt(4));
        
        assertThrows(NullPointerException.class, () -> intVector.getInt(2));
    }

    @Test
    void testGetBoolean() {
        assertTrue(boolVector.getBoolean(0));
        assertFalse(boolVector.getBoolean(1));
        
        assertThrows(NullPointerException.class, () -> boolVector.getBoolean(2));
    }

    @Test
    void testGetValueViaGetters() {
        // Test specific getters instead of getValue
        assertEquals("value1", stringVector.getString(0));
        assertEquals(1, intVector.getInt(0));
        assertEquals(true, boolVector.getBoolean(0));
        
        // Test null values throw exceptions for specific getters
        assertThrows(IllegalStateException.class, () -> stringVector.getString(2));
        assertThrows(IllegalStateException.class, () -> intVector.getInt(2));
        assertThrows(IllegalStateException.class, () -> boolVector.getBoolean(2));
    }

    @Test
    void testIndexOutOfBounds() {
        assertThrows(IndexOutOfBoundsException.class, () -> stringVector.isNullAt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> stringVector.isNullAt(4));
        assertThrows(IllegalStateException.class, () -> intVector.getInt(5));
        assertThrows(IndexOutOfBoundsException.class, () -> boolVector.getBoolean(3));
    }

    @Test
    void testEmptyVector() {
        DefaultColumnVector emptyVector = new DefaultColumnVector(StringType.STRING, List.of(), new boolean[0]);
        assertEquals(0, emptyVector.getSize());
        assertThrows(IndexOutOfBoundsException.class, () -> emptyVector.isNullAt(0));
    }

    @Test
    void testSingleElementVector() {
        DefaultColumnVector singleVector = new DefaultColumnVector(StringType.STRING, List.of("single"), new boolean[]{false});
        assertEquals(1, singleVector.getSize());
        assertEquals("single", singleVector.getString(0));
        assertFalse(singleVector.isNullAt(0));
    }

    @Test
    void testAllNullVector() {
        List<Object> allNullData = List.of(null, null, null);
        DefaultColumnVector allNullVector = new DefaultColumnVector(StringType.STRING, allNullData, new boolean[]{true, true, true});
        
        assertEquals(3, allNullVector.getSize());
        assertTrue(allNullVector.isNullAt(0));
        assertTrue(allNullVector.isNullAt(1));
        assertTrue(allNullVector.isNullAt(2));
    }

    @Test
    void testConstructorWithNullDataType() {
        assertThrows(IllegalArgumentException.class, () -> 
            new DefaultColumnVector(null, List.of("data"), new boolean[1]));
    }

    @Test
    void testConstructorWithNullData() {
        assertThrows(IllegalArgumentException.class, () -> 
            new DefaultColumnVector(StringType.STRING, null, new boolean[1]));
    }

    @Test
    void testUnsupportedOperations() {
        // Test methods that should throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getByte(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getShort(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getLong(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getFloat(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getDouble(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getBinary(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getDecimal(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getMap(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getArray(0));
        assertThrows(UnsupportedOperationException.class, () -> stringVector.getChild(0));
    }
}