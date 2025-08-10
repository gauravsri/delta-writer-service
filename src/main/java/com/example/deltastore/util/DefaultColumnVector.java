package com.example.deltastore.util;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Complete Delta Kernel ColumnVector implementation following kernel_help.md
 */
public class DefaultColumnVector implements ColumnVector {
    private final DataType dataType;
    private final List<Object> data;
    private final boolean[] nulls;
    
    public DefaultColumnVector(DataType dataType, List<Object> data, boolean[] nulls) {
        this.dataType = dataType;
        this.data = new ArrayList<>(data);
        this.nulls = nulls != null ? Arrays.copyOf(nulls, nulls.length) : new boolean[data.size()];
    }
    
    @Override
    public DataType getDataType() {
        return dataType;
    }
    
    @Override
    public int getSize() {
        return data.size();
    }
    
    @Override
    public boolean isNullAt(int rowId) {
        return rowId >= 0 && rowId < nulls.length && nulls[rowId];
    }
    
    @Override
    public boolean getBoolean(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return (Boolean) data.get(rowId);
    }
    
    @Override
    public byte getByte(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).byteValue();
    }
    
    @Override
    public short getShort(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).shortValue();
    }
    
    @Override
    public int getInt(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).intValue();
    }
    
    @Override
    public long getLong(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).longValue();
    }
    
    @Override
    public float getFloat(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).floatValue();
    }
    
    @Override
    public double getDouble(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return ((Number) data.get(rowId)).doubleValue();
    }
    
    @Override
    public String getString(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return (String) data.get(rowId);
    }
    
    @Override
    public byte[] getBinary(int rowId) {
        if (isNullAt(rowId)) throw new IllegalStateException("Value is null at " + rowId);
        return (byte[]) data.get(rowId);
    }
    
    @Override
    public ColumnVector getChild(int ordinal) {
        // For struct/array types - simplified implementation
        throw new UnsupportedOperationException("Complex types not implemented");
    }
    
    public ColumnVector slice(int start, int end) {
        List<Object> slicedData = data.subList(start, end);
        boolean[] slicedNulls = Arrays.copyOfRange(nulls, start, end);
        return new DefaultColumnVector(dataType, slicedData, slicedNulls);
    }
    
    @Override
    public void close() {
        // Cleanup if needed
    }
}