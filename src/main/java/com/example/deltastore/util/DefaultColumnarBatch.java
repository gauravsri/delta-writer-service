package com.example.deltastore.util;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.StructType;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Complete Delta Kernel ColumnarBatch implementation following kernel_help.md
 */
@Slf4j
public class DefaultColumnarBatch implements ColumnarBatch {
    private final StructType schema;
    private final List<ColumnVector> columns;
    private final int size;
    
    public DefaultColumnarBatch(StructType schema, List<ColumnVector> columns) {
        this.schema = schema;
        this.columns = new ArrayList<>(columns);
        this.size = columns.isEmpty() ? 0 : columns.get(0).getSize();
        
        // Validate all columns have same size
        for (ColumnVector column : columns) {
            if (column.getSize() != size) {
                throw new IllegalArgumentException("All columns must have the same size");
            }
        }
    }
    
    @Override
    public StructType getSchema() {
        return schema;
    }
    
    @Override
    public int getSize() {
        return size;
    }
    
    @Override
    public ColumnVector getColumnVector(int ordinal) {
        if (ordinal < 0 || ordinal >= columns.size()) {
            throw new IllegalArgumentException("Invalid ordinal: " + ordinal);
        }
        return columns.get(ordinal);
    }
    
    public ColumnarBatch slice(int start, int end) {
        List<ColumnVector> slicedColumns = new ArrayList<>();
        for (ColumnVector column : columns) {
            // Create sliced column using DefaultColumnVector slice method
            if (column instanceof DefaultColumnVector) {
                slicedColumns.add(((DefaultColumnVector) column).slice(start, end));
            } else {
                slicedColumns.add(column); // Fallback for other implementations
            }
        }
        return new DefaultColumnarBatch(schema, slicedColumns);
    }
    
    public void close() {
        for (ColumnVector column : columns) {
            try {
                column.close();
            } catch (Exception e) {
                log.warn("Failed to close column vector", e);
            }
        }
    }
}