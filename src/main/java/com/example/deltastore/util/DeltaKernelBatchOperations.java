package com.example.deltastore.util;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.*;
import org.apache.avro.generic.GenericRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Complete Delta Kernel batch operations following kernel_help.md patterns
 */
@Slf4j
public class DeltaKernelBatchOperations {

    /**
     * Create FilteredColumnarBatch from Avro records using complete Delta Kernel approach
     */
    public static FilteredColumnarBatch createBatchFromAvroRecords(List<GenericRecord> records, StructType schema) {
        if (records.isEmpty()) {
            return createEmptyBatch(schema);
        }

        log.debug("Creating Delta Kernel batch from {} Avro records", records.size());

        List<ColumnVector> columns = new ArrayList<>();
        
        for (int colIndex = 0; colIndex < schema.length(); colIndex++) {
            StructField field = schema.at(colIndex);
            String fieldName = field.getName();
            DataType dataType = field.getDataType();
            
            List<Object> columnData = new ArrayList<>();
            boolean[] nulls = new boolean[records.size()];
            
            for (int rowIndex = 0; rowIndex < records.size(); rowIndex++) {
                Object value = records.get(rowIndex).get(fieldName);
                if (value == null) {
                    nulls[rowIndex] = true;
                    columnData.add(getDefaultValue(dataType));
                } else {
                    nulls[rowIndex] = false;
                    columnData.add(value);
                }
            }
            
            columns.add(new DefaultColumnVector(dataType, columnData, nulls));
        }
        
        ColumnarBatch batch = new DefaultColumnarBatch(schema, columns);
        return new FilteredColumnarBatch(batch, Optional.empty());
    }
    
    /**
     * Create empty FilteredColumnarBatch for schema-only operations
     */
    public static FilteredColumnarBatch createEmptyBatch(StructType schema) {
        List<ColumnVector> emptyColumns = new ArrayList<>();
        
        for (int colIndex = 0; colIndex < schema.length(); colIndex++) {
            StructField field = schema.at(colIndex);
            DataType dataType = field.getDataType();
            
            List<Object> emptyData = new ArrayList<>();
            boolean[] emptyNulls = new boolean[0];
            
            emptyColumns.add(new DefaultColumnVector(dataType, emptyData, emptyNulls));
        }
        
        ColumnarBatch emptyBatch = new DefaultColumnarBatch(schema, emptyColumns);
        return new FilteredColumnarBatch(emptyBatch, Optional.empty());
    }

    /**
     * Infer Delta schema from Avro records
     */
    public static StructType inferSchemaFromAvroRecords(List<GenericRecord> records) {
        if (records.isEmpty()) {
            throw new IllegalArgumentException("Cannot infer schema from empty records");
        }
        
        GenericRecord firstRecord = records.get(0);
        org.apache.avro.Schema avroSchema = firstRecord.getSchema();
        
        StructType schemaBuilder = new StructType();
        
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            String fieldName = field.name();
            DataType dataType = convertAvroTypeToDeltalType(field.schema());
            boolean nullable = isNullable(field.schema());
            schemaBuilder = schemaBuilder.add(fieldName, dataType, nullable);
        }
        
        return schemaBuilder;
    }
    
    private static DataType convertAvroTypeToDeltalType(org.apache.avro.Schema avroSchema) {
        switch (avroSchema.getType()) {
            case STRING:
                return StringType.STRING;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return LongType.LONG;
            case FLOAT:
                return FloatType.FLOAT;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BYTES:
                return BinaryType.BINARY;
            case UNION:
                // Handle nullable types (union with null)
                for (org.apache.avro.Schema unionType : avroSchema.getTypes()) {
                    if (unionType.getType() != org.apache.avro.Schema.Type.NULL) {
                        return convertAvroTypeToDeltalType(unionType);
                    }
                }
                return StringType.STRING; // fallback
            default:
                return StringType.STRING; // fallback for unsupported types
        }
    }
    
    private static boolean isNullable(org.apache.avro.Schema avroSchema) {
        if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            return avroSchema.getTypes().stream()
                .anyMatch(type -> type.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }
    
    private static Object getDefaultValue(DataType dataType) {
        if (dataType instanceof IntegerType) return 0;
        if (dataType instanceof LongType) return 0L;
        if (dataType instanceof DoubleType) return 0.0;
        if (dataType instanceof FloatType) return 0.0f;
        if (dataType instanceof BooleanType) return false;
        if (dataType instanceof StringType) return "";
        if (dataType instanceof BinaryType) return new byte[0];
        return null;
    }

}