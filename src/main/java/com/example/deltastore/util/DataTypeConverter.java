package com.example.deltastore.util;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class DataTypeConverter {
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    public Map<String, Object> rowToMap(Row row, StructType schema) {
        Map<String, Object> map = new HashMap<>();
        
        log.debug("Converting row with {} fields in schema", schema.length());
        
        for (int i = 0; i < schema.length(); i++) {
            StructField field = schema.at(i);
            String fieldName = field.getName();
            DataType dataType = field.getDataType();
            
            try {
                // Check if the row actually has this column index
                if (i >= getRowSize(row)) {
                    log.debug("Row only has {} columns, but schema expects {}. Setting field '{}' to null", 
                             getRowSize(row), schema.length(), fieldName);
                    map.put(fieldName, null);
                    continue;
                }
                
                boolean isNull = row.isNullAt(i);
                log.debug("Checking field '{}' at index {}: isNull = {}", fieldName, i, isNull);
                
                if (isNull) {
                    map.put(fieldName, null);
                    continue;
                }
                
                Object value = convertValue(row, i, dataType);
                map.put(fieldName, value);
                log.debug("Successfully converted field '{}' at index {} = {} (type: {})", 
                         fieldName, i, value, value != null ? value.getClass().getSimpleName() : "null");
                
            } catch (Exception e) {
                log.warn("Failed to convert field '{}' of type '{}' at index {}, using null: {}", 
                        fieldName, dataType, i, e.getMessage());
                map.put(fieldName, null);
            }
        }
        
        return map;
    }
    
    /**
     * Get the actual number of columns in the row data.
     * This is a workaround to handle schema/data mismatch.
     */
    private int getRowSize(Row row) {
        try {
            // Try to access each column index until we get an exception
            int size = 0;
            for (int i = 0; i < 100; i++) { // reasonable upper bound
                try {
                    row.isNullAt(i);
                    size++;
                } catch (IllegalArgumentException e) {
                    break;
                }
            }
            return size;
        } catch (Exception e) {
            log.warn("Could not determine row size, defaulting to 0", e);
            return 0;
        }
    }
    
    private Object convertValue(Row row, int index, DataType dataType) {
        if (dataType instanceof StringType) {
            return row.getString(index);
        } else if (dataType instanceof IntegerType) {
            return row.getInt(index);
        } else if (dataType instanceof LongType) {
            return row.getLong(index);
        } else if (dataType instanceof DoubleType) {
            return row.getDouble(index);
        } else if (dataType instanceof FloatType) {
            return row.getFloat(index);
        } else if (dataType instanceof BooleanType) {
            return row.getBoolean(index);
        } else if (dataType instanceof ByteType) {
            return row.getByte(index);
        } else if (dataType instanceof ShortType) {
            return row.getShort(index);
        } else if (dataType instanceof DecimalType) {
            return row.getDecimal(index);
        } else if (dataType instanceof DateType) {
            // Delta stores dates as days since epoch
            int daysSinceEpoch = row.getInt(index);
            return LocalDate.ofEpochDay(daysSinceEpoch).format(DATE_FORMATTER);
        } else if (dataType instanceof TimestampType) {
            // Delta stores timestamps as microseconds since epoch
            long microsSinceEpoch = row.getLong(index);
            LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
                microsSinceEpoch / 1_000_000, 
                (int) ((microsSinceEpoch % 1_000_000) * 1000), 
                java.time.ZoneOffset.UTC
            );
            return dateTime.format(DATETIME_FORMATTER);
        } else if (dataType instanceof BinaryType) {
            return row.getBinary(index);
        } else {
            // For unknown types, try to get as string
            log.debug("Unknown data type '{}', attempting string conversion", dataType);
            return row.getString(index);
        }
    }
    
    public Object convertToExpectedType(Object value, DataType expectedType) {
        if (value == null) {
            return null;
        }
        
        try {
            if (expectedType instanceof StringType) {
                return value.toString();
            } else if (expectedType instanceof IntegerType) {
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return Integer.valueOf(value.toString());
            } else if (expectedType instanceof LongType) {
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return Long.valueOf(value.toString());
            } else if (expectedType instanceof DoubleType) {
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return Double.valueOf(value.toString());
            } else if (expectedType instanceof BooleanType) {
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.valueOf(value.toString());
            } else if (expectedType instanceof DecimalType) {
                if (value instanceof BigDecimal) {
                    return value;
                }
                return new BigDecimal(value.toString());
            } else {
                return value.toString();
            }
        } catch (Exception e) {
            log.warn("Failed to convert value '{}' to type '{}', returning as string", value, expectedType, e);
            return value.toString();
        }
    }
}