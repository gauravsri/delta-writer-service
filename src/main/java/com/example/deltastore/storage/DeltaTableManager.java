package com.example.deltastore.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Manages operations on a Delta Lake table, abstracting the low-level
 * interactions with the Delta Kernel and Parquet files.
 */
public interface DeltaTableManager {

    /**
     * Writes a list of Avro records to the specified Delta table.
     * This method handles the creation of Parquet files and the transactional
     * commit to the Delta log.
     *
     * @param tableName The name of the target table.
     * @param records   The list of records to write.
     * @param schema    The Avro schema for the records.
     */
    void write(String tableName, List<GenericRecord> records, Schema schema);

}
