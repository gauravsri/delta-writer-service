package com.example.deltastore.service;

import java.util.Map;

import java.util.Map;
import java.util.Optional;

public interface DataService {

    /**
     * Saves a data payload to the specified table.
     *
     * @param tableName The name of the target table.
     * @param data The data to save, as a map of field names to values.
     */
    void saveData(String tableName, Map<String, Object> data);

    /**
     * Reads a single record from the specified table by its primary key.
     *
     * @param tableName The name of the target table.
     * @param primaryKey The primary key of the record to read.
     * @return An Optional containing the record as a Map, or an empty Optional if not found.
     */
    Optional<Map<String, Object>> readData(String tableName, String primaryKey);
}
