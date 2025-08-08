package com.example.deltastore.service;

import java.util.Map;

public interface DataService {

    /**
     * Saves a data payload to the specified table.
     *
     * @param tableName The name of the target table.
     * @param data The data to save, as a map of field names to values.
     */
    void saveData(String tableName, Map<String, Object> data);

}
