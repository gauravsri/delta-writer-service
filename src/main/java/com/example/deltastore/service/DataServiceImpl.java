package com.example.deltastore.service;

import com.example.deltastore.config.SchemaRegistryConfig;
import com.example.deltastore.domain.model.TableSchema;
import com.example.deltastore.storage.DeltaTableManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class DataServiceImpl implements DataService {

    private final SchemaRegistryConfig schemaRegistry;
    private final DeltaTableManager deltaTableManager;

    @Override
    public void saveData(String tableName, Map<String, Object> data) {
        log.info("Attempting to save data for table '{}'", tableName);

        // 1. Look up table schema from config
        TableSchema tableSchema = schemaRegistry.getTableSchema(tableName)
                .orElseThrow(() -> new IllegalArgumentException("No schema configured for table: " + tableName));

        try {
            // 2. Load Avro schema from classpath
            Schema avroSchema = loadAvroSchema(tableSchema.getSchemaPath());

            // 3. Convert data Map to Avro GenericRecord
            GenericRecord record = convertMapToGenericRecord(data, avroSchema);

            // 4. Write record using DeltaTableManager
            deltaTableManager.write(tableName, Collections.singletonList(record), avroSchema);
            log.info("Successfully saved data for table '{}'", tableName);

        } catch (IOException e) {
            log.error("Failed to load Avro schema for table '{}' from path '{}'", tableName, tableSchema.getSchemaPath(), e);
            throw new RuntimeException("Schema loading failed for table: " + tableName, e);
        }
    }

    private Schema loadAvroSchema(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path.replace("classpath:", ""));
        try (InputStream inputStream = resource.getInputStream()) {
            return new Schema.Parser().parse(inputStream);
        }
    }

    private GenericRecord convertMapToGenericRecord(Map<String, Object> data, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            if (data.containsKey(field.name())) {
                record.put(field.name(), data.get(field.name()));
            }
        }
        // TODO: Add validation to ensure all required fields are present
        return record;
    }

    @Override
    public Optional<Map<String, Object>> readData(String tableName, String primaryKeyValue) {
        log.info("Reading data for table '{}' with key '{}'", tableName, primaryKeyValue);

        TableSchema tableSchema = schemaRegistry.getTableSchema(tableName)
                .orElseThrow(() -> new IllegalArgumentException("No schema configured for table: " + tableName));

        String primaryKeyColumn = tableSchema.getPrimaryKey();
        if (primaryKeyColumn == null || primaryKeyColumn.isBlank()) {
            throw new IllegalStateException("No primary key configured for table: " + tableName);
        }

        return deltaTableManager.read(tableName, primaryKeyColumn, primaryKeyValue);
    }
}
