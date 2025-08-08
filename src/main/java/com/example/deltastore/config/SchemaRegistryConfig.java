package com.example.deltastore.config;

import com.example.deltastore.domain.model.TableSchema;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@ConfigurationProperties(prefix = "app.schema-registry")
@Data
public class SchemaRegistryConfig {

    private List<TableSchema> tables;

    private Map<String, TableSchema> tableMap;

    public Optional<TableSchema> getTableSchema(String name) {
        if (tableMap == null && tables != null) {
            tableMap = tables.stream()
                    .collect(Collectors.toMap(TableSchema::getName, Function.identity()));
        }
        return Optional.ofNullable(tableMap != null ? tableMap.get(name) : null);
    }
}
