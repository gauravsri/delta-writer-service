package com.example.deltastore.entity;

import org.apache.avro.Schema;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Metadata information for registered entities
 */
@Data
@Builder
public class EntityMetadata {
    private String entityType;
    private Schema schema;
    private String primaryKeyColumn;
    private List<String> partitionColumns;
    private Map<String, String> properties;
    private LocalDateTime registeredAt;
    private LocalDateTime lastUpdated;
    private String schemaVersion;
    private boolean active;
}