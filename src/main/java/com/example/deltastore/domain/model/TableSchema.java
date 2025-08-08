package com.example.deltastore.domain.model;

import lombok.Data;

import java.util.List;

@Data
public class TableSchema {
    private String name;
    private String schemaPath;
    private String primaryKey;
    private List<String> partitionBy;
}
