package com.example.deltastore.config;

import com.example.deltastore.domain.model.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaRegistryConfigTest {

    private SchemaRegistryConfig schemaRegistryConfig;
    private TableSchema userSchema;

    @BeforeEach
    void setUp() {
        schemaRegistryConfig = new SchemaRegistryConfig();
        userSchema = new TableSchema();
        userSchema.setName("users");
        userSchema.setSchemaPath("classpath:schemas/users.avsc");
        schemaRegistryConfig.setTables(Collections.singletonList(userSchema));
    }

    @Test
    void whenGetTableSchema_andTableExists_thenReturnsSchema() {
        // When
        Optional<TableSchema> result = schemaRegistryConfig.getTableSchema("users");

        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo("users");
        assertThat(result.get().getSchemaPath()).isEqualTo("classpath:schemas/users.avsc");
    }

    @Test
    void whenGetTableSchema_andTableDoesNotExist_thenReturnsEmpty() {
        // When
        Optional<TableSchema> result = schemaRegistryConfig.getTableSchema("non_existent_table");

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void whenGetTableSchema_isCalledMultipleTimes_thenReturnsSameInstance() {
        // When
        Optional<TableSchema> result1 = schemaRegistryConfig.getTableSchema("users");
        Optional<TableSchema> result2 = schemaRegistryConfig.getTableSchema("users");

        // Then
        assertThat(result1).isPresent();
        assertThat(result2).isPresent();
        assertThat(result1.get()).isSameAs(result2.get());
    }
}
