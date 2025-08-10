package com.example.deltastore.api.controller;

import com.example.deltastore.service.EntityService;
import com.example.deltastore.validation.EntityValidator;
import lombok.Builder;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

/**
 * Configuration class that holds all the components needed for a specific entity type
 * in the generic controller.
 */
@Data
@Builder
public class EntityControllerConfig {
    private final String entityType;
    private final EntityService<GenericRecord> entityService;
    private final EntityValidator<GenericRecord> entityValidator;
    private final EntityConverter entityConverter;
    private final Class<? extends GenericRecord> entityClass;
}