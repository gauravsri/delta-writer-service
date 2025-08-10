package com.example.deltastore.api.controller;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

/**
 * Interface for converting between different entity representations.
 * Each entity type can have its own converter implementation.
 */
public interface EntityConverter {
    
    /**
     * Converts a Map (from JSON request body) to a GenericRecord entity.
     * 
     * @param entityData The map data from the request
     * @return GenericRecord representation of the entity
     */
    GenericRecord convertFromMap(Map<String, Object> entityData);
    
    /**
     * Gets the entity type name this converter handles.
     * 
     * @return The entity type name (e.g., "users", "products")
     */
    String getEntityType();
}