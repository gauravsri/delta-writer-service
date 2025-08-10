package com.example.deltastore.service;

import com.example.deltastore.api.dto.BatchCreateResponse;
import com.example.deltastore.entity.EntityOperationResult;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Generic entity service interface that handles CRUD operations for any entity type.
 * This replaces entity-specific services like UserService to provide consistency
 * and reduce code duplication.
 * 
 * @param <T> The entity type that extends GenericRecord
 */
public interface EntityService<T extends GenericRecord> {

    /**
     * Saves an entity record.
     * @param entity The entity object to save.
     */
    void save(T entity);

    /**
     * Saves an entity record and returns detailed operation result.
     * @param entity The entity object to save.
     * @return EntityOperationResult with operation details
     */
    EntityOperationResult<T> saveWithResult(T entity);

    /**
     * Saves multiple entity records in a single optimized batch operation.
     * This method is more efficient than multiple individual save operations.
     * @param entities The list of entities to save
     * @return BatchCreateResponse with statistics about the operation
     */
    BatchCreateResponse saveBatch(List<T> entities);

    /**
     * Saves multiple entity records and returns detailed operation result.
     * @param entities The list of entities to save
     * @return EntityOperationResult with operation details
     */
    EntityOperationResult<T> saveAllWithResult(List<T> entities);

    /**
     * Gets the entity type name this service handles.
     * @return The entity type name (e.g., "users", "products")
     */
    String getEntityType();
}