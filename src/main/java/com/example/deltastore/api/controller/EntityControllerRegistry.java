package com.example.deltastore.api.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that manages configurations for all supported entity types in the generic controller.
 * New entity types can be registered here to automatically get full CRUD support.
 */
@Component
@Slf4j
public class EntityControllerRegistry {

    private final Map<String, EntityControllerConfig> configs = new ConcurrentHashMap<>();

    /**
     * Registers a new entity type configuration.
     * 
     * @param config The configuration for the entity type
     */
    public void registerEntityType(EntityControllerConfig config) {
        configs.put(config.getEntityType(), config);
        log.info("Registered entity type '{}' in generic controller", config.getEntityType());
    }

    /**
     * Gets the configuration for a specific entity type.
     * 
     * @param entityType The entity type name
     * @return The configuration or null if not found
     */
    public EntityControllerConfig getConfig(String entityType) {
        return configs.get(entityType);
    }

    /**
     * Gets all supported entity types.
     * 
     * @return List of supported entity type names
     */
    public List<String> getSupportedEntityTypes() {
        return List.copyOf(configs.keySet());
    }

    /**
     * Checks if an entity type is supported.
     * 
     * @param entityType The entity type name
     * @return true if supported, false otherwise
     */
    public boolean isSupported(String entityType) {
        return configs.containsKey(entityType);
    }

    /**
     * Gets the count of registered entity types.
     * 
     * @return Number of registered entity types
     */
    public int getRegisteredCount() {
        return configs.size();
    }
}