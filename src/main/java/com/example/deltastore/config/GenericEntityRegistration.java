package com.example.deltastore.config;

import com.example.deltastore.api.controller.EntityControllerConfig;
import com.example.deltastore.api.controller.EntityControllerRegistry;
import com.example.deltastore.api.controller.EntityConverter;
import com.example.deltastore.api.controller.ReflectionEntityConverter;
import com.example.deltastore.service.EntityService;
import com.example.deltastore.service.GenericEntityServiceImpl;
import com.example.deltastore.validation.EntityValidator;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.storage.DeltaTableManager;
import com.example.deltastore.entity.GenericEntityService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import jakarta.annotation.PostConstruct;
import java.util.Map;

/**
 * Generic configuration-based entity registration system.
 * Automatically registers all entities defined in deltastore.tables configuration
 * without requiring entity-specific code.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
@Profile("local")
public class GenericEntityRegistration {

    private final EntityControllerRegistry registry;
    private final ReflectionEntityConverter reflectionEntityConverter;
    private final EntityValidator<GenericRecord> entityValidator;
    
    @Qualifier("optimized")
    private final DeltaTableManager deltaTableManager;
    private final DeltaStoreMetrics metrics;
    private final GenericEntityService genericEntityService;
    private final DeltaStoreConfiguration deltaStoreConfig;

    /**
     * Generic entity converter adapter that bridges ReflectionEntityConverter 
     * to the EntityConverter interface for any entity type.
     */
    private static class GenericEntityConverterAdapter implements EntityConverter {
        private final ReflectionEntityConverter reflectionConverter;
        private final String entityType;
        private final Class<? extends GenericRecord> entityClass;

        public GenericEntityConverterAdapter(ReflectionEntityConverter reflectionConverter, 
                                           String entityType, 
                                           Class<? extends GenericRecord> entityClass) {
            this.reflectionConverter = reflectionConverter;
            this.entityType = entityType;
            this.entityClass = entityClass;
        }

        @Override
        public GenericRecord convertFromMap(Map<String, Object> entityData) {
            return reflectionConverter.convertFromMap(entityData, entityClass);
        }

        @Override
        public String getEntityType() {
            return entityType;
        }
    }

    @PostConstruct
    public void registerConfiguredEntityTypes() {
        log.info("Starting generic entity registration from configuration...");
        
        Map<String, DeltaStoreConfiguration.TableConfig> tables = deltaStoreConfig.getTables();
        if (tables == null || tables.isEmpty()) {
            log.warn("No tables configured in deltastore.tables - no entities will be registered");
            return;
        }

        int registeredCount = 0;
        for (Map.Entry<String, DeltaStoreConfiguration.TableConfig> entry : tables.entrySet()) {
            String entityType = entry.getKey();
            DeltaStoreConfiguration.TableConfig tableConfig = entry.getValue();
            
            try {
                // Attempt to register the entity generically
                registerEntityType(entityType, tableConfig);
                registeredCount++;
                log.info("Successfully registered entity type: {}", entityType);
            } catch (Exception e) {
                log.error("Failed to register entity type '{}': {}", entityType, e.getMessage(), e);
                // Continue with other entities rather than failing completely
            }
        }
        
        log.info("Generic entity registration completed. {} out of {} entities registered successfully.", 
                registeredCount, tables.size());
        log.info("Registered entity types: {}", registry.getSupportedEntityTypes());
    }

    /**
     * Registers a single entity type generically based on configuration.
     * This method attempts to work with any entity type without hardcoding specifics.
     */
    private void registerEntityType(String entityType, DeltaStoreConfiguration.TableConfig tableConfig) {
        log.debug("Registering entity type: {} with config: {}", entityType, tableConfig);
        
        // Create generic entity service for this entity type
        EntityService<GenericRecord> entityService = new GenericEntityServiceImpl<>(
            entityType, deltaTableManager, metrics, genericEntityService);

        // Try to find the corresponding Avro class
        Class<? extends GenericRecord> entityClass = findEntityClass(entityType);
        if (entityClass == null) {
            log.warn("Could not find Avro class for entity type '{}', using generic approach", entityType);
            // For now, skip entities without corresponding Avro classes
            // In a fully generic system, we might generate them dynamically
            return;
        }

        // Create generic converter adapter
        EntityConverter entityConverter = new GenericEntityConverterAdapter(
            reflectionEntityConverter, entityType, entityClass);

        // Build and register the entity configuration
        EntityControllerConfig config = EntityControllerConfig.builder()
            .entityType(entityType)
            .entityService(entityService)
            .entityValidator(entityValidator)
            .entityConverter(entityConverter)
            .entityClass(entityClass)
            .build();

        registry.registerEntityType(config);
        log.info("Registered generic entity type '{}' with primary key: {} and partitions: {}", 
                entityType, 
                tableConfig.getPrimaryKeyColumn(), 
                tableConfig.getPartitionColumns());
    }

    /**
     * Attempts to find the Avro-generated class for an entity type.
     * Uses naming conventions to locate the class.
     */
    @SuppressWarnings("unchecked")
    private Class<? extends GenericRecord> findEntityClass(String entityType) {
        try {
            // Try common naming patterns for Avro classes
            String[] classNamePatterns = {
                // Singular capitalized (users -> User)
                "com.example.deltastore.schemas." + capitalize(entityType.replaceAll("s$", "")),
                // Exact match capitalized (orders -> Orders) 
                "com.example.deltastore.schemas." + capitalize(entityType),
                // Singular with different patterns
                "com.example.deltastore.schemas." + entityType.substring(0, entityType.length() - 1).toUpperCase() 
                    + entityType.substring(entityType.length() - 1).toLowerCase()
            };

            for (String className : classNamePatterns) {
                try {
                    Class<?> clazz = Class.forName(className);
                    if (GenericRecord.class.isAssignableFrom(clazz)) {
                        log.debug("Found Avro class for entity '{}': {}", entityType, className);
                        return (Class<? extends GenericRecord>) clazz;
                    }
                } catch (ClassNotFoundException e) {
                    // Try next pattern
                    log.trace("Class not found: {}", className);
                }
            }
            
            log.warn("Could not find Avro class for entity type: {}", entityType);
            return null;
            
        } catch (Exception e) {
            log.error("Error finding entity class for '{}': {}", entityType, e.getMessage());
            return null;
        }
    }

    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}