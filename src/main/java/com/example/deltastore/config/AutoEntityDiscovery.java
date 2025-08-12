package com.example.deltastore.config;

import com.example.deltastore.api.controller.EntityControllerConfig;
import com.example.deltastore.api.controller.EntityControllerRegistry;
import com.example.deltastore.api.controller.ReflectionEntityConverter;
import com.example.deltastore.entity.GenericEntityService;
import com.example.deltastore.metrics.DeltaStoreMetrics;
import com.example.deltastore.service.EntityService;
import com.example.deltastore.service.GenericEntityServiceImpl;
import com.example.deltastore.storage.DeltaTableManager;
import com.example.deltastore.util.BatchMemoryMonitor;
import com.example.deltastore.validation.EntityValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Automatically discovers entity types from Avro schema files and registers them.
 * Eliminates the need for manual registration code in Java.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AutoEntityDiscovery {

    private final EntityControllerRegistry registry;
    private final ReflectionEntityConverter reflectionConverter;
    private final ApplicationContext applicationContext;
    
    // Pattern to extract entity name and class from Avro schema
    private static final Pattern ENTITY_NAME_PATTERN = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern NAMESPACE_PATTERN = Pattern.compile("\"namespace\"\\s*:\\s*\"([^\"]+)\"");

    @PostConstruct
    public void discoverAndRegisterEntities() {
        log.info("Starting auto-discovery of entity types from Avro schemas...");
        
        try {
            // Discover from classpath avro schemas
            discoverFromClasspathAvroSchemas();
            
            log.info("Completed auto-discovery. {} entity types registered.", 
                    registry.getRegisteredCount());
                    
        } catch (Exception e) {
            log.error("Failed to auto-discover entity types", e);
        }
    }
    
    private void discoverFromClasspathAvroSchemas() throws IOException {
        // Get all .avsc files from classpath
        Resource[] resources = applicationContext.getResources("classpath:avro/*.avsc");
        
        for (Resource resource : resources) {
            try {
                processAvroSchemaFile(resource);
            } catch (Exception e) {
                log.warn("Failed to process Avro schema {}: {}", 
                        resource.getFilename(), e.getMessage());
            }
        }
        
        // Also check src/main/avro directory for development
        try {
            Path avroDir = Paths.get("src/main/avro");
            if (Files.exists(avroDir)) {
                Files.walk(avroDir)
                    .filter(path -> path.toString().endsWith(".avsc"))
                    .forEach(this::processAvroSchemaPath);
            }
        } catch (Exception e) {
            log.debug("Could not scan src/main/avro directory: {}", e.getMessage());
        }
    }
    
    private void processAvroSchemaFile(Resource resource) throws IOException {
        String content = new String(resource.getInputStream().readAllBytes());
        processAvroSchemaContent(content, resource.getFilename());
    }
    
    private void processAvroSchemaPath(Path schemaPath) {
        try {
            String content = Files.readString(schemaPath);
            processAvroSchemaContent(content, schemaPath.getFileName().toString());
        } catch (Exception e) {
            log.warn("Failed to process schema file {}: {}", schemaPath, e.getMessage());
        }
    }
    
    private void processAvroSchemaContent(String schemaContent, String filename) {
        try {
            // Parse schema to get entity information
            Schema schema = new Schema.Parser().parse(schemaContent);
            
            String entityName = schema.getName().toLowerCase(); // e.g., "User" -> "users"
            if (!entityName.endsWith("s")) {
                entityName += "s"; // Pluralize for REST convention
            }
            
            String namespace = schema.getNamespace();
            String fullClassName = namespace + "." + schema.getName();
            
            // Try to load the generated Avro class
            Class<?> avroClass = tryLoadClass(fullClassName);
            if (avroClass == null) {
                log.warn("Could not load Avro class {} for schema {}", fullClassName, filename);
                return;
            }
            
            // Check if already registered
            if (registry.isSupported(entityName)) {
                log.debug("Entity type '{}' already registered, skipping", entityName);
                return;
            }
            
            // Create configuration for this entity type
            EntityControllerConfig config = createEntityConfig(entityName, avroClass);
            registry.registerEntityType(config);
            
            log.info("Auto-registered entity type '{}' from schema {}", entityName, filename);
            
        } catch (Exception e) {
            log.error("Failed to process Avro schema {}: {}", filename, e.getMessage());
        }
    }
    
    @SuppressWarnings("unchecked")
    private EntityControllerConfig createEntityConfig(String entityType, Class<?> avroClass) {
        // Create generic entity service using reflection
        EntityService<GenericRecord> entityService = createGenericEntityService(entityType);
        
        // Get or create generic validator
        EntityValidator<GenericRecord> entityValidator = getGenericEntityValidator();
        
        // Create reflection-based converter
        ReflectionEntityConverter.EntitySpecificConverter entityConverter = 
            new ReflectionEntityConverter.EntitySpecificConverter(reflectionConverter, avroClass);
        
        return EntityControllerConfig.builder()
            .entityType(entityType)
            .entityService(entityService)
            .entityValidator(entityValidator)
            .entityConverter(entityConverter)
            .entityClass((Class<? extends GenericRecord>) avroClass)
            .build();
    }
    
    @SuppressWarnings("unchecked")
    private EntityService<GenericRecord> createGenericEntityService(String entityType) {
        try {
            // Get required dependencies from application context
            var deltaTableManager = applicationContext.getBean("optimized", DeltaTableManager.class);
            var metricsService = applicationContext.getBean("deltaStoreMetrics", DeltaStoreMetrics.class);
            var genericEntityService = applicationContext.getBean("genericEntityService", GenericEntityService.class);
            var batchMemoryMonitor = applicationContext.getBean(BatchMemoryMonitor.class);
            
            // Use reflection to create GenericEntityServiceImpl
            return (EntityService<GenericRecord>) new GenericEntityServiceImpl<GenericRecord>(
                entityType,
                deltaTableManager,
                metricsService,
                genericEntityService,
                batchMemoryMonitor
            );
        } catch (Exception e) {
            log.error("Failed to create entity service for {}: {}", entityType, e.getMessage());
            throw new RuntimeException("Could not create entity service", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private EntityValidator<GenericRecord> getGenericEntityValidator() {
        try {
            // Get generic validator bean from application context
            return (EntityValidator<GenericRecord>) applicationContext.getBean("entityValidator");
        } catch (Exception e) {
            log.error("Failed to get generic entity validator: {}", e.getMessage());
            throw new RuntimeException("Could not get entity validator", e);
        }
    }
    
    private Class<?> tryLoadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            // Try common variations
            String[] variations = {
                className,
                className.replace(".schemas.", ".avro."),
                className.replace(".schemas.", ".model."),
                "com.example.deltastore.schemas." + className.substring(className.lastIndexOf('.') + 1)
            };
            
            for (String variation : variations) {
                try {
                    return Class.forName(variation);
                } catch (ClassNotFoundException ignored) {
                }
            }
            
            return null;
        }
    }
}