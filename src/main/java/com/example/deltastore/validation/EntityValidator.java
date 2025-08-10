package com.example.deltastore.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generic entity validator that uses Drools rules to validate any GenericRecord entity type.
 * Rules are loaded dynamically based on entity type from classpath resources.
 * 
 * @param <T> The entity type that extends GenericRecord
 */
@Component
@Slf4j
public class EntityValidator<T extends GenericRecord> {

    private final Map<String, KieContainer> kieContainers = new ConcurrentHashMap<>();
    private final KieServices kieServices;

    public EntityValidator() {
        this.kieServices = KieServices.Factory.get();
    }

    @PostConstruct
    public void init() {
        log.info("Initializing EntityValidator with Drools rules engine");
        // Load default rules for known entity types
        loadRulesForEntityType("users");
        // Add more entity types as needed:
        // loadRulesForEntityType("products");
        // loadRulesForEntityType("orders");
    }

    @PreDestroy
    public void cleanup() {
        kieContainers.values().forEach(container -> {
            try {
                container.dispose();
            } catch (Exception e) {
                log.warn("Error disposing KieContainer: {}", e.getMessage());
            }
        });
        kieContainers.clear();
    }

    /**
     * Validates an entity using Drools rules specific to the entity type.
     * 
     * @param entityType The type of entity (e.g., "users", "products")
     * @param entity The entity to validate
     * @return List of validation error messages
     */
    public List<String> validate(String entityType, T entity) {
        if (entity == null) {
            return List.of(entityType + " entity cannot be null");
        }

        List<String> errors = new ArrayList<>();
        
        try {
            KieContainer kieContainer = getOrCreateKieContainer(entityType);
            if (kieContainer == null) {
                log.warn("No validation rules found for entity type: {}", entityType);
                return errors; // Return empty list if no rules found
            }

            KieSession kieSession = kieContainer.newKieSession();
            try {
                // Add validation context
                ValidationContext context = new ValidationContext();
                kieSession.insert(context);
                kieSession.insert(entity);
                
                // Execute validation rules
                kieSession.fireAllRules();
                
                // Collect validation errors
                errors.addAll(context.getErrors());
                
                log.debug("Validation completed for {} entity. Found {} errors", entityType, errors.size());
            } finally {
                kieSession.dispose();
            }
            
        } catch (Exception e) {
            log.error("Error during validation for entity type: {}", entityType, e);
            errors.add("Internal validation error: " + e.getMessage());
        }

        return errors;
    }

    /**
     * Validates a User entity (convenience method for backward compatibility).
     * 
     * @param user The user entity to validate
     * @return List of validation error messages
     */
    public List<String> validateUser(T user) {
        return validate("users", user);
    }

    private KieContainer getOrCreateKieContainer(String entityType) {
        return kieContainers.computeIfAbsent(entityType, this::loadRulesForEntityType);
    }

    private KieContainer loadRulesForEntityType(String entityType) {
        try {
            String rulesPath = "validation/rules/" + entityType + "-validation.drl";
            InputStream rulesStream = getClass().getClassLoader().getResourceAsStream(rulesPath);
            
            if (rulesStream == null) {
                log.warn("No validation rules file found at: {}", rulesPath);
                return null;
            }

            String rulesContent = new String(rulesStream.readAllBytes(), StandardCharsets.UTF_8);
            
            KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
            kieFileSystem.write("src/main/resources/" + rulesPath, rulesContent);
            
            KieBuilder kieBuilder = kieServices.newKieBuilder(kieFileSystem);
            kieBuilder.buildAll();
            
            if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
                log.error("Drools compilation errors for {}: {}", entityType, kieBuilder.getResults().getMessages());
                return null;
            }

            KieContainer kieContainer = kieServices.newKieContainer(kieBuilder.getKieModule().getReleaseId());
            log.info("Successfully loaded validation rules for entity type: {}", entityType);
            
            return kieContainer;
            
        } catch (IOException e) {
            log.error("Error loading validation rules for entity type: {}", entityType, e);
            return null;
        }
    }

    /**
     * Context class to collect validation errors during rule execution.
     */
    public static class ValidationContext {
        private final List<String> errors = new ArrayList<>();

        public void addError(String error) {
            errors.add(error);
        }

        public List<String> getErrors() {
            return new ArrayList<>(errors);
        }

        public boolean hasErrors() {
            return !errors.isEmpty();
        }

        public int getErrorCount() {
            return errors.size();
        }
    }
}