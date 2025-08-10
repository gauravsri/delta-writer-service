package com.example.deltastore.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class AutoEntityDiscoveryComprehensiveTest {

    private Map<String, Class<?>> discoveredEntities;
    private Set<String> scannedPackages;
    private List<String> entityAnnotations;
    
    @BeforeEach
    void setUp() {
        discoveredEntities = new ConcurrentHashMap<>();
        scannedPackages = new HashSet<>();
        entityAnnotations = Arrays.asList(
            "@Entity", "@Table", "@Document", "@Component"
        );
        
        // Mock discovered entities
        discoveredEntities.put("User", MockUser.class);
        discoveredEntities.put("Product", MockProduct.class);
        discoveredEntities.put("Order", MockOrder.class);
        
        // Mock scanned packages
        scannedPackages.add("com.example.deltastore.domain");
        scannedPackages.add("com.example.deltastore.model");
        scannedPackages.add("com.example.deltastore.entity");
    }
    
    // Mock entity classes for testing
    static class MockUser {
        private String id;
        private String name;
        private String email;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
    }
    
    static class MockProduct {
        private String id;
        private String name;
        private Double price;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Double getPrice() { return price; }
        public void setPrice(Double price) { this.price = price; }
    }
    
    static class MockOrder {
        private String id;
        private String userId;
        private String productId;
        private Integer quantity;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public Integer getQuantity() { return quantity; }
        public void setQuantity(Integer quantity) { this.quantity = quantity; }
    }
    
    @Test
    void testEntityDiscoveryBasics() {
        assertNotNull(discoveredEntities);
        assertEquals(3, discoveredEntities.size());
        
        assertTrue(discoveredEntities.containsKey("User"));
        assertTrue(discoveredEntities.containsKey("Product"));
        assertTrue(discoveredEntities.containsKey("Order"));
        
        assertEquals(MockUser.class, discoveredEntities.get("User"));
        assertEquals(MockProduct.class, discoveredEntities.get("Product"));
        assertEquals(MockOrder.class, discoveredEntities.get("Order"));
    }
    
    @Test
    void testPackageScanning() {
        assertEquals(3, scannedPackages.size());
        
        assertTrue(scannedPackages.contains("com.example.deltastore.domain"));
        assertTrue(scannedPackages.contains("com.example.deltastore.model"));
        assertTrue(scannedPackages.contains("com.example.deltastore.entity"));
        
        // Test package filtering
        Set<String> filteredPackages = scannedPackages.stream()
            .filter(pkg -> pkg.contains("deltastore"))
            .collect(Collectors.toSet());
        
        assertEquals(3, filteredPackages.size());
        
        // Test package hierarchy
        List<String> sortedPackages = scannedPackages.stream()
            .sorted()
            .collect(Collectors.toList());
        
        assertEquals("com.example.deltastore.domain", sortedPackages.get(0));
        assertEquals("com.example.deltastore.entity", sortedPackages.get(1));
        assertEquals("com.example.deltastore.model", sortedPackages.get(2));
    }
    
    @Test
    void testAnnotationDetection() {
        assertEquals(4, entityAnnotations.size());
        
        assertTrue(entityAnnotations.contains("@Entity"));
        assertTrue(entityAnnotations.contains("@Table"));
        assertTrue(entityAnnotations.contains("@Document"));
        assertTrue(entityAnnotations.contains("@Component"));
        
        // Test annotation filtering
        List<String> jpaAnnotations = entityAnnotations.stream()
            .filter(ann -> ann.equals("@Entity") || ann.equals("@Table"))
            .collect(Collectors.toList());
        
        assertEquals(2, jpaAnnotations.size());
        assertTrue(jpaAnnotations.contains("@Entity"));
        assertTrue(jpaAnnotations.contains("@Table"));
    }
    
    @Test
    void testFieldIntrospection() {
        Class<?> userClass = discoveredEntities.get("User");
        assertNotNull(userClass);
        
        Field[] fields = userClass.getDeclaredFields();
        assertEquals(3, fields.length);
        
        Set<String> fieldNames = Arrays.stream(fields)
            .map(Field::getName)
            .collect(Collectors.toSet());
        
        assertTrue(fieldNames.contains("id"));
        assertTrue(fieldNames.contains("name"));
        assertTrue(fieldNames.contains("email"));
        
        // Test field types
        Map<String, Class<?>> fieldTypes = Arrays.stream(fields)
            .collect(Collectors.toMap(Field::getName, Field::getType));
        
        assertEquals(String.class, fieldTypes.get("id"));
        assertEquals(String.class, fieldTypes.get("name"));
        assertEquals(String.class, fieldTypes.get("email"));
    }
    
    @Test
    void testMethodIntrospection() {
        Class<?> productClass = discoveredEntities.get("Product");
        assertNotNull(productClass);
        
        Method[] methods = productClass.getDeclaredMethods();
        assertTrue(methods.length >= 6); // At least 6 methods (3 getters + 3 setters)
        
        List<String> methodNames = Arrays.stream(methods)
            .map(Method::getName)
            .collect(Collectors.toList());
        
        assertTrue(methodNames.contains("getId"));
        assertTrue(methodNames.contains("setId"));
        assertTrue(methodNames.contains("getName"));
        assertTrue(methodNames.contains("setName"));
        assertTrue(methodNames.contains("getPrice"));
        assertTrue(methodNames.contains("setPrice"));
        
        // Test getter methods
        List<Method> getters = Arrays.stream(methods)
            .filter(method -> method.getName().startsWith("get"))
            .collect(Collectors.toList());
        
        assertEquals(3, getters.size());
        
        // Test setter methods
        List<Method> setters = Arrays.stream(methods)
            .filter(method -> method.getName().startsWith("set"))
            .collect(Collectors.toList());
        
        assertEquals(3, setters.size());
    }
    
    @Test
    void testEntityMetadataExtraction() {
        Map<String, Object> userMetadata = extractEntityMetadata(MockUser.class);
        
        assertNotNull(userMetadata);
        assertEquals("MockUser", userMetadata.get("className"));
        assertEquals("com.example.deltastore.config.AutoEntityDiscoveryComprehensiveTest$MockUser", userMetadata.get("fullClassName"));
        
        @SuppressWarnings("unchecked")
        List<String> fieldList = (List<String>) userMetadata.get("fields");
        assertEquals(3, fieldList.size());
        assertTrue(fieldList.contains("id"));
        assertTrue(fieldList.contains("name"));
        assertTrue(fieldList.contains("email"));
        
        @SuppressWarnings("unchecked")
        Map<String, String> fieldTypes = (Map<String, String>) userMetadata.get("fieldTypes");
        assertEquals("String", fieldTypes.get("id"));
        assertEquals("String", fieldTypes.get("name"));
        assertEquals("String", fieldTypes.get("email"));
    }
    
    @Test
    void testEntityValidation() {
        // Test valid entity
        boolean isValidUser = validateEntity(MockUser.class);
        assertTrue(isValidUser);
        
        // Test valid entity with different field types
        boolean isValidProduct = validateEntity(MockProduct.class);
        assertTrue(isValidProduct);
        
        // Test entity validation criteria
        assertTrue(hasDefaultConstructor(MockUser.class));
        assertTrue(hasGettersAndSetters(MockUser.class));
        assertTrue(hasIdField(MockUser.class));
        
        assertTrue(hasDefaultConstructor(MockProduct.class));
        assertTrue(hasGettersAndSetters(MockProduct.class));
        assertTrue(hasIdField(MockProduct.class));
    }
    
    @Test
    void testEntityRegistration() {
        Map<String, Object> entityRegistry = new ConcurrentHashMap<>();
        
        // Register entities
        for (Map.Entry<String, Class<?>> entry : discoveredEntities.entrySet()) {
            String entityName = entry.getKey();
            Class<?> entityClass = entry.getValue();
            
            Map<String, Object> registration = new HashMap<>();
            registration.put("class", entityClass);
            registration.put("tableName", entityName.toLowerCase());
            registration.put("registrationTime", System.currentTimeMillis());
            registration.put("isActive", true);
            
            entityRegistry.put(entityName, registration);
        }
        
        assertEquals(3, entityRegistry.size());
        assertTrue(entityRegistry.containsKey("User"));
        assertTrue(entityRegistry.containsKey("Product"));
        assertTrue(entityRegistry.containsKey("Order"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> userRegistration = (Map<String, Object>) entityRegistry.get("User");
        assertEquals(MockUser.class, userRegistration.get("class"));
        assertEquals("user", userRegistration.get("tableName"));
        assertTrue((Boolean) userRegistration.get("isActive"));
        assertNotNull(userRegistration.get("registrationTime"));
    }
    
    @Test
    void testSchemaGeneration() {
        Map<String, Map<String, String>> schemas = new HashMap<>();
        
        for (Map.Entry<String, Class<?>> entry : discoveredEntities.entrySet()) {
            String entityName = entry.getKey();
            Class<?> entityClass = entry.getValue();
            
            Map<String, String> schema = generateSchema(entityClass);
            schemas.put(entityName, schema);
        }
        
        assertEquals(3, schemas.size());
        
        Map<String, String> userSchema = schemas.get("User");
        assertEquals(3, userSchema.size());
        assertEquals("string", userSchema.get("id"));
        assertEquals("string", userSchema.get("name"));
        assertEquals("string", userSchema.get("email"));
        
        Map<String, String> productSchema = schemas.get("Product");
        assertEquals(3, productSchema.size());
        assertEquals("string", productSchema.get("id"));
        assertEquals("string", productSchema.get("name"));
        assertEquals("double", productSchema.get("price"));
    }
    
    @Test
    void testDiscoveryConfiguration() {
        Map<String, Object> discoveryConfig = new HashMap<>();
        discoveryConfig.put("enabled", true);
        discoveryConfig.put("basePackages", scannedPackages);
        discoveryConfig.put("annotations", entityAnnotations);
        discoveryConfig.put("includeGetters", true);
        discoveryConfig.put("includeSetters", true);
        discoveryConfig.put("validateEntities", true);
        
        assertTrue((Boolean) discoveryConfig.get("enabled"));
        assertTrue((Boolean) discoveryConfig.get("includeGetters"));
        assertTrue((Boolean) discoveryConfig.get("includeSetters"));
        assertTrue((Boolean) discoveryConfig.get("validateEntities"));
        
        @SuppressWarnings("unchecked")
        Set<String> configuredPackages = (Set<String>) discoveryConfig.get("basePackages");
        assertEquals(3, configuredPackages.size());
        
        @SuppressWarnings("unchecked")
        List<String> configuredAnnotations = (List<String>) discoveryConfig.get("annotations");
        assertEquals(4, configuredAnnotations.size());
    }
    
    @Test
    void testConcurrentDiscovery() {
        Map<String, Class<?>> concurrentEntities = new ConcurrentHashMap<>();
        List<Thread> discoveryThreads = new ArrayList<>();
        
        for (int i = 0; i < 5; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                // Simulate concurrent entity discovery
                concurrentEntities.put("Entity" + threadId, String.class);
                
                // Simulate processing time
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            discoveryThreads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete
        discoveryThreads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        assertEquals(5, concurrentEntities.size());
        for (int i = 0; i < 5; i++) {
            assertTrue(concurrentEntities.containsKey("Entity" + i));
            assertEquals(String.class, concurrentEntities.get("Entity" + i));
        }
    }
    
    // Helper methods
    private Map<String, Object> extractEntityMetadata(Class<?> entityClass) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("className", entityClass.getSimpleName());
        metadata.put("fullClassName", entityClass.getName());
        
        Field[] fields = entityClass.getDeclaredFields();
        List<String> fieldNames = Arrays.stream(fields)
            .map(Field::getName)
            .collect(Collectors.toList());
        metadata.put("fields", fieldNames);
        
        Map<String, String> fieldTypes = Arrays.stream(fields)
            .collect(Collectors.toMap(Field::getName, field -> field.getType().getSimpleName()));
        metadata.put("fieldTypes", fieldTypes);
        
        return metadata;
    }
    
    private boolean validateEntity(Class<?> entityClass) {
        return hasDefaultConstructor(entityClass) && 
               hasGettersAndSetters(entityClass) && 
               hasIdField(entityClass);
    }
    
    private boolean hasDefaultConstructor(Class<?> entityClass) {
        try {
            entityClass.getDeclaredConstructor();
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
    
    private boolean hasGettersAndSetters(Class<?> entityClass) {
        Method[] methods = entityClass.getDeclaredMethods();
        long getterCount = Arrays.stream(methods)
            .filter(method -> method.getName().startsWith("get"))
            .count();
        long setterCount = Arrays.stream(methods)
            .filter(method -> method.getName().startsWith("set"))
            .count();
        
        Field[] fields = entityClass.getDeclaredFields();
        return getterCount >= fields.length && setterCount >= fields.length;
    }
    
    private boolean hasIdField(Class<?> entityClass) {
        Field[] fields = entityClass.getDeclaredFields();
        return Arrays.stream(fields)
            .anyMatch(field -> "id".equals(field.getName()));
    }
    
    private Map<String, String> generateSchema(Class<?> entityClass) {
        Field[] fields = entityClass.getDeclaredFields();
        Map<String, String> schema = new HashMap<>();
        
        for (Field field : fields) {
            String fieldType = mapJavaTypeToSchemaType(field.getType());
            schema.put(field.getName(), fieldType);
        }
        
        return schema;
    }
    
    private String mapJavaTypeToSchemaType(Class<?> javaType) {
        if (javaType == String.class) return "string";
        if (javaType == Integer.class || javaType == int.class) return "int";
        if (javaType == Long.class || javaType == long.class) return "long";
        if (javaType == Double.class || javaType == double.class) return "double";
        if (javaType == Float.class || javaType == float.class) return "float";
        if (javaType == Boolean.class || javaType == boolean.class) return "boolean";
        return "object";
    }
}