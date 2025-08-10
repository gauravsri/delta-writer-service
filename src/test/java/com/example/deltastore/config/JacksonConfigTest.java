package com.example.deltastore.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

class JacksonConfigTest {

    private JacksonConfig jacksonConfig;

    @BeforeEach
    void setUp() {
        jacksonConfig = new JacksonConfig();
    }

    @Test
    void testObjectMapperConfiguration() {
        ObjectMapper objectMapper = jacksonConfig.objectMapper();
        
        assertNotNull(objectMapper);
        
        // Test property naming strategy
        assertEquals(PropertyNamingStrategies.SNAKE_CASE, 
            objectMapper.getPropertyNamingStrategy());
    }

    @Test
    void testObjectMapperSerializationFeatures() {
        ObjectMapper objectMapper = jacksonConfig.objectMapper();
        
        // Test that the mapper handles various data types correctly
        assertNotNull(objectMapper.getSerializationConfig());
        assertNotNull(objectMapper.getDeserializationConfig());
    }

    @Test
    void testSnakeCaseNaming() throws Exception {
        ObjectMapper objectMapper = jacksonConfig.objectMapper();
        
        // Test object with camelCase properties
        TestObject testObj = new TestObject("test123", "testUser");
        String json = objectMapper.writeValueAsString(testObj);
        
        // Should contain snake_case property names
        assertTrue(json.contains("user_id"));
        assertTrue(json.contains("user_name"));
        assertFalse(json.contains("userId"));
        assertFalse(json.contains("userName"));
    }

    @Test
    void testDeserialization() throws Exception {
        ObjectMapper objectMapper = jacksonConfig.objectMapper();
        
        String json = "{\"user_id\":\"test123\",\"user_name\":\"testUser\"}";
        TestObject testObj = objectMapper.readValue(json, TestObject.class);
        
        assertEquals("test123", testObj.getUserId());
        assertEquals("testUser", testObj.getUserName());
    }

    @Test
    void testObjectMapperIsNotNull() {
        ObjectMapper objectMapper = jacksonConfig.objectMapper();
        assertNotNull(objectMapper);
    }

    // Test class for serialization/deserialization
    public static class TestObject {
        private String userId;
        private String userName;
        
        public TestObject() {}
        
        public TestObject(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }
        
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        
        public String getUserName() { return userName; }
        public void setUserName(String userName) { this.userName = userName; }
    }
}