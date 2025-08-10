package com.example.deltastore.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StoragePropertiesTest {

    @Test
    void testGetMaskedAccessKey() {
        StorageProperties properties = new StorageProperties();
        properties.setAccessKey("verylongaccesskey123");
        
        assertEquals("very****", properties.getMaskedAccessKey());
    }

    @Test
    void testGetMaskedAccessKeyShort() {
        StorageProperties properties = new StorageProperties();
        properties.setAccessKey("abc");
        
        assertEquals("****", properties.getMaskedAccessKey());
    }

    @Test
    void testGetMaskedAccessKeyNull() {
        StorageProperties properties = new StorageProperties();
        properties.setAccessKey(null);
        
        assertEquals("****", properties.getMaskedAccessKey());
    }

    @Test
    void testGetMaskedSecretKey() {
        StorageProperties properties = new StorageProperties();
        properties.setSecretKey("anysecretkey");
        
        assertEquals("****", properties.getMaskedSecretKey());
    }

    @Test
    void testGetMaskedSecretKeyShort() {
        StorageProperties properties = new StorageProperties();
        properties.setSecretKey("xyz");
        
        assertEquals("****", properties.getMaskedSecretKey());
    }

    @Test
    void testGetMaskedSecretKeyNull() {
        StorageProperties properties = new StorageProperties();
        properties.setSecretKey(null);
        
        assertEquals("****", properties.getMaskedSecretKey());
    }

    @Test
    void testSettersAndGetters() {
        StorageProperties properties = new StorageProperties();
        
        properties.setBucketName("test-bucket");
        properties.setEndpoint("http://localhost:9000");
        properties.setAccessKey("testkey");
        properties.setSecretKey("testsecret");
        
        assertEquals("test-bucket", properties.getBucketName());
        assertEquals("http://localhost:9000", properties.getEndpoint());
        assertEquals("testkey", properties.getAccessKey());
        assertEquals("testsecret", properties.getSecretKey());
    }
}