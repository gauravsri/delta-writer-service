package com.example.deltastore.api.controller;

import com.example.deltastore.storage.OptimizedDeltaTableManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PerformanceControllerTest {

    @Mock
    private OptimizedDeltaTableManager optimizedManager;

    private PerformanceController performanceController;

    @BeforeEach
    void setUp() {
        performanceController = new PerformanceController();
        // Use reflection to set the private field
        try {
            java.lang.reflect.Field field = PerformanceController.class.getDeclaredField("optimizedManager");
            field.setAccessible(true);
            field.set(performanceController, optimizedManager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set optimizedManager field", e);
        }
    }

    @Test
    void testConstructorWithValidManager() {
        assertNotNull(performanceController);
    }

    @Test
    void testGetMetricsReturnsValidResponse() {
        // Mock metrics data
        Map<String, Long> mockMetrics = new HashMap<>();
        mockMetrics.put("writes", 10L);
        mockMetrics.put("reads", 5L);
        mockMetrics.put("conflicts", 0L);
        mockMetrics.put("cache_hits", 3L);
        mockMetrics.put("cache_misses", 7L);
        mockMetrics.put("queue_size", 2L);
        mockMetrics.put("avg_write_latency_ms", 6500L);
        mockMetrics.put("cache_hit_rate_percent", 30L);

        when(optimizedManager.getMetrics()).thenReturn(mockMetrics);

        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        Map<String, Object> responseBody = response.getBody();
        assertNotNull(responseBody);
        
        assertTrue(responseBody.containsKey("optimization_enabled"));
        assertTrue(responseBody.containsKey("optimized_metrics"));
        assertTrue(responseBody.containsKey("timestamp"));
        
        assertEquals(true, responseBody.get("optimization_enabled"));
        assertEquals(mockMetrics, responseBody.get("optimized_metrics"));
        assertTrue(responseBody.get("timestamp") instanceof Long);
        
        verify(optimizedManager).getMetrics();
    }

    @Test
    void testGetMetricsWithEmptyMetrics() {
        Map<String, Long> emptyMetrics = new HashMap<>();
        when(optimizedManager.getMetrics()).thenReturn(emptyMetrics);

        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        
        Map<String, Object> responseBody = response.getBody();
        assertNotNull(responseBody);
        
        assertEquals(true, responseBody.get("optimization_enabled"));
        assertEquals(emptyMetrics, responseBody.get("optimized_metrics"));
        assertTrue(responseBody.get("timestamp") instanceof Long);
        
        verify(optimizedManager).getMetrics();
    }

    @Test
    void testGetMetricsTimestampIsRecent() {
        Map<String, Long> mockMetrics = new HashMap<>();
        mockMetrics.put("writes", 1L);
        when(optimizedManager.getMetrics()).thenReturn(mockMetrics);

        long beforeCall = System.currentTimeMillis();
        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();
        long afterCall = System.currentTimeMillis();

        Map<String, Object> responseBody = response.getBody();
        Long timestamp = (Long) responseBody.get("timestamp");

        assertTrue(timestamp >= beforeCall);
        assertTrue(timestamp <= afterCall);
    }

    @Test
    void testGetMetricsMultipleCallsHaveDifferentTimestamps() throws InterruptedException {
        Map<String, Long> mockMetrics = new HashMap<>();
        mockMetrics.put("writes", 1L);
        when(optimizedManager.getMetrics()).thenReturn(mockMetrics);

        ResponseEntity<Map<String, Object>> response1 = performanceController.getMetrics();
        
        // Small delay to ensure different timestamps
        Thread.sleep(10);
        
        ResponseEntity<Map<String, Object>> response2 = performanceController.getMetrics();

        Long timestamp1 = (Long) response1.getBody().get("timestamp");
        Long timestamp2 = (Long) response2.getBody().get("timestamp");

        assertTrue(timestamp2 > timestamp1);
        
        verify(optimizedManager, times(2)).getMetrics();
    }

    @Test
    void testGetMetricsHandlesAllMetricTypes() {
        // Test with all expected metric types
        Map<String, Long> completeMetrics = new HashMap<>();
        completeMetrics.put("writes", 100L);
        completeMetrics.put("reads", 50L);
        completeMetrics.put("conflicts", 2L);
        completeMetrics.put("cache_hits", 25L);
        completeMetrics.put("cache_misses", 75L);
        completeMetrics.put("queue_size", 5L);
        completeMetrics.put("avg_write_latency_ms", 5500L);
        completeMetrics.put("cache_hit_rate_percent", 25L);

        when(optimizedManager.getMetrics()).thenReturn(completeMetrics);

        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();
        Map<String, Object> responseBody = response.getBody();
        
        @SuppressWarnings("unchecked")
        Map<String, Long> returnedMetrics = (Map<String, Long>) responseBody.get("optimized_metrics");

        assertEquals(8, returnedMetrics.size());
        assertEquals(100L, returnedMetrics.get("writes"));
        assertEquals(50L, returnedMetrics.get("reads"));
        assertEquals(2L, returnedMetrics.get("conflicts"));
        assertEquals(25L, returnedMetrics.get("cache_hits"));
        assertEquals(75L, returnedMetrics.get("cache_misses"));
        assertEquals(5L, returnedMetrics.get("queue_size"));
        assertEquals(5500L, returnedMetrics.get("avg_write_latency_ms"));
        assertEquals(25L, returnedMetrics.get("cache_hit_rate_percent"));
    }

    @Test
    void testGetMetricsResponseStructure() {
        Map<String, Long> mockMetrics = new HashMap<>();
        mockMetrics.put("test_metric", 42L);
        when(optimizedManager.getMetrics()).thenReturn(mockMetrics);

        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();
        Map<String, Object> responseBody = response.getBody();

        // Verify the exact structure expected by the API consumers
        assertTrue(responseBody.containsKey("optimization_enabled"));
        assertTrue(responseBody.containsKey("optimized_metrics"));
        assertTrue(responseBody.containsKey("timestamp"));
        
        assertEquals(3, responseBody.size()); // Exactly 3 fields
        
        assertTrue(responseBody.get("optimization_enabled") instanceof Boolean);
        assertTrue(responseBody.get("optimized_metrics") instanceof Map);
        assertTrue(responseBody.get("timestamp") instanceof Long);
    }

    @Test
    void testOptimizationEnabledAlwaysTrue() {
        Map<String, Long> mockMetrics = new HashMap<>();
        when(optimizedManager.getMetrics()).thenReturn(mockMetrics);

        ResponseEntity<Map<String, Object>> response = performanceController.getMetrics();
        Map<String, Object> responseBody = response.getBody();

        assertEquals(true, responseBody.get("optimization_enabled"));
    }
}