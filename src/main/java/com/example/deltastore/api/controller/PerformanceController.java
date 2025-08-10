package com.example.deltastore.api.controller;

import com.example.deltastore.storage.OptimizedDeltaTableManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.HashMap;

/**
 * Performance monitoring and testing controller
 */
@RestController
@RequestMapping("/api/v1/performance")
@Slf4j
public class PerformanceController {
    
    @Autowired(required = false)
    @Qualifier("optimized")
    private OptimizedDeltaTableManager optimizedManager;
    
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> response = new HashMap<>();
        
        if (optimizedManager != null) {
            response.put("optimized_metrics", optimizedManager.getMetrics());
            response.put("optimization_enabled", true);
        } else {
            response.put("optimization_enabled", false);
            response.put("message", "Using standard Delta manager");
        }
        
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/toggle-optimization")
    public ResponseEntity<Map<String, String>> toggleOptimization(@RequestParam boolean enable) {
        // This would require dynamic bean switching in production
        Map<String, String> response = new HashMap<>();
        response.put("status", enable ? "Optimization enabled" : "Optimization disabled");
        response.put("note", "Restart required for changes to take effect");
        
        return ResponseEntity.ok(response);
    }
}