package com.example.deltastore.api.controller;

import com.example.deltastore.storage.OptimizedDeltaTableManager;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Performance Monitoring", description = "API for monitoring system performance and optimization metrics")
public class PerformanceController {
    
    @Autowired(required = false)
    @Qualifier("optimized")
    private OptimizedDeltaTableManager optimizedManager;
    
    @Operation(
            summary = "Get performance metrics",
            description = "Retrieves comprehensive performance and operational metrics including write latencies, batch processing stats, and system optimization details."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved performance metrics",
                    content = @Content(mediaType = "application/json",
                            examples = @ExampleObject(value = "{\n  \"optimization_enabled\": true,\n  \"optimized_metrics\": {\n    \"writes\": 1547,\n    \"avg_write_latency_ms\": 1575,\n    \"checkpoints_created\": 87,\n    \"batch_consolidations\": 234\n  },\n  \"timestamp\": 1691661000000\n}")))
    })
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