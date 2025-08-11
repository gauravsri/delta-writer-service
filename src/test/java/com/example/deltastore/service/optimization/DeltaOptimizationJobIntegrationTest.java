package com.example.deltastore.service.optimization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class DeltaOptimizationJobIntegrationTest {

    private DeltaOptimizationJob optimizationJob;

    @BeforeEach
    void setUp() {
        optimizationJob = new DeltaOptimizationJob();
    }

    @Test
    void testConstructor() {
        assertNotNull(optimizationJob);
    }

    @Test
    void testRunDeltaMaintenance() {
        // This would normally be triggered by Spring's @Scheduled annotation
        // For testing, we invoke the method directly
        assertDoesNotThrow(() -> optimizationJob.runDeltaMaintenance());
        
        // The method should complete without throwing exceptions
        // In a real implementation, this would trigger delta table optimization
    }

    @Test
    void testOptimizationJobBehavior() {
        // Test that the optimization job can be called multiple times
        assertDoesNotThrow(() -> {
            optimizationJob.runDeltaMaintenance();
            optimizationJob.runDeltaMaintenance();
            optimizationJob.runDeltaMaintenance();
        });
    }

    @Test
    void testToString() {
        String result = optimizationJob.toString();
        assertNotNull(result);
        assertTrue(result.contains("DeltaOptimizationJob"));
    }

    @Test
    void testOptimizationJobState() {
        // Test that different instances are created properly
        DeltaOptimizationJob job1 = new DeltaOptimizationJob();
        DeltaOptimizationJob job2 = new DeltaOptimizationJob();
        
        // Both should be properly instantiated
        assertNotNull(job1);
        assertNotNull(job2);
        
        // Both should be able to run maintenance
        assertDoesNotThrow(() -> job1.runDeltaMaintenance());
        assertDoesNotThrow(() -> job2.runDeltaMaintenance());
    }
}