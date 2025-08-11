package com.example.deltastore.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
@JsonDeserialize(builder = BatchCreateResponse.BatchCreateResponseBuilder.class)
@Schema(description = "Response object for batch create operations containing success/failure counts and detailed statistics")
public class BatchCreateResponse {
    
    @JsonPOJOBuilder(withPrefix = "")
    public static class BatchCreateResponseBuilder {
        // Lombok will generate the builder methods
    }
    
    @Schema(description = "Total number of entities requested for creation", example = "100")
    private int totalRequested;
    
    @Schema(description = "Number of entities successfully created", example = "98")
    private int successCount;
    
    @Schema(description = "Number of entities that failed to be created", example = "2")
    private int failureCount;
    
    private List<String> successfulUserIds;
    private List<FailureDetail> failures;
    private List<String> errors; // For compatibility with error responses
    
    private LocalDateTime processedAt;
    private long processingTimeMs;
    
    // Statistics
    private BatchStatistics statistics;
    
    @Data
    @Builder
    @JsonDeserialize(builder = FailureDetail.FailureDetailBuilder.class)
    public static class FailureDetail {
        
        @JsonPOJOBuilder(withPrefix = "")
        public static class FailureDetailBuilder {
            // Lombok will generate the builder methods
        }
        private String userId;
        private int index;
        private String error;
        private String errorType;
    }
    
    @Data
    @Builder
    @JsonDeserialize(builder = BatchStatistics.BatchStatisticsBuilder.class)
    public static class BatchStatistics {
        
        @JsonPOJOBuilder(withPrefix = "")
        public static class BatchStatisticsBuilder {
            // Lombok will generate the builder methods
        }
        private int totalBatches;
        private int avgBatchSize;
        private long avgProcessingTimePerBatch;
        private long totalDeltaTransactionTime;
        private int deltaTransactionCount;
        private Map<String, Object> additionalMetrics;
    }
}