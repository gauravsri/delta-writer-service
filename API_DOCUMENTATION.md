# Delta Writer Service - API Documentation

[![API Version](https://img.shields.io/badge/API%20Version-v3.0-blue.svg)]()
[![OpenAPI](https://img.shields.io/badge/OpenAPI-3.0-green.svg)]()

## Overview

The Delta Writer Service provides a REST API for high-performance write operations to Delta Lake tables. The API follows RESTful principles and supports JSON request/response format.

**Base URL**: `http://localhost:8080/api/v1`

## Authentication

Currently, the service operates without authentication in local/development mode. Production deployments should implement appropriate authentication mechanisms.

## Generic Entity Operations

### Create Entity

Create a single entity of any registered type.

```http
POST /api/v1/entities/{entityType}
Content-Type: application/json

{
  "field1": "value1",
  "field2": "value2"
}
```

**Path Parameters:**
- `entityType` (string): The type of entity to create (e.g., "users", "orders", "products")

**Request Body:** JSON object with entity fields matching the entity's Avro schema

**Response:**
```json
{
  "success": true,
  "entityType": "users",
  "operationType": "WRITE",
  "recordCount": 1,
  "message": "Entity saved successfully",
  "executionTimeMs": 245
}
```

### Create Entities Batch

Create multiple entities in a single batch operation for improved performance.

```http
POST /api/v1/entities/{entityType}/batch
Content-Type: application/json

{
  "entities": [
    {"field1": "value1", "field2": "value2"},
    {"field1": "value3", "field2": "value4"}
  ],
  "options": {
    "validationMode": "STRICT",
    "conflictResolution": "FAIL_ON_CONFLICT",
    "enableOptimizations": true
  }
}
```

**Request Body:**
- `entities` (array): Array of entity objects to create
- `options` (object, optional): Batch processing options
  - `validationMode`: "STRICT" | "LENIENT" (default: "STRICT")
  - `conflictResolution`: "FAIL_ON_CONFLICT" | "OVERWRITE" (default: "FAIL_ON_CONFLICT")
  - `enableOptimizations`: boolean (default: true)

**Response:**
```json
{
  "totalRequested": 100,
  "successCount": 98,
  "failureCount": 2,
  "successfulUserIds": ["user1", "user2", "..."],
  "failures": [
    {
      "userId": "user99",
      "index": 98,
      "error": "Validation failed: email required",
      "errorType": "VALIDATION_ERROR"
    }
  ],
  "processedAt": "2024-08-10T10:30:00Z",
  "processingTimeMs": 1250,
  "statistics": {
    "totalBatches": 5,
    "avgBatchSize": 20,
    "avgProcessingTimePerBatch": 250,
    "totalDeltaTransactionTime": 800,
    "deltaTransactionCount": 5
  }
}
```

### Get Supported Entity Types

Retrieve list of all supported entity types.

```http
GET /api/v1/entities
```

**Response:**
```json
{
  "supportedEntityTypes": ["users", "orders", "products"],
  "message": "Supported entity types for this API"
}
```

## Performance & Monitoring

### Get Performance Metrics

Retrieve comprehensive performance and operational metrics.

```http
GET /api/v1/performance/metrics
```

**Response:**
```json
{
  "optimization_enabled": true,
  "writes": 1547,
  "conflicts": 2,
  "queue_size": 15,
  "avg_write_latency_ms": 1575,
  "checkpoints_created": 87,
  "batch_consolidations": 234,
  "optimal_batch_size": 750,
  "configured_batch_timeout_ms": 1000,
  "configured_max_batch_size": 1000,
  "configured_max_retries": 3,
  "configured_checkpoint_interval": 10,
  "s3a_optimizations_enabled": true,
  "parquet_block_size_mb": 256,
  "connection_pool_size": 200,
  "schema_cache_stats": {
    "cached_schemas": 5,
    "schema_names": ["users", "orders", "products", "customers", "payments"],
    "cache_hit_rate": 0.94,
    "total_requests": 1200,
    "cache_hits": 1128,
    "cache_misses": 72
  }
}
```

### Health Check

Standard health check endpoint for monitoring and load balancers.

```http
GET /actuator/health
```

**Response:**
```json
{
  "status": "UP",
  "components": {
    "deltaStore": {
      "status": "UP",
      "details": {
        "storage_backend": "S3A",
        "connection_pool_health": "HEALTHY",
        "schema_manager": "OPERATIONAL"
      }
    },
    "diskSpace": {
      "status": "UP"
    },
    "db": {
      "status": "UP"
    }
  }
}
```

## Error Responses

### Standard Error Format

All API errors follow a consistent format:

```json
{
  "success": false,
  "error": "Error message",
  "errorType": "VALIDATION_ERROR",
  "timestamp": "2024-08-10T10:30:00Z",
  "path": "/api/v1/entities/users",
  "details": {
    "field": "email",
    "reason": "Invalid email format"
  }
}
```

### HTTP Status Codes

- `200 OK`: Request successful
- `201 Created`: Entity created successfully  
- `206 Partial Content`: Batch operation completed with some failures
- `400 Bad Request`: Invalid request format or validation errors
- `404 Not Found`: Entity type not supported
- `500 Internal Server Error`: Server-side processing error
- `503 Service Unavailable`: Service temporarily unavailable

### Common Error Types

- `VALIDATION_ERROR`: Entity validation failed
- `SCHEMA_ERROR`: Schema compatibility issues
- `STORAGE_ERROR`: Storage backend connectivity issues
- `CAPACITY_ERROR`: System capacity limits exceeded
- `TIMEOUT_ERROR`: Operation timeout

## Rate Limits

The API implements rate limiting to ensure system stability:

- **Per IP**: 1000 requests per minute
- **Per Entity Type**: 10,000 operations per minute  
- **Batch Operations**: 100 batches per minute

Rate limit headers are included in responses:
```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1691661000
```

## Best Practices

### Batch Operations

1. **Use batch endpoints** for multiple entities to improve performance
2. **Optimal batch size**: 100-1000 entities per batch
3. **Handle partial failures**: Always check the response for failed entities

### Performance Optimization

1. **Entity partitioning**: Design partition columns for query patterns
2. **Schema evolution**: Use backward-compatible schema changes when possible  
3. **Connection pooling**: Reuse connections for high-throughput scenarios

### Error Handling

1. **Implement retry logic** with exponential backoff for transient errors
2. **Validate entities client-side** before sending to reduce server load
3. **Monitor rate limits** and implement client-side throttling

## SDK Examples

### Java SDK

```java
// Create DeltaWriterClient
DeltaWriterClient client = DeltaWriterClient.builder()
    .baseUrl("http://localhost:8080")
    .build();

// Create single entity
User user = new User("user123", "john@example.com", "John Doe");
CreateEntityResponse response = client.createEntity("users", user);

// Create batch
List<User> users = Arrays.asList(user1, user2, user3);
BatchCreateRequest request = BatchCreateRequest.builder()
    .entities(users)
    .validationMode(ValidationMode.STRICT)
    .build();
    
BatchCreateResponse batchResponse = client.createBatch("users", request);
```

### cURL Examples

```bash
# Create single entity
curl -X POST "http://localhost:8080/api/v1/entities/users" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user123","username":"johndoe","email":"john@example.com"}'

# Create batch
curl -X POST "http://localhost:8080/api/v1/entities/users/batch" \
  -H "Content-Type: application/json" \
  -d '{
    "entities": [
      {"user_id":"user1","username":"john1","email":"john1@example.com"},
      {"user_id":"user2","username":"john2","email":"john2@example.com"}
    ]
  }'

# Get metrics  
curl -X GET "http://localhost:8080/api/v1/performance/metrics"
```

### Python SDK

```python
import requests

# Create DeltaWriter client
class DeltaWriterClient:
    def __init__(self, base_url):
        self.base_url = base_url
    
    def create_entity(self, entity_type, entity_data):
        response = requests.post(
            f"{self.base_url}/api/v1/entities/{entity_type}",
            json=entity_data
        )
        return response.json()
    
    def create_batch(self, entity_type, entities):
        response = requests.post(
            f"{self.base_url}/api/v1/entities/{entity_type}/batch",
            json={"entities": entities}
        )
        return response.json()

# Usage
client = DeltaWriterClient("http://localhost:8080")
response = client.create_entity("users", {
    "user_id": "user123",
    "username": "johndoe", 
    "email": "john@example.com"
})
```

## OpenAPI Specification

The complete OpenAPI 3.0 specification is available at:
- **Development**: `http://localhost:8080/v3/api-docs`
- **Swagger UI**: `http://localhost:8080/swagger-ui.html`

## Support

For API support and questions:
- **Documentation**: [README.md](./README.md)
- **GitHub Issues**: [Issues](https://github.com/your-org/delta-writer-service/issues)
- **Email**: api-support@delta-writer.com

---

**Last Updated**: August 2024 | **API Version**: v3.0