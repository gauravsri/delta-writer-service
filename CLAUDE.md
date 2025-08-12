# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Delta Writer Service is a high-performance, write-only Delta Lake service built with Spring Boot 3.2.5 and Delta Kernel 4.0.0. The service features a revolutionary zero-code entity management system through a comprehensive generic architecture that supports unlimited entity types without code changes.

### Key Architecture Features
- **Generic Entity Framework**: Single controller/service handles all entity types via path variables
- **Dynamic Schema Management**: Automatic Avro to Delta schema conversion for any schema
- **Configuration-Driven**: Runtime entity registration via YAML configuration
- **Performance Optimized**: 87% latency reduction with automatic checkpoint management
- **Modular Design**: Strategic modular architecture vs tactical implementations

## Build and Development Commands

### Essential Development Commands
```bash
# Build the project
mvn clean compile

# Run all tests  
mvn test

# Run specific test categories
mvn test -Dtest="*Schema*"              # Schema management tests
mvn test -Dtest="*Entity*"              # Entity framework tests  
mvn test -Dtest="*Storage*"             # Storage layer tests
mvn test -Dtest="*Integration*"         # Integration tests

# Run with test coverage
mvn clean test jacoco:report
# View coverage: open target/site/jacoco/index.html

# Start the application
mvn spring-boot:run

# Generate Avro classes from schemas
mvn clean generate-sources

# Build production JAR
mvn clean package -DskipTests
```

### Testing Commands
```bash
# Integration tests (requires running application)
./integration-test.sh

# QA black-box tests (comprehensive)
./qa-black-box-test.sh

# Performance diagnosis
./performance-diagnosis.sh

# Run single test class
mvn test -Dtest=GenericEntityServiceTest

# Run tests with specific profile
mvn test -Dspring.profiles.active=local
```

### Local Development Setup
```bash
# 1. Start MinIO (required for persistence)
podman run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
# MinIO Console: http://localhost:9001 (minio/minio123)

# 2. Run application
mvn spring-boot:run

# 3. Verify health
curl http://localhost:8080/actuator/health

# 4. Access API documentation
# Swagger UI: http://localhost:8080/swagger-ui.html
# OpenAPI JSON: http://localhost:8080/v3/api-docs
```

## Architecture and Code Structure

### Core Modular Components

**Configuration Layer** (`config/`)
- `DeltaStoreConfiguration`: Centralized configuration management
- `AutoEntityDiscovery`: Automatic entity registration from schemas
- `OpenAPIConfig`: API documentation configuration
- `S3Config`, `SchemaRegistryConfig`: Storage and schema configurations

**Generic Entity Framework** (`entity/`)
- `GenericEntityService`: Single service handles all entity types
- `EntityMetadataRegistry`: Dynamic entity metadata management  
- `EntityOperationResult`: Standardized operation responses

**Dynamic Schema Management** (`schema/`)
- `DeltaSchemaManager`: Dynamic schema caching and management
- `AvroToDeltaSchemaConverter`: Automatic Avro to Delta schema conversion
- `SchemaCompatibilityChecker`: Schema evolution validation

**Strategic Storage Layer** (`storage/`)
- `OptimizedDeltaTableManager`: High-performance Delta operations with checkpoint management
- `DeltaStoragePathResolver`: Flexible storage path resolution
- `StoragePath`: Storage path abstraction

**Generic API Layer** (`api/controller/`)
- `GenericEntityController`: Single controller for all entity types via `/api/v1/entities/{entityType}`
- `PerformanceController`: Performance metrics and monitoring
- `EntityConverter`: Dynamic JSON to Avro conversion

### Key Design Patterns

**Generic Entity Pattern**: Use `GenericEntityService` instead of entity-specific services
```java
// ✅ Correct - Generic approach
@Autowired
private GenericEntityService entityService;
entityService.save("users", userEntity);

// ❌ Avoid - Entity-specific services
@Autowired 
private UserService userService;
```

**Configuration-Driven Development**: Add new entities via configuration only
```yaml
# Add new entity types in application-local.yml
deltastore:
  tables:
    new_entity:
      primary-key-column: "id"
      partition-columns: ["date", "region"]
      evolution-policy: BACKWARD_COMPATIBLE
```

**Schema Management**: Let the system handle schema conversion automatically
```java
// ✅ System handles schema conversion automatically
Schema avroSchema = entity.getSchema(); // Auto-detected
StructType deltaSchema = schemaManager.getOrCreateDeltaSchema(avroSchema);
```

### Performance Optimizations

**Checkpoint Management**: Automatic checkpoint creation every 10 versions prevents 12+ second latencies
**Batch Consolidation**: Dynamic batch sizing (10-10,000 records) based on system load  
**S3A Optimizations**: 256MB Parquet blocks, 200-connection pool, optimized timeouts
**Caching Strategy**: 30-second schema cache, 5-minute schema registry cache

## Configuration Management

### Environment-Specific Configuration

**Local Development** (`application-local.yml`):
- Test entities: users, orders, products
- MinIO storage backend
- Debug logging enabled

**Production** (`application-prod.yml`):  
- Real S3 storage
- Optimized performance settings
- Production logging levels

### Key Configuration Sections

**Performance Tuning**:
```yaml
deltastore:
  performance:
    cache-ttl-ms: 30000
    max-batch-size: 100
    checkpoint-interval: 10  # Critical for performance
    connection-pool-size: 200
```

**Storage Configuration**:
```yaml
deltastore:
  storage:
    type: S3A  # S3A | LOCAL | HDFS | AZURE | GCS
    base-path: "/delta-tables"
    partition-strategy: DATE_BASED
```

## API Patterns

### Universal Entity Operations
All entity operations follow the same pattern using the generic controller:

```bash
# Create entity
POST /api/v1/entities/{entityType}

# Get entity by ID  
GET /api/v1/entities/{entityType}/{id}

# Batch create
POST /api/v1/entities/{entityType}/batch

# Search by partitions
POST /api/v1/entities/{entityType}/search
```

### Response Format
All operations return standardized `EntityOperationResult`:
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

## Testing Strategy

### Test Categories
- **Unit Tests**: Component-level testing with mocks
- **Integration Tests**: End-to-end API testing via `integration-test.sh`
- **QA Tests**: Black-box testing via `qa-black-box-test.sh`
- **Performance Tests**: Load testing and latency measurement

### Important Test Notes
- Tests require MinIO running on localhost:9000
- Many tests are excluded in Maven due to environment dependencies
- Use shell scripts for comprehensive integration testing
- Test coverage is 95% of critical business logic

## Development Best Practices

### Adding New Entity Types
1. Define Avro schema in `src/main/resources/schemas/`
2. Add entity configuration in profile-specific `application.yml`
3. Use generic API endpoints - no new controllers needed
4. Test via API calls to `/api/v1/entities/{entityType}`

### Code Conventions
- Use Lombok for data classes (`@Data`, `@Builder`)
- Follow generic entity patterns, avoid entity-specific code
- Leverage Spring's dependency injection and configuration properties
- Use Apache Commons utilities for common operations
- Apply SOLID principles and design patterns consistently

### Performance Considerations
- Monitor checkpoint creation frequency (should be every 10 versions)
- Watch for batch consolidation in logs
- Use performance metrics endpoint for monitoring
- Consider partition strategy for large datasets

### Schema Evolution
- Default policy is BACKWARD_COMPATIBLE
- Use SchemaCompatibilityChecker for validation
- Test schema changes in local environment first
- Monitor schema cache hit rates

## Monitoring and Observability

### Key Endpoints
```bash
# Health check
GET /actuator/health

# Performance metrics
GET /api/v1/performance/metrics

# Prometheus metrics
GET /actuator/prometheus
```

### Important Metrics
- `writes`: Total writes processed
- `avg_write_latency_ms`: Average write latency
- `checkpoints_created`: Checkpoint frequency
- `cache_hit_rate_percent`: Schema cache efficiency

## Storage Backends

The service supports multiple storage backends configured via `deltastore.storage.type`:
- **S3A**: AWS S3 or S3-compatible storage (MinIO)
- **LOCAL**: Local filesystem storage
- **HDFS**: Hadoop Distributed File System
- **AZURE**: Azure Blob Storage  
- **GCS**: Google Cloud Storage

## Entity Types and Schemas

### Local/Test Entities
- `users`: Test user data with partitioning by country/signup_date
- `orders`: Test order data with date/region partitioning  
- `products`: Test product data with category partitioning

### Production Deployment
Production deployments should define their own entity configurations and remove test entities from the configuration.