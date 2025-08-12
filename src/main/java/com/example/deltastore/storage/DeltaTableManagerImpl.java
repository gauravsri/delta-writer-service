package com.example.deltastore.storage;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import com.example.deltastore.config.StorageProperties;
import com.example.deltastore.exception.TableWriteException;
import com.example.deltastore.util.DeltaKernelBatchOperations;
import com.example.deltastore.schema.DeltaSchemaManager;
import io.delta.kernel.DataWriteContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Service
@Slf4j
public class DeltaTableManagerImpl implements DeltaTableManager {

    private final StorageProperties storageProperties;
    private final DeltaSchemaManager deltaSchemaManager;
    private Configuration hadoopConf;
    private final Engine engine;

    public DeltaTableManagerImpl(StorageProperties storageProperties, DeltaSchemaManager deltaSchemaManager) {
        this.storageProperties = storageProperties;
        this.deltaSchemaManager = deltaSchemaManager;
        this.hadoopConf = new Configuration();
        
        log.info("Initializing Delta Table Manager for bucket: {}", storageProperties.getBucketName());
        log.debug("Using endpoint: {} with access key: {}", 
                 storageProperties.getEndpoint(), 
                 storageProperties.getMaskedAccessKey());
        
        // Configure S3A for MinIO following Delta Kernel 4.0.0 documentation
        String endpoint = storageProperties.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            log.info("Configuring S3A for MinIO following Delta Kernel documentation");
            
            // MinIO endpoint configuration
            hadoopConf.set("fs.s3a.endpoint", endpoint);
            hadoopConf.set("fs.s3a.access.key", storageProperties.getAccessKey());
            hadoopConf.set("fs.s3a.secret.key", storageProperties.getSecretKey());
            
            // Enable path style access (required for MinIO)
            hadoopConf.set("fs.s3a.path.style.access", "true");
            
            // S3A FileSystem implementation
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            
            // Disable SSL (for local MinIO)
            hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
            
            // Additional S3A configurations for better performance (per documentation)
            hadoopConf.set("fs.s3a.connection.maximum", "100");
            hadoopConf.set("fs.s3a.threads.max", "10");
            hadoopConf.set("fs.s3a.connection.establish.timeout", "5000");
            hadoopConf.set("fs.s3a.connection.timeout", "10000");
            
            // Buffer settings (per documentation)
            hadoopConf.set("fs.s3a.buffer.dir", "/tmp");
            hadoopConf.set("fs.s3a.fast.upload", "true");
            hadoopConf.set("fs.s3a.fast.upload.buffer", "disk");
            
            // Credentials provider
            hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            
            log.info("✓ Configured S3A filesystem for MinIO endpoint: {}", endpoint);
        } else {
            // Fallback to local filesystem for unit tests
            hadoopConf.set("fs.default.name", "file:///");
            hadoopConf.set("fs.defaultFS", "file:///");
            log.info("Using local filesystem (no endpoint configured)");
        }

        this.engine = DefaultEngine.create(hadoopConf);
    }

    @Override
    public void write(String tableName, List<GenericRecord> records, Schema schema) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (records == null || records.isEmpty()) {
            log.warn("No records to write for table: {}", tableName);
            return;
        }
        
        log.info("Writing {} records to table: {}", records.size(), tableName);
        
        // Determine path based on configuration
        String tablePath = getTablePath(tableName);
        
        // Use Delta Kernel 4.0.0 APIs for proper Delta Lake writes
        try {
            // Note: S3/MinIO will auto-create directories when writing files
            log.debug("Preparing to write to Delta table at: {}", tablePath);

            // Convert Avro schema to Delta StructType
            StructType deltaSchema = convertAvroSchemaToDelta(schema);
            
            // Create or get existing Delta table
            Table table;
            boolean isNewTable = !doesTableExist(tablePath);
            
            if (isNewTable) {
                // Create new Delta table using Transaction API
                table = Table.forPath(engine, tablePath);
                TransactionBuilder txnBuilder = table.createTransactionBuilder(
                    engine, 
                    "Delta Writer Service v1.0", 
                    Operation.CREATE_TABLE
                );
                
                // Set schema for new table
                txnBuilder = txnBuilder.withSchema(engine, deltaSchema);
                
                // Build and prepare transaction
                Transaction txn = txnBuilder.build(engine);
                
                // Convert records to Delta format and commit
                writeDeltaRecords(txn, engine, records, schema);
                
                log.info("Successfully created new Delta table: {} with {} records", tableName, records.size());
            } else {
                // Append to existing Delta table
                table = Table.forPath(engine, tablePath);
                TransactionBuilder txnBuilder = table.createTransactionBuilder(
                    engine, 
                    "Delta Writer Service v1.0", 
                    Operation.WRITE
                );
                
                // Build and prepare transaction
                Transaction txn = txnBuilder.build(engine);
                
                // Convert records to Delta format and commit
                writeDeltaRecords(txn, engine, records, schema);
                
                log.info("Successfully appended {} records to existing Delta table: {}", records.size(), tableName);
            }
            
        } catch (Exception e) {
            throw new TableWriteException("Unexpected error writing to Delta table: " + tableName, e);
        }
    }


    
    /**
     * Check if a Delta table exists at the given path
     */
    private boolean doesTableExist(String tablePath) {
        try {
            Table table = Table.forPath(engine, tablePath);
            table.getLatestSnapshot(engine);
            return true;
        } catch (Exception e) {
            log.debug("Table does not exist at path: {}", tablePath);
            return false;
        }
    }
    
    /**
     * Get the table path based on storage configuration
     */
    private String getTablePath(String tableName) {
        String endpoint = storageProperties.getEndpoint();
        String bucketName = storageProperties.getBucketName();
        
        if (endpoint != null && !endpoint.isEmpty()) {
            // S3A path for MinIO/S3
            String s3Path = "s3a://" + bucketName + "/" + tableName;
            log.debug("Using S3A path: {}", s3Path);
            return s3Path;
        } else {
            // Local filesystem path for unit tests
            String localPath = "/tmp/delta-tables/" + bucketName + "/" + tableName;
            log.debug("Using local filesystem path: {}", localPath);
            return localPath;
        }
    }
    
    /**
     * Convert Avro Schema to Delta StructType using proper schema manager
     */
    private StructType convertAvroSchemaToDelta(Schema avroSchema) {
        return deltaSchemaManager.getOrCreateDeltaSchema(avroSchema);
    }
    
    /**
     * Write records using complete Delta Kernel approach following kernel_help.md
     */
    private void writeDeltaRecords(Transaction txn, Engine engine, List<GenericRecord> records, Schema schema) {
        try {
            Row txnState = txn.getTransactionState(engine);
            
            if (records.isEmpty()) {
                io.delta.kernel.utils.CloseableIterable<Row> dataActions = 
                    io.delta.kernel.utils.CloseableIterable.emptyIterable();
                TransactionCommitResult result = txn.commit(engine, dataActions);
                log.info("Delta transaction committed successfully at version: {} (empty)", result.getVersion());
                return;
            }
            
            log.info("Writing {} Avro records using complete Delta Kernel approach", records.size());
            
            // Step 1: Infer schema from Avro records
            StructType deltaSchema = DeltaKernelBatchOperations.inferSchemaFromAvroRecords(records);
            
            // Step 2: Create FilteredColumnarBatch from Avro records (following kernel_help.md)
            FilteredColumnarBatch recordBatch = DeltaKernelBatchOperations.createBatchFromAvroRecords(records, deltaSchema);
            CloseableIterator<FilteredColumnarBatch> data = createCloseableIterator(
                java.util.Collections.singletonList(recordBatch).iterator());
            
            // Step 3: Set partition values (empty for non-partitioned table)
            Map<String, io.delta.kernel.expressions.Literal> partitionValues = new HashMap<>();
            
            // Step 4: Transform logical data to physical data (following kernel_help.md pattern)
            CloseableIterator<FilteredColumnarBatch> physicalData = 
                io.delta.kernel.Transaction.transformLogicalData(engine, txnState, data, partitionValues);
            
            // Step 5: Get write context (following kernel_help.md pattern)
            DataWriteContext writeContext = 
                io.delta.kernel.Transaction.getWriteContext(engine, txnState, partitionValues);
            
            // Step 6: Write Parquet files using Delta Kernel's handler (following kernel_help.md pattern)
            CloseableIterator<io.delta.kernel.utils.DataFileStatus> dataFiles = engine.getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns()
                );
            
            // Step 7: Generate AddFile actions properly (following kernel_help.md pattern)
            CloseableIterator<Row> dataActions = io.delta.kernel.Transaction.generateAppendActions(
                engine, txnState, dataFiles, writeContext);
            
            // Step 8: Convert to list and commit
            List<Row> actionsList = new ArrayList<>();
            while (dataActions.hasNext()) {
                actionsList.add(dataActions.next());
            }
            dataActions.close();
            
            io.delta.kernel.utils.CloseableIterable<Row> dataActionsIterable = 
                io.delta.kernel.utils.CloseableIterable.inMemoryIterable(createCloseableIterator(actionsList.iterator()));
            
            TransactionCommitResult result = txn.commit(engine, dataActionsIterable);
            
            log.info("✅ Complete Delta Kernel write successful at version: {} with {} records", 
                result.getVersion(), records.size());
            
        } catch (Exception e) {
            log.error("Failed to write records using complete Delta Kernel approach", e);
            throw new RuntimeException("Failed to write Delta records using complete Delta Kernel approach", e);
        }
    }
    
    
    
    /**
     * Helper method to create CloseableIterator from any Iterator
     */
    private <T> CloseableIterator<T> createCloseableIterator(java.util.Iterator<T> iterator) {
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }
            
            @Override
            public T next() {
                return iterator.next();
            }
            
            @Override
            public void close() throws IOException {
                // Nothing to close for in-memory iterator
            }
        };
    }
    
    
}