package com.example.deltastore.storage;

import io.delta.kernel.DeltaLog;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.internal.InternalSnapshotUtils;
import io.delta.kernel.utils.FileStatus;
import io.delta.storage.LogStore;
import io.delta.storage.StorageFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import io.delta.kernel.Transaction;
import io.delta.kernel.actions.Action;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;


@Service
public class DeltaTableManagerImpl implements DeltaTableManager {

    private final String bucketName;
    private final Configuration hadoopConf;
    private final Engine engine;

    public DeltaTableManagerImpl(
            @Value("${app.storage.bucket-name}") String bucketName,
            @Value("${app.storage.endpoint}") String endpoint,
            @Value("${app.storage.access-key}") String accessKey,
            @Value("${app.storage.secret-key}") String secretKey) {
        this.bucketName = bucketName;
        this.hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.endpoint", endpoint);
        hadoopConf.set("fs.s3a.access.key", accessKey);
        hadoopConf.set("fs.s3a.secret.key", secretKey);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        this.engine = DefaultEngine.create(hadoopConf);
    }

    @Override
    public void write(String tableName, List<GenericRecord> records, Schema schema) {
        String tablePath = "s3a://" + bucketName + "/" + tableName;
        DeltaLog log = DeltaLog.forTable(engine, tablePath);
        Snapshot snapshot = log.snapshot();
        Transaction txn = log.startTransaction();

        long commitTime = System.currentTimeMillis();
        String fileName = UUID.randomUUID().toString() + ".parquet";
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(tablePath, fileName);

        try {
            // 1. Write the data to a new Parquet file
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                    .withSchema(schema)
                    .withConf(hadoopConf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (GenericRecord record : records) {
                    writer.write(record);
                }
            }

            // 2. Get the status of the new file to create the AddFile action
            FileStatus fileStatus = Utils.getFileStatus(engine.getFileSystemClient(), hadoopPath.toString());

            AddFile addFile = new AddFile(
                fileName,
                Collections.emptyMap(), // partition values
                fileStatus.getSize(),
                commitTime,
                true, // dataChange
                null, // stats
                null // tags
            );

            // 3. Commit the new file to the Delta log
            List<Action> actions = Collections.singletonList(addFile);
            txn.commit(actions, "APPEND", "java-api");

        } catch (IOException e) {
            throw new RuntimeException("Failed to write to Delta table", e);
        }
    }
}
