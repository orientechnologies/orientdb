package com.orientechnologies.backup.uploader;

import static org.junit.Assert.*;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import io.findify.s3mock.S3Mock;
import java.io.File;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OS3DeltaUploadingStrategyTest {
  private OrientDB orientDB;
  private final String dbURL = "plocal:target/upload-s3";
  private final String dbName = "upload-s3";
  private final int maxRetries = 5;
  private final int count = 10;
  private String backupLocation;

  private S3Mock s3Api;
  private AmazonS3 s3Client;

  private final String backupDirPath = "orientdb-backups";
  private final String dbBucketName = "orientdb-databases-backup";
  private final String fileBucketName = "orientdb-file-backup";
  private final String backupPath = "target/backup/";

  @Test
  public void test() {
    OS3DeltaUploadingStrategy strategy = new OS3DeltaUploadingStrategy();
    OUploadMetadata metadata =
        strategy.executeS3FileUpload(s3Client, fileBucketName, backupLocation, "backup-files");
    assertNotNull(metadata);
    assertTrue(strategy.executeS3Upload(s3Client, dbBucketName, backupPath, backupDirPath));
    assertNotNull(strategy.executeS3Download(s3Client, dbBucketName, backupDirPath));
  }

  @Before
  public void setUp() {
    s3Api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
    s3Api.start();
    EndpointConfiguration endpoint =
        new EndpointConfiguration("http://localhost:8001", "us-west-2");
    s3Client =
        AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    // Create DB and schema
    orientDB = new OrientDB(dbURL, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);
    // TODO: create admin user
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      /* Create schema */
      OClass personClass = session.createVertexClass("Person");
      personClass.createProperty("id", OType.STRING);
      for (int i = 0; i < count; i++) {
        final int vertexId = i;
        session.executeWithRetry(
            maxRetries,
            oDatabaseSession -> {
              OVertex v = session.newVertex("Person");
              v.setProperty("id", vertexId);
              v.save();
              return null;
            });
      }
      String fileBackupName = session.incrementalBackup(this.backupPath);
      backupLocation = Paths.get(backupPath, fileBackupName).toString();
    }
  }

  @After
  public void tearDown() {
    if (s3Api != null) s3Api.shutdown();
    orientDB.drop(dbName);
    orientDB.close();
    OFileUtils.deleteRecursively(new File(this.backupPath));
  }
}
