package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 6/26/13
 */
@Test
public class LocalPaginatedStorageSmallCacheBigRecordsCrashRestore {
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;

  private File                buildDir;
  private final AtomicLong    idGen           = new AtomicLong();

  private ExecutorService     executorService = Executors.newCachedThreadPool();
  private Process             process;

  @BeforeClass
  public void beforeClass() throws Exception {
    OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
    OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(0);
    OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
    OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(0);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageSmallCacheBigRecordsCrashRestore";

    buildDir = new File(buildDirectory);
    if (buildDir.exists())
      buildDir.delete();

    buildDir.mkdir();

    String javaExec = System.getProperty("java.home") + "/bin/java";
    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, RemoteDBRunner.class.getName());
    processBuilder.inheritIO();

    process = processBuilder.start();

    Thread.sleep(5000);
  }

  @AfterClass
  public void afterClass() {
    testDocumentTx.drop();
    baseDocumentTx.drop();

    Assert.assertTrue(buildDir.delete());
  }

  @BeforeMethod
  public void beforeMethod() {
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath()
        + "/baseLocalPaginatedStorageSmallCacheBigRecordsCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageSmallCacheBigRecordsCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  public void testDocumentCreation() throws Exception {
    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 2; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

    Thread.sleep(900000);

    long lastTs = System.currentTimeMillis();
    process.destroy();

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath()
        + "/testLocalPaginatedStorageSmallCacheBigRecordsCrashRestore");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    testDocumentTx.open("admin", "admin");
    compareDocuments(lastTs);
  }

  private void createSchema(ODatabaseDocumentTx dbDocumentTx) {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbDocumentTx);

    OSchema schema = dbDocumentTx.getMetadata().getSchema();
    if (!schema.existsClass("TestClass")) {
      OClass testClass = schema.createClass("TestClass");
      testClass.createProperty("id", OType.LONG);
      testClass.createProperty("timestamp", OType.LONG);
      testClass.createProperty("stringValue", OType.STRING);
      testClass.createProperty("binaryValue", OType.BINARY);

      testClass.createIndex("idIndex", OClass.INDEX_TYPE.UNIQUE, "id");

      schema.save();
    }
  }

  private void compareDocuments(long lastTs) {
    long minTs = Long.MAX_VALUE;
    int clusterId = baseDocumentTx.getClusterIdByName("TestClass");

    OStorage baseStorage = baseDocumentTx.getStorage();

    OPhysicalPosition[] physicalPositions = baseStorage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(
        OClusterPositionFactory.INSTANCE.valueOf(0)));

    int recordsRestored = 0;
    int recordsTested = 0;
    while (physicalPositions.length > 0) {
      final ORecordId rid = new ORecordId(clusterId);

      for (OPhysicalPosition physicalPosition : physicalPositions) {
        rid.clusterPosition = physicalPosition.clusterPosition;

        ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
        ODocument baseDocument = baseDocumentTx.load(rid);

        ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
        List<ODocument> testDocuments = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = "
            + baseDocument.field("id")));
        if (testDocuments.size() == 0) {
          if (((Long) baseDocument.field("timestamp")) < minTs)
            minTs = baseDocument.field("timestamp");
        } else {
          ODocument testDocument = testDocuments.get(0);
          Assert.assertEquals(testDocument.field("id"), baseDocument.field("id"));
          Assert.assertEquals(testDocument.field("timestamp"), baseDocument.field("timestamp"));
          Assert.assertEquals(testDocument.field("stringValue"), baseDocument.field("stringValue"));
          Assert.assertEquals(testDocument.field("binaryValue"), baseDocument.field("binaryValue"));
          recordsRestored++;
        }

        recordsTested++;

        if (recordsTested % 10000 == 0)
          System.out.println(recordsTested + " were tested, " + recordsRestored + " were restored ...");
      }

      physicalPositions = baseStorage.higherPhysicalPositions(clusterId, physicalPositions[physicalPositions.length - 1]);
    }

    System.out.println(recordsRestored + " records were restored. Total records " + recordsTested
        + ". Max interval for lost records " + (lastTs - minTs));
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
      OGlobalConfiguration.CACHE_LEVEL1_SIZE.setValue(0);
      OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
      OGlobalConfiguration.CACHE_LEVEL2_SIZE.setValue(0);
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(512);
      OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.setValue(100);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-create-big-records-config.xml"));
      server.activate();
      while (true)
        ;
    }
  }

  public class DataPropagationTask implements Callable<Void> {
    private ODatabaseDocumentTx baseDB;
    private ODatabaseDocumentTx testDB;

    public DataPropagationTask(ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDocumentTx) {
      this.baseDB = new ODatabaseDocumentTx(baseDB.getURL());
      this.testDB = new ODatabaseDocumentTx(testDocumentTx.getURL());
    }

    @Override
    public Void call() throws Exception {
      Random random = new Random();
      baseDB.open("admin", "admin");
      testDB.open("admin", "admin");

      try {
        while (true) {
          final ODocument document = new ODocument("TestClass");
          document.field("id", idGen.getAndIncrement());
          document.field("timestamp", System.currentTimeMillis());
          document.field("stringValue", "sfe" + random.nextLong());

          byte[] binaryValue = new byte[random.nextInt(2 * 65536) + 65537];
          random.nextBytes(binaryValue);

          document.field("binaryValue", binaryValue);

          saveDoc(document);
        }

      } finally {
        baseDB.close();
        testDB.close();
      }
    }

    private void saveDoc(ODocument document) {
      ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);

      ODocument testDoc = new ODocument();
      document.copyTo(testDoc);
      document.save();

      ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
      testDoc.save();
      ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);
    }
  }

}
