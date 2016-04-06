package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin
 * @since 20.06.13
 */
public class LocalPaginatedStorageCreateCrashRestoreIT {
  private final AtomicLong idGen = new AtomicLong();
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;
  private File                buildDir;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private Process process;

  public void spawnServer() throws Exception {
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageCreateCrashRestore";

    buildDir = new File(buildDirectory);
    if (buildDir.exists())
      buildDir.delete();

    buildDir.mkdir();

    String javaExec = System.getProperty("java.home") + "/bin/java";
    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-XX:MaxDirectMemorySize=512g", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, RemoteDBRunner.class.getName());
    processBuilder.inheritIO();

    process = processBuilder.start();

    Thread.sleep(5000);
  }

  @After
  public void afterClass() {
    testDocumentTx.activateOnCurrentThread();
    testDocumentTx.drop();

    baseDocumentTx.activateOnCurrentThread();
    baseDocumentTx.drop();

    OFileUtils.deleteRecursively(buildDir);
    Assert.assertFalse(buildDir.exists());

  }

  @Before
  public void beforeMethod() throws Exception {
    spawnServer();
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  @Test
  public void testDocumentCreation() throws Exception {
    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

    System.out.println("Wait for 5 minutes");
    TimeUnit.MINUTES.sleep(5);

    long lastTs = System.currentTimeMillis();

    System.out.println("Wait for process to destroy");

    process.destroy();
    process.waitFor();
    System.out.println("Process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageCrashRestore");
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

      testClass.createIndex("idIndex", OClass.INDEX_TYPE.UNIQUE, "id");

      schema.save();
    }
  }

  private void compareDocuments(long lastTs) {
    long minTs = Long.MAX_VALUE;
    baseDocumentTx.activateOnCurrentThread();
    int clusterId = baseDocumentTx.getClusterIdByName("TestClass");

    OStorage baseStorage = baseDocumentTx.getStorage();

    OPhysicalPosition[] physicalPositions = baseStorage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));

    int recordsRestored = 0;
    int recordsTested = 0;
    while (physicalPositions.length > 0) {
      final ORecordId rid = new ORecordId(clusterId);

      for (OPhysicalPosition physicalPosition : physicalPositions) {
        rid.clusterPosition = physicalPosition.clusterPosition;

        baseDocumentTx.activateOnCurrentThread();
        ODocument baseDocument = baseDocumentTx.load(rid);

        testDocumentTx.activateOnCurrentThread();
        List<ODocument> testDocuments = testDocumentTx
            .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + baseDocument.field("id")));
        if (testDocuments.size() == 0) {
          if (((Long) baseDocument.field("timestamp")) < minTs)
            minTs = baseDocument.field("timestamp");
        } else {
          ODocument testDocument = testDocuments.get(0);
          Assert.assertEquals(testDocument.field("id"), baseDocument.field("id"));
          Assert.assertEquals(testDocument.field("timestamp"), baseDocument.field("timestamp"));
          Assert.assertEquals(testDocument.field("stringValue"), baseDocument.field("stringValue"));
          recordsRestored++;
        }

        recordsTested++;

        if (recordsTested % 10000 == 0)
          System.out.println(recordsTested + " were tested, " + recordsRestored + " were restored ...");
      }

      physicalPositions = baseStorage.higherPhysicalPositions(clusterId, physicalPositions[physicalPositions.length - 1]);
    }

    System.out.println(
        recordsRestored + " records were restored. Total records " + recordsTested + ". Max interval for lost records " + (lastTs
            - minTs));
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-create-config.xml"));
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
          document.field("id", idGen.incrementAndGet());
          document.field("timestamp", System.currentTimeMillis());
          document.field("stringValue", "sfe" + random.nextLong());

          saveDoc(document);
        }

      } finally {
        baseDB.close();
        testDB.close();
      }
    }

    private void saveDoc(ODocument document) {

      baseDB.activateOnCurrentThread();
      baseDB.begin();
      ODocument testDoc = new ODocument();
      document.copyTo(testDoc);
      document.save();
      baseDB.commit();

      ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
      testDB.activateOnCurrentThread();
      testDB.begin();
      testDoc.save();
      testDB.commit();
    }
  }

}
