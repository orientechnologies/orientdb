package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.concur.lock.OLockManager;
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

/**
 * @author Andrey Lomakin
 * @since 6/24/13
 */
@Test
public class LocalPaginatedStorageUpdateCrashRestore {
  private ODatabaseDocumentTx           baseDocumentTx;
  private ODatabaseDocumentTx           testDocumentTx;

  private File                          buildDir;
  private int                           idGen           = 0;

  private OLockManager<Integer> idLockManager   = new OLockManager<Integer>(true, 1000);

  private ExecutorService               executorService = Executors.newCachedThreadPool();
  private Process                       process;

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-update-config.xml"));
      server.activate();
      while (true)
        ;
    }
  }

  public class DataUpdateTask implements Callable<Void> {
    private ODatabaseDocumentTx baseDB;
    private ODatabaseDocumentTx testDB;

    public DataUpdateTask(ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDocumentTx) {
      this.baseDB = new ODatabaseDocumentTx(baseDB.getURL());
      this.testDB = new ODatabaseDocumentTx(testDocumentTx.getURL());
    }

    @Override
    public Void call() throws Exception {
      Random random = new Random();
      baseDB.open("admin", "admin");
      testDB.open("admin", "admin");

      int counter = 0;

      try {
        while (true) {
          final int idToUpdate = random.nextInt(idGen);
          idLockManager.acquireLock(idToUpdate, OLockManager.LOCK.EXCLUSIVE);
          try {
            OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + idToUpdate);
            final List<ODocument> result = baseDB.query(query);

            Assert.assertTrue(!result.isEmpty());

            final ODocument document = result.get(0);
            document.field("timestamp", System.currentTimeMillis());
            document.field("stringValue", "vde" + random.nextLong());

            saveDoc(document, baseDB, testDB);

            counter++;

            if (counter % 50000 == 0)
              System.out.println(counter + " records were updated.");
          } finally {
            idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OLockManager.LOCK.EXCLUSIVE);
          }
        }
      } finally {
        baseDB.close();
        testDB.close();
      }
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageUpdateCrashRestore";

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

    Assert.assertTrue(new File(buildDir, "plugins").delete());
    Assert.assertTrue(buildDir.delete());
  }

  @BeforeMethod
  public void beforeMethod() {
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath()
        + "/baseLocalPaginatedStorageUpdateCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageUpdateCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  public void testDocumentUpdate() throws Exception {
    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);
    System.out.println("Schema was created.");

    System.out.println("Document creation was started.");
    createDocuments();
    System.out.println("Document creation was finished.");

    System.out.println("Start documents update.");

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new DataUpdateTask(baseDocumentTx, testDocumentTx)));
    }

    Thread.sleep(300000);

    long lastTs = System.currentTimeMillis();
    System.out.println("Wait for process to destroy");
    // process.destroyForcibly();
    process.waitFor();
    System.out.println("Process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println("Documents update was stopped.");

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath()
        + "/testLocalPaginatedStorageUpdateCrashRestore");

    long startRestoreTime = System.currentTimeMillis();
    testDocumentTx.open("admin", "admin");
    long endRestoreTime = System.currentTimeMillis();

    System.out.println("Restore time : " + (endRestoreTime - startRestoreTime));
    testDocumentTx.close();

    testDocumentTx.open("admin", "admin");

    System.out.println("Start documents comparison.");
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

  private void createDocuments() {
    Random random = new Random();

    for (int i = 0; i < 1000000; i++) {
      final ODocument document = new ODocument("TestClass");
      document.field("id", idGen++);
      document.field("timestamp", System.currentTimeMillis());
      document.field("stringValue", "sfe" + random.nextLong());

      saveDoc(document, baseDocumentTx, testDocumentTx);

      if (i % 10000 == 0)
        System.out.println(i + " documents were created.");
    }
  }

  private void saveDoc(ODocument document, ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDB) {
    ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);

    ODocument testDoc = new ODocument();
    document.copyTo(testDoc);
    document.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
    testDoc.save();
    ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);
  }

  private void compareDocuments(long lastTs) {
    long minTs = Long.MAX_VALUE;
    int clusterId = baseDocumentTx.getClusterIdByName("TestClass");

    OStorage baseStorage = baseDocumentTx.getStorage();

    OPhysicalPosition[] physicalPositions = baseStorage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));

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
        Assert.assertTrue(!testDocuments.isEmpty());

        ODocument testDocument = testDocuments.get(0);
        if (testDocument.field("timestamp").equals(baseDocument.field("timestamp"))
            && testDocument.field("stringValue").equals(baseDocument.field("stringValue"))) {
          recordsRestored++;
        } else {
          if (((Long) baseDocument.field("timestamp")) < minTs)
            minTs = baseDocument.field("timestamp");
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

}
