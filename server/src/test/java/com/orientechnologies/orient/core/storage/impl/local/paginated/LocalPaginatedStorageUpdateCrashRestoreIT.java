package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OOneEntryPerKeyLockManager;
import com.orientechnologies.common.io.OFileUtils;
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
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin
 * @since 6/24/13
 */
public class LocalPaginatedStorageUpdateCrashRestoreIT {
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;

  private File buildDir;
  private AtomicInteger idGen = new AtomicInteger(0);

  private OOneEntryPerKeyLockManager<Integer> idLockManager = new OOneEntryPerKeyLockManager<Integer>(true, 1000, 10000);

  private ExecutorService executorService = Executors.newCachedThreadPool();
  private Process process;

  public void spawnServer() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageUpdateCrashRestore";

    buildDir = new File(buildDirectory);

    buildDirectory = buildDir.getCanonicalPath();
    buildDir = new File(buildDirectory);

    if (buildDir.exists())
      OFileUtils.deleteFolderIfEmpty(buildDir);

    buildDir.mkdir();

    final File mutexFile = new File(buildDir, "mutex.ct");
    final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
    mutex.seek(0);
    mutex.write(0);

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = (new File(javaExec)).getCanonicalPath();

    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-XX:MaxDirectMemorySize=512g", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, "-DmutexFile=" + mutexFile.getCanonicalPath(), RemoteDBRunner.class.getName());
    processBuilder.inheritIO();

    process = processBuilder.start();


    System.out.println(LocalPaginatedStorageUpdateCrashRestoreIT.class.getSimpleName() + ": Wait for server start");
    boolean started = false;
    do {
      Thread.sleep(5000);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(LocalPaginatedStorageUpdateCrashRestoreIT.class.getSimpleName() + ": Server was started");
  }

  @After
  public void tearDown() {
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
    baseDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageUpdateCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageUpdateCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  @Test
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

    System.out.println("Documents update was stopped.");

    testDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageUpdateCrashRestore");

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
      document.field("id", idGen.incrementAndGet());
      document.field("timestamp", System.currentTimeMillis());
      document.field("stringValue", "sfe" + random.nextLong());

      saveDoc(document, baseDocumentTx, testDocumentTx);

      if (i % 10000 == 0)
        System.out.println(i + " documents were created.");
    }
  }

  private void saveDoc(ODocument document, ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDB) {

    baseDB.activateOnCurrentThread();
    ODocument testDoc = new ODocument();
    document.copyTo(testDoc);
    document.save();

    ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
    testDB.activateOnCurrentThread();
    testDoc.save();

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
        Assert.assertTrue(!testDocuments.isEmpty());

        ODocument testDocument = testDocuments.get(0);
        if (testDocument.field("timestamp").equals(baseDocument.field("timestamp")) && testDocument.field("stringValue")
            .equals(baseDocument.field("stringValue"))) {
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

    System.out.println(
        recordsRestored + " records were restored. Total records " + recordsTested + ". lost records " + (recordsTested
            - recordsRestored));
    long maxInterval = minTs == Long.MAX_VALUE ? 0 : lastTs - minTs;
    System.out.println("Lost records max interval (ms) : " + maxInterval);

    assertThat(recordsTested - recordsRestored).isLessThan(120);

    assertThat(maxInterval).isLessThan(2000);
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-update-config.xml"));
      server.activate();

      final String mutexFile = System.getProperty("mutexFile");
      final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
      mutex.seek(0);
      mutex.write(1);
      mutex.close();
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
          final int idToUpdate = random.nextInt(idGen.get());
          idLockManager.acquireLock(idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
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
            idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
          }
        }
      } finally {
        baseDB.activateOnCurrentThread();
        baseDB.close();

        testDB.activateOnCurrentThread();
        testDB.close();
      }
    }
  }

}
