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
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin
 * @since 6/25/13
 */

public class LocalPaginatedStorageMixCrashRestoreIT {
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;

  private File buildDir;
  private AtomicInteger idGen = new AtomicInteger();

  private OOneEntryPerKeyLockManager<Integer> idLockManager = new OOneEntryPerKeyLockManager<Integer>(true, 1000, 10000);

  private ExecutorService executorService = Executors.newCachedThreadPool();

  private Process process;

  private ConcurrentSkipListSet<Integer> addedIds   = new ConcurrentSkipListSet<Integer>();
  private ConcurrentSkipListSet<Integer> updatedIds = new ConcurrentSkipListSet<Integer>();

  private ConcurrentHashMap<Integer, Long> deletedIds = new ConcurrentHashMap<Integer, Long>();

  private final AtomicLong lastOperation = new AtomicLong();

  public void spawnServer() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageMixCrashRestore";

    buildDir = new File(buildDirectory);

    buildDirectory = buildDir.getCanonicalPath();
    buildDir = new File(buildDirectory);

    if (buildDir.exists())
      OFileUtils.deleteRecursively(buildDir);

    buildDir.mkdir();

    final File mutexFile = new File(buildDir, "mutex.ct");
    final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
    mutex.seek(0);
    mutex.write(0);

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = new File(javaExec).getCanonicalPath();

    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-XX:MaxDirectMemorySize=512g", "-classpath",
        System.getProperty("java.class.path"), "-DmutexFile=" + mutexFile.getCanonicalPath(), "-DORIENTDB_HOME=" + buildDirectory,
        RemoteDBRunner.class.getName());

    processBuilder.inheritIO();

    process = processBuilder.start();

    System.out.println(LocalPaginatedStorageMixCrashRestoreIT.class.getSimpleName() + ": Wait for server start");
    boolean started = false;
    do {
      Thread.sleep(5000);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(LocalPaginatedStorageMixCrashRestoreIT.class.getSimpleName() + ": Server was started");
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
  public void setUp() throws Exception {
    spawnServer();
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageMixCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageMixCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  @Test
  public void testDocumentChanges() throws Exception {
    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);
    System.out.println("Schema was created.");

    System.out.println("Document creation was started.");
    createDocuments();
    System.out.println("Document creation was finished.");

    System.out.println("Start data changes.");

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new DataChangeTask(baseDocumentTx, testDocumentTx)));
    }

    System.out.println("Wait for 5 minutes");
    TimeUnit.MINUTES.sleep(5);

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

    System.out.println("Data changes were stopped.");
    System.out.println(
        addedIds.size() + " records were added. " + updatedIds.size() + " were updated. " + deletedIds.size() + " were deleted.");

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageMixCrashRestore");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    testDocumentTx.open("admin", "admin");

    System.out.println("Start documents comparison.");
    compareDocuments(lastOperation.get());
  }

  private void createSchema(ODatabaseDocumentTx dbDocumentTx) {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbDocumentTx);

    OSchema schema = dbDocumentTx.getMetadata().getSchema();
    if (!schema.existsClass("TestClass")) {
      OClass testClass = schema.createClass("TestClass");
      testClass.createProperty("id", OType.INTEGER);
      testClass.createProperty("timestamp", OType.LONG);
      testClass.createProperty("stringValue", OType.STRING);

      testClass.createIndex("idIndex", OClass.INDEX_TYPE.UNIQUE, "id");

      schema.save();
    }
  }

  private void createDocuments() {
    Random random = new Random();

    for (int i = 0; i < 100000; i++) {
      final ODocument document = new ODocument("TestClass");
      document.field("id", idGen.getAndIncrement());
      document.field("timestamp", System.currentTimeMillis());
      document.field("stringValue", "sfe" + random.nextLong());

      saveDoc(document, baseDocumentTx, testDocumentTx);
      addedIds.add(document.<Integer>field("id"));

      if (i % 10000 == 0)
        System.out.println(i + " documents were created.");
    }
  }

  private void saveDoc(ODocument document, ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDB) {
    baseDB.activateOnCurrentThread();

    ODocument testDoc = new ODocument();
    document.copyTo(testDoc);
    document.save();

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

        ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
        ODocument baseDocument = baseDocumentTx.load(rid);

        int id = baseDocument.<Integer>field("id");
        if (addedIds.contains(id)) {
          ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
          List<ODocument> testDocuments = testDocumentTx
              .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + baseDocument.field("id")));
          if (testDocuments.size() == 0) {
            if (((Long) baseDocument.field("timestamp")) < minTs)
              minTs = baseDocument.field("timestamp");
          } else {
            ODocument testDocument = testDocuments.get(0);
            Assert.assertEquals((Object) testDocument.field("id"), baseDocument.field("id"));
            Assert.assertEquals((Object) testDocument.field("timestamp"), baseDocument.field("timestamp"));
            Assert.assertEquals((Object) testDocument.field("stringValue"), baseDocument.field("stringValue"));
            recordsRestored++;
          }

          recordsTested++;
        } else if (updatedIds.contains(id)) {
          ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
          List<ODocument> testDocuments = testDocumentTx
              .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + baseDocument.field("id")));
          if (testDocuments.size() == 0) {
            if (((Long) baseDocument.field("timestamp")) < minTs)
              minTs = baseDocument.field("timestamp");
          } else {
            ODocument testDocument = testDocuments.get(0);
            if (testDocument.field("timestamp").equals(baseDocument.field("timestamp")) && testDocument.field("stringValue")
                .equals(baseDocument.field("stringValue"))) {
              recordsRestored++;
            } else {
              if (((Long) baseDocument.field("timestamp")) < minTs)
                minTs = baseDocument.field("timestamp");
            }
          }

          recordsTested++;
        }

        if (recordsTested % 10000 == 0)
          System.out.println(recordsTested + " were tested, " + recordsRestored + " were restored ...");
      }

      physicalPositions = baseStorage.higherPhysicalPositions(clusterId, physicalPositions[physicalPositions.length - 1]);
    }

    ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
    System.out.println("Check deleted records");
    for (Map.Entry<Integer, Long> deletedEntry : deletedIds.entrySet()) {
      int deletedId = deletedEntry.getKey();
      List<ODocument> testDocuments = testDocumentTx
          .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + deletedId));
      if (!testDocuments.isEmpty()) {
        if (deletedEntry.getValue() < minTs)
          minTs = deletedEntry.getValue();
      } else
        recordsRestored++;

      recordsTested++;
    }

    System.out.println("Deleted records were checked." + deletedIds.size() + " were verified.");

    System.out.println(
        recordsRestored + " records were restored. Total records " + recordsTested + ". Max interval for lost records " + (lastTs
            - minTs));

  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-mix-config.xml"));
      server.activate();

      final String mutexFile = System.getProperty("mutexFile");
      final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
      mutex.seek(0);
      mutex.write(1);
      mutex.close();
    }
  }

  public class DataChangeTask implements Callable<Void> {
    private ODatabaseDocumentTx baseDB;
    private ODatabaseDocumentTx testDB;

    private Random random = new Random();

    public DataChangeTask(ODatabaseDocumentTx baseDB, ODatabaseDocumentTx testDocumentTx) {
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
          double rndOutcome = random.nextDouble();

          int actionType = -1;

          if (rndOutcome <= 0.2) {
            if (addedIds.size() + updatedIds.size() >= 100000)
              actionType = 2;
            else
              actionType = 0;

          } else if (rndOutcome > 0.2 && rndOutcome <= 0.6)
            actionType = 1;
          else if (rndOutcome > 0.6) {
            if (addedIds.size() + updatedIds.size() <= 2000000)
              actionType = 0;
            else
              actionType = 2;
          }

          long ts = -1;
          switch (actionType) {
          case 0:
            ts = createRecord();
            break;
          case 1:
            ts = updateRecord();
            break;
          case 2:
            ts = deleteRecord();
            break;
          default:
            throw new IllegalStateException("Invalid action type " + actionType);
          }

          long currentTs = lastOperation.get();
          while (currentTs < ts && !lastOperation.compareAndSet(currentTs, ts)) {
            currentTs = lastOperation.get();
          }
        }
      } finally {
        baseDB.activateOnCurrentThread();
        baseDB.close();
        testDB.activateOnCurrentThread();
        testDB.close();
      }
    }

    private long createRecord() {
      long ts = -1;
      final int idToCreate = idGen.getAndIncrement();
      idLockManager.acquireLock(idToCreate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      try {
        final ODocument document = new ODocument("TestClass");
        document.field("id", idToCreate);
        ts = System.currentTimeMillis();
        document.field("timestamp", ts);
        document.field("stringValue", "sfe" + random.nextLong());

        saveDoc(document, baseDB, testDB);

        addedIds.add(document.<Integer>field("id"));
      } finally {
        idLockManager.releaseLock(Thread.currentThread(), idToCreate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      }

      return ts;
    }

    private long deleteRecord() {
      int closeId = random.nextInt(idGen.get());

      Integer idToDelete = addedIds.ceiling(closeId);
      if (idToDelete == null)
        idToDelete = addedIds.first();

      idLockManager.acquireLock(idToDelete, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);

      while (deletedIds.containsKey(idToDelete)) {
        idLockManager.releaseLock(Thread.currentThread(), idToDelete, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
        closeId = random.nextInt(idGen.get());

        idToDelete = addedIds.ceiling(closeId);
        if (idToDelete == null)
          idToDelete = addedIds.first();
        idLockManager.acquireLock(idToDelete, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      }

      addedIds.remove(idToDelete);
      updatedIds.remove(idToDelete);

      ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);
      int deleted = baseDB.command(new OCommandSQL("delete from TestClass where id  = " + idToDelete)).execute();
      Assert.assertEquals(deleted, 1);

      ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
      deleted = testDB.command(new OCommandSQL("delete from TestClass where id  = " + idToDelete)).execute();
      Assert.assertEquals(deleted, 1);

      ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);

      long ts = System.currentTimeMillis();

      deletedIds.put(idToDelete, ts);

      idLockManager.releaseLock(Thread.currentThread(), idToDelete, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);

      return ts;
    }

    private long updateRecord() {
      int closeId = random.nextInt(idGen.get());

      Integer idToUpdate = addedIds.ceiling(closeId);
      if (idToUpdate == null)
        idToUpdate = addedIds.first();

      idLockManager.acquireLock(idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);

      while (deletedIds.containsKey(idToUpdate)) {
        idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
        closeId = random.nextInt(idGen.get());

        idToUpdate = addedIds.ceiling(closeId);
        if (idToUpdate == null)
          idToUpdate = addedIds.first();

        idLockManager.acquireLock(idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
      }

      addedIds.remove(idToUpdate);

      baseDB.activateOnCurrentThread();
      List<ODocument> documentsToUpdate = baseDB
          .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + idToUpdate));
      Assert.assertTrue(!documentsToUpdate.isEmpty());

      final ODocument documentToUpdate = documentsToUpdate.get(0);
      long ts = System.currentTimeMillis();

      documentToUpdate.field("timestamp", ts);
      documentToUpdate.field("stringValue", "vde" + random.nextLong());

      saveDoc(documentToUpdate, baseDB, testDB);

      updatedIds.add(idToUpdate);

      idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);

      return ts;
    }
  }
}
