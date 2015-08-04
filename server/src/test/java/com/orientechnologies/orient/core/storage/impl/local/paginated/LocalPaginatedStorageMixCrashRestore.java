package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;

/**
 * @author Andrey Lomakin
 * @since 6/25/13
 */
@Test
public class LocalPaginatedStorageMixCrashRestore {
  private ODatabaseDocumentTx              baseDocumentTx;
  private ODatabaseDocumentTx              testDocumentTx;

  private File                             buildDir;
  private AtomicInteger                    idGen           = new AtomicInteger();

  private OLockManager<Integer>            idLockManager   = new OLockManager<Integer>(true, 1000);

  private ExecutorService                  executorService = Executors.newCachedThreadPool();

  private Process                          process;

  private ConcurrentSkipListSet<Integer>   addedIds        = new ConcurrentSkipListSet<Integer>();
  private ConcurrentSkipListSet<Integer>   updatedIds      = new ConcurrentSkipListSet<Integer>();

  private ConcurrentHashMap<Integer, Long> deletedIds      = new ConcurrentHashMap<Integer, Long>();

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-mix-config.xml"));
      server.activate();
      while (true)
        ;
    }
  }

  public class DataChangeTask implements Callable<Void> {
    private ODatabaseDocumentTx baseDB;
    private ODatabaseDocumentTx testDB;

    private Random              random = new Random();

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

          switch (actionType) {
          case 0:
            createRecord();
            break;
          case 1:
            updateRecord();
            break;
          case 2:
            deleteRecord();
            break;
          default:
            throw new IllegalStateException("Invalid action type " + actionType);
          }
        }
      } finally {
        baseDB.close();
        testDB.close();
      }
    }

    private void createRecord() {
      final int idToCreate = idGen.getAndIncrement();
      idLockManager.acquireLock(idToCreate, OLockManager.LOCK.EXCLUSIVE);
      try {
        final ODocument document = new ODocument("TestClass");
        document.field("id", idToCreate);
        document.field("timestamp", System.currentTimeMillis());
        document.field("stringValue", "sfe" + random.nextLong());

        saveDoc(document, baseDB, testDB);

        addedIds.add(document.<Integer> field("id"));
      } finally {
        idLockManager.releaseLock(Thread.currentThread(), idToCreate, OLockManager.LOCK.EXCLUSIVE);
      }
    }

    private void deleteRecord() {
      int closeId = random.nextInt(idGen.get());

      Integer idToDelete = addedIds.ceiling(closeId);
      if (idToDelete == null)
        idToDelete = addedIds.first();

      idLockManager.acquireLock(idToDelete, OLockManager.LOCK.EXCLUSIVE);

      while (deletedIds.containsKey(idToDelete)) {
        idLockManager.releaseLock(Thread.currentThread(), idToDelete, OLockManager.LOCK.EXCLUSIVE);
        closeId = random.nextInt(idGen.get());

        idToDelete = addedIds.ceiling(closeId);
        if (idToDelete == null)
          idToDelete = addedIds.first();
        idLockManager.acquireLock(idToDelete, OLockManager.LOCK.EXCLUSIVE);
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

      idLockManager.releaseLock(Thread.currentThread(), idToDelete, OLockManager.LOCK.EXCLUSIVE);
    }

    private void updateRecord() {
      int closeId = random.nextInt(idGen.get());

      Integer idToUpdate = addedIds.ceiling(closeId);
      if (idToUpdate == null)
        idToUpdate = addedIds.first();

      idLockManager.acquireLock(idToUpdate, OLockManager.LOCK.EXCLUSIVE);

      while (deletedIds.containsKey(idToUpdate)) {
        idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OLockManager.LOCK.EXCLUSIVE);
        closeId = random.nextInt(idGen.get());

        idToUpdate = addedIds.ceiling(closeId);
        if (idToUpdate == null)
          idToUpdate = addedIds.first();

        idLockManager.acquireLock(idToUpdate, OLockManager.LOCK.EXCLUSIVE);
      }

      addedIds.remove(idToUpdate);

      List<ODocument> documentsToUpdate = baseDB.query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = "
          + idToUpdate));
      Assert.assertTrue(!documentsToUpdate.isEmpty());

      final ODocument documentToUpdate = documentsToUpdate.get(0);
      documentToUpdate.field("timestamp", System.currentTimeMillis());
      documentToUpdate.field("stringValue", "vde" + random.nextLong());

      saveDoc(documentToUpdate, baseDB, testDB);

      updatedIds.add(idToUpdate);

      idLockManager.releaseLock(Thread.currentThread(), idToUpdate, OLockManager.LOCK.EXCLUSIVE);
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageMixCrashRestore";

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
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageMixCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageMixCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

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

    System.out.println("Data changes were stopped.");
    System.out.println(addedIds.size() + " records were added. " + updatedIds.size() + " were updated. " + deletedIds.size()
        + " were deleted.");

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageMixCrashRestore");
    testDocumentTx.open("admin", "admin");
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
      testClass.createProperty("id", OType.INTEGER);
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
      document.field("id", idGen.getAndIncrement());
      document.field("timestamp", System.currentTimeMillis());
      document.field("stringValue", "sfe" + random.nextLong());

      saveDoc(document, baseDocumentTx, testDocumentTx);
      addedIds.add(document.<Integer> field("id"));

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

        int id = baseDocument.<Integer> field("id");
        if (addedIds.contains(id)) {
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
            recordsRestored++;
          }

          recordsTested++;
        } else if (updatedIds.contains(id)) {
          ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
          List<ODocument> testDocuments = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = "
              + baseDocument.field("id")));
          if (testDocuments.size() == 0) {
            if (((Long) baseDocument.field("timestamp")) < minTs)
              minTs = baseDocument.field("timestamp");
          } else {
            ODocument testDocument = testDocuments.get(0);
            if (testDocument.field("timestamp").equals(baseDocument.field("timestamp"))
                && testDocument.field("stringValue").equals(baseDocument.field("stringValue"))) {
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
      List<ODocument> testDocuments = testDocumentTx.query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = "
          + deletedId));
      if (!testDocuments.isEmpty()) {
        if (deletedEntry.getValue() < minTs)
          minTs = deletedEntry.getValue();
      } else
        recordsRestored++;

      recordsTested++;
    }

    System.out.println("Deleted records were checked." + deletedIds.size() + " were verified.");

    System.out.println(recordsRestored + " records were restored. Total records " + recordsTested
        + ". Max interval for lost records " + (lastTs - minTs));

  }
}
