package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
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
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 6/26/13
 */
public class LocalPaginatedStorageSmallCacheBigRecordsCrashRestoreIT {
  private final AtomicLong idGen = new AtomicLong();
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;
  private File                buildDir;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private Process process;

  public void spawnServer() throws Exception {
    final File mutexFile = new File(buildDir, "mutex.ct");
    final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
    mutex.seek(0);
    mutex.write(0);

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = new File(javaExec).getCanonicalPath();

    System.setProperty("ORIENTDB_HOME", buildDir.getCanonicalPath());

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec,  "-Xmx4096m", "-XX:MaxDirectMemorySize=512g", "-classpath",
        System.getProperty("java.class.path"), "-DmutexFile=" + mutexFile.getCanonicalPath(),
        "-DORIENTDB_HOME=" + buildDir.getCanonicalPath(), RemoteDBRunner.class.getName());
    processBuilder.inheritIO();

    process = processBuilder.start();

    System.out.println(LocalPaginatedStorageSmallCacheBigRecordsCrashRestoreIT.class.getSimpleName() + ": Wait for server start");
    boolean started = false;
    do {
      Thread.sleep(5000);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(LocalPaginatedStorageSmallCacheBigRecordsCrashRestoreIT.class.getSimpleName() + ": Server was started");
  }

  @After
  public void tearDown() {
    if (testDocumentTx != null) {
      testDocumentTx.activateOnCurrentThread();
      testDocumentTx.drop();
    }

    if (baseDocumentTx != null) {
      baseDocumentTx.activateOnCurrentThread();
      baseDocumentTx.drop();
    }

    OFileUtils.deleteRecursively(buildDir);
    Assert.assertFalse(buildDir.exists());
  }

  @Before
  public void setuUp() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageSmallCacheBigRecordsCrashRestore";

    buildDir = new File(buildDirectory);

    buildDirectory = buildDir.getCanonicalPath();
    buildDir = new File(buildDirectory);

    if (buildDir.exists())
      OFileUtils.deleteRecursively(buildDir);

    buildDir.mkdir();

    baseDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageSmallCacheBigRecordsCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    spawnServer();

    final OServerAdmin serverAdmin = new OServerAdmin("remote:localhost:3500");
    serverAdmin.connect("root", "root");
    serverAdmin.createDatabase("testLocalPaginatedStorageSmallCacheBigRecordsCrashRestore", "graph", "plocal");
    serverAdmin.close();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testLocalPaginatedStorageSmallCacheBigRecordsCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  @Test
  public void testDocumentCreation() throws Exception {
    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

    System.out.println("Wait for 5 minutes");
    TimeUnit.MINUTES.sleep(5);

    long lastTs = System.currentTimeMillis();
    process.destroy();
    process.waitFor();

    System.out.println("OrientDB server process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    testDocumentTx = new ODatabaseDocumentTx(
        "plocal:" + new File(new File(buildDir, "databases"), "testLocalPaginatedStorageSmallCacheBigRecordsCrashRestore"));

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
    baseDocumentTx.activateOnCurrentThread();
    int clusterId = baseDocumentTx.getClusterIdByName("TestClass");

    OStorage baseStorage = baseDocumentTx.getStorage();

    OPhysicalPosition[] physicalPositions = baseStorage.ceilingPhysicalPositions(clusterId, new OPhysicalPosition(0));

    int recordsRestored = 0;
    int recordsTested = 0;
    while (physicalPositions.length > 0) {
      final ORecordId rid = new ORecordId(clusterId);

      for (OPhysicalPosition physicalPosition : physicalPositions) {

        rid.setClusterPosition(physicalPosition.clusterPosition);

        baseDocumentTx.activateOnCurrentThread();
        ODocument baseDocument = baseDocumentTx.load(rid);

        testDocumentTx.activateOnCurrentThread();
        List<ODocument> testDocuments = testDocumentTx
            .query(new OSQLSynchQuery<ODocument>("select from TestClass where id  = " + baseDocument.field("id")));

        if (testDocuments.size() == 0) {
          if (((Long) baseDocument.field("timestamp")) < minTs) {
            minTs = baseDocument.field("timestamp");
          }
        } else {
          ODocument testDocument = testDocuments.get(0);

          assertThat(testDocument.<Object>field("id")).as("id:: %s", testDocument.<Object>field("id"))
              .isEqualTo(baseDocument.field("id"));
          assertThat(testDocument.<Object>field("timestamp")).as("documents:: %s - %s", testDocument, baseDocument)
              .isEqualTo(baseDocument.field("timestamp"));
          assertThat(testDocument.<Object>field("stringValue")).as("id:: %s", testDocument.<Object>field("id"))
              .isEqualTo(baseDocument.field("stringValue"));
          assertThat(testDocument.<Object>field("binaryValue")).as("id:: %s", testDocument.<Object>field("id"))
              .isEqualTo(baseDocument.field("binaryValue"));

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
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(512);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-create-big-records-config.xml"));
      server.activate();

      final String mutexFile = System.getProperty("mutexFile");
      final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
      mutex.seek(0);
      mutex.write(1);
      mutex.close();
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
        baseDB.activateOnCurrentThread();

        baseDB.close();
        testDB.activateOnCurrentThread();

        testDB.close();
      }
    }

    private void saveDoc(ODocument document) {

      baseDB.activateOnCurrentThread();
      ODocument testDoc = new ODocument();
      document.copyTo(testDoc);
      document.save();

      testDB.activateOnCurrentThread();
      testDoc.save();
    }
  }

}
