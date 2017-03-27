package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 9/22/14
 */

public class IndexCrashRestoreSingleValueIT {
  private final AtomicLong idGen = new AtomicLong();
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;
  private File                buildDir;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private Process serverProcess;

  @Before
  public void setuUp() throws Exception {
    spwanServer();
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseUniqueIndexCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testUniqueIndexCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  public void spwanServer() throws Exception {
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/uniqueIndexCrashRestore";

    buildDir = new File(buildDirectory);
    if (buildDir.exists())
      OFileUtils.deleteRecursively(buildDir);

    buildDir.mkdir();

    final File mutexFile = new File(buildDir, "mutex.ct");
    final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
    mutex.seek(0);
    mutex.write(0);

    buildDirectory = buildDir.getCanonicalPath();
    buildDir = new File(buildDirectory);

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = new File(javaExec).getCanonicalPath();

    System.setProperty("ORIENTDB_HOME", buildDirectory);

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-XX:MaxDirectMemorySize=512g", "-classpath",
        System.getProperty("java.class.path"), "-DORIENTDB_HOME=" + buildDirectory, "-DmutexFile=" + mutexFile.getCanonicalPath(),
        RemoteDBRunner.class.getName());
    CrashRestoreUtils.inheritIO(processBuilder);

    serverProcess = processBuilder.start();

    System.out.println(IndexCrashRestoreSingleValueIT.class.getSimpleName() + ": Wait for server start");
    boolean started = false;
    do {
      Thread.sleep(5000);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(IndexCrashRestoreSingleValueIT.class.getSimpleName() + ": Server was started");
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

  @Test
  public void testEntriesAddition() throws Exception {

    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);

    System.out.println("Start data propagation");

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

    TimeUnit.MINUTES.sleep(5);

    System.out.println("Wait for process to destroy");
    CrashRestoreUtils.destroyForcibly(serverProcess);

    serverProcess.waitFor();
    System.out.println("Process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        future.cancel(true);
      }
    }

    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/testUniqueIndexCrashRestore");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    testDocumentTx.open("admin", "admin");

    System.out.println("Start data comparison.");
    compareIndexes();
  }

  private void compareIndexes() {
    ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
    OIndexCursor cursor = baseDocumentTx.getMetadata().getIndexManager().getIndex("mi").cursor();

    long lastTs = 0;
    long minLostTs = Long.MAX_VALUE;

    long restoredRecords = 0;

    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
      Integer key = (Integer) entry.getKey();

      OIdentifiable identifiable = entry.getValue();
      ODocument doc = identifiable.getRecord();

      long ts = doc.<Long>field("ts");
      if (ts > lastTs)
        lastTs = ts;

      entry = cursor.nextEntry();

      ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
      OIndex testIndex = testDocumentTx.getMetadata().getIndexManager().getIndex("mi");

      Set<OIdentifiable> result = (Set<OIdentifiable>) testIndex.get(key);
      if (result == null || result.isEmpty()) {
        if (minLostTs > ts)
          minLostTs = ts;
      } else
        restoredRecords++;
    }

    ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
    System.out.println(
        "Restored entries : " + restoredRecords + " out of : " + baseDocumentTx.getMetadata().getIndexManager().getIndex("mi")
            .getSize());

    long maxInterval = minLostTs == Long.MAX_VALUE ? 0 : lastTs - minLostTs;
    System.out.println("Lost records max interval (ms) : " + maxInterval);

    assertThat(maxInterval).isLessThan(2000);
  }

  private void createSchema(ODatabaseDocumentTx dbDocumentTx) {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbDocumentTx);
    dbDocumentTx.command(new OCommandSQL("create index mi notunique integer")).execute();
    dbDocumentTx.getMetadata().getIndexManager().reload();
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class.getResourceAsStream(
          "/com/orientechnologies/orient/core/storage/impl/local/paginated/index-crash-single-value-config.xml"));
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
      baseDB.open("admin", "admin");
      testDB.open("admin", "admin");

      try {
        while (true) {
          long id = idGen.getAndIncrement();
          long ts = System.currentTimeMillis();

          ODatabaseRecordThreadLocal.INSTANCE.set(baseDB);

          ODocument doc = new ODocument();
          doc.field("ts", ts);
          doc.save();

          baseDB.command(new OCommandSQL("insert into index:mi (key, rid) values (" + id + ", " + doc.getIdentity() + ")"))
              .execute();

          ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
          doc = new ODocument();
          doc.field("ts", ts);
          doc.save();

          testDB.command(new OCommandSQL("insert into index:mi (key, rid) values (" + id + ", " + doc.getIdentity() + ")"))
              .execute();
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
