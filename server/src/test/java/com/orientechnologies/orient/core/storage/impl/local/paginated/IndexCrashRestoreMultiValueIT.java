package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 9/25/14
 */
public class IndexCrashRestoreMultiValueIT {
  private final AtomicLong          idGen;
  private final String              testIndexName;
  private final String              baseIndexName;
  private       ODatabaseDocumentTx baseDocumentTx;
  private       ODatabaseDocumentTx testDocumentTx;
  private       File                buildDir;
  private       ExecutorService     executorService;
  private       Process             serverProcess;

  public IndexCrashRestoreMultiValueIT() {
    executorService = Executors.newCachedThreadPool();
    idGen = new AtomicLong();

    baseIndexName = "baseIndexCrashRestoreMultivalue";
    testIndexName = "testIndexCrashRestoreMultivalue";
  }

  @Before
  public void beforeMethod() throws Exception {
    spawnServer();
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/" + baseIndexName);
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/" + testIndexName);
    testDocumentTx.open("admin", "admin");
  }

  public void spawnServer() throws Exception {
    OLogManager.instance().installCustomFormatter();
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(1000000);
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
    OGlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/" + getClass().getSimpleName();

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      OFileUtils.deleteRecursively(buildDir);
    }

    buildDir.mkdirs();

    String javaExec = System.getProperty("java.home") + "/bin/java";

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-XX:MaxDirectMemorySize=512g", "-classpath", System.getProperty("java.class.path"),
        "-DORIENTDB_HOME=" + buildDirectory, RemoteDBRunner.class.getName());

    processBuilder.inheritIO();

    serverProcess = processBuilder.start();

    System.out.println("OrientDb server started :: " + serverProcess);
    Thread.sleep(5000);
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
    for (int i = 0; i < 4; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

    System.out.println("Wait for 5 minutes");
    TimeUnit.MINUTES.sleep(5);

    System.out.println("Wait for process to destroy");
    serverProcess.destroy();

    serverProcess.waitFor();
    System.out.println("Process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        future.cancel(true);
      }
    }

    System.out.println("All loaders done");

    System.out.println("Open remote crashed DB in plocal to recover");
    testDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/" + testIndexName);
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    System.out.println("Reopen cleaned db");
    testDocumentTx.open("admin", "admin");

    System.out.println("Start data comparison.");
    compareIndexes();
  }

  private void compareIndexes() {
    baseDocumentTx.activateOnCurrentThread();
    OIndexCursor cursor = baseDocumentTx.getMetadata().getIndexManager().getIndex("mi").cursor();

    long lastTs = 0;
    long minLostTs = Long.MAX_VALUE;
    long restoredRecords = 0;

    Map.Entry<Object, OIdentifiable> entry = cursor.nextEntry();
    while (entry != null) {
      baseDocumentTx.activateOnCurrentThread();
      Integer key = (Integer) entry.getKey();

      OIdentifiable identifiable = entry.getValue();
      ODocument doc = identifiable.getRecord();

      long ts = doc.<Long>field("ts");
      if (ts > lastTs)
        lastTs = ts;

      entry = cursor.nextEntry();

      testDocumentTx.activateOnCurrentThread();
      OIndex testIndex = testDocumentTx.getMetadata().getIndexManager().getIndex("mi");

      Set<OIdentifiable> result = (Set<OIdentifiable>) testIndex.get(key);
      if (result == null || result.size() < 10) {
        if (minLostTs > ts)
          minLostTs = ts;
      } else {
        boolean cnt = true;

        for (int i = 0; i < 10; i++) {
          if (!result.contains(new ORecordId("#0:" + i))) {
            cnt = false;
            break;
          }
        }

        if (!cnt) {
          if (minLostTs > ts)
            minLostTs = ts;
        } else {
          restoredRecords++;
        }
      }

    }

    baseDocumentTx.activateOnCurrentThread();
    System.out.println(
        "Restored entries : " + restoredRecords + " out of : " + baseDocumentTx.getMetadata().getIndexManager().getIndex("mi")
            .getSize() + " minLostTs:: " + minLostTs);

    long maxInterval = minLostTs == Long.MAX_VALUE ? 0 : lastTs - minLostTs;
    System.out.println("Lost records max interval (ms) : " + maxInterval);

    assertThat(maxInterval).isLessThan(2000);

  }

  private void createSchema(ODatabaseDocumentTx dbDocumentTx) {
    dbDocumentTx.activateOnCurrentThread();

    OIndex<?> index = dbDocumentTx.getMetadata().getIndexManager().getIndex("mi");
    if (index == null) {
      System.out.println("create index for db:: " + dbDocumentTx.getURL());
      dbDocumentTx.command(new OCommandSQL("create index mi notunique integer")).execute();
      dbDocumentTx.getMetadata().getIndexManager().reload();
    }
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
      OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(100000000);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class.getResourceAsStream(
          "/com/orientechnologies/orient/core/storage/impl/local/paginated/index-crash-multivalue-value-config.xml"));
      server.activate();

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

          if (id % 1000 == 0)
            System.out.println(Thread.currentThread().getName() + " inserted:: " + id);

          baseDB.command(new OCommandSQL("insert into index:mi (key, rid) values (" + id + ", " + doc.getIdentity() + ")"))
              .execute();

          ODatabaseRecordThreadLocal.INSTANCE.set(testDB);
          for (int i = 0; i < 10; i++) {
            testDB.command(new OCommandSQL("insert into index:mi (key, rid) values (" + id + ", #0:" + i + ")")).execute();
          }

        }
      } catch (Exception e) {
        throw e;
      } finally {
        try {
          baseDB.close();
        } catch (Exception e) {
        }

        try {
          testDB.close();
        } catch (Exception e) {
        }
      }

      // return null;
    }
  }
}
