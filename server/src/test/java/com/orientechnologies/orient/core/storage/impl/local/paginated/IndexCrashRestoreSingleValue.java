package com.orientechnologies.orient.core.storage.impl.local.paginated;

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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 9/22/14
 */
@Test
public class IndexCrashRestoreSingleValue {
  private ODatabaseDocumentTx baseDocumentTx;
  private ODatabaseDocumentTx testDocumentTx;

  private File                buildDir;
  private final AtomicLong    idGen           = new AtomicLong();

  private ExecutorService     executorService = Executors.newCachedThreadPool();
  private Process             process;

  @BeforeClass
  public void beforeClass() throws Exception {
		OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/uniqueIndexCrashRestore";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
        buildDir.delete();
                }

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
    baseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDir.getAbsolutePath() + "/baseUniqueIndexCrashRestore");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    testDocumentTx = new ODatabaseDocumentTx("remote:localhost:3500/testUniqueIndexCrashRestore");
    testDocumentTx.open("admin", "admin");
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
			OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);
      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/index-crash-single-value-config.xml"));
      server.activate();
      while (true)
        ;
    }
  }

  public void testEntriesAddition() throws Exception {

    createSchema(baseDocumentTx);
    createSchema(testDocumentTx);

    System.out.println("Start data propagation");

    List<Future> futures = new ArrayList<Future>();
    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(new DataPropagationTask(baseDocumentTx, testDocumentTx)));
    }

		Thread.sleep(1800000);

    System.out.println("Wait for process to destroy");
    Process p = Runtime.getRuntime().exec("pkill -9 -f RemoteDBRunner");
    p.waitFor();

    process.waitFor();
    System.out.println("Process was destroyed");

    for (Future future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        e.printStackTrace();
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

      long ts = doc.<Long> field("ts");
      if (ts > lastTs) {
          lastTs = ts;
      }

      entry = cursor.nextEntry();

      ODatabaseRecordThreadLocal.INSTANCE.set(testDocumentTx);
      OIndex testIndex = testDocumentTx.getMetadata().getIndexManager().getIndex("mi");

      Set<OIdentifiable> result = (Set<OIdentifiable>) testIndex.get(key);
      if (result == null || result.isEmpty()) {
        if (minLostTs > ts) {
            minLostTs = ts;
        }
      } else {
          restoredRecords++;
      }
    }

    ODatabaseRecordThreadLocal.INSTANCE.set(baseDocumentTx);
    System.out.println("Restored entries : " + restoredRecords + " out of : "
        + baseDocumentTx.getMetadata().getIndexManager().getIndex("mi").getSize());
    System.out.println("Lost records max interval : " + (minLostTs == Long.MAX_VALUE ? 0 : lastTs - minLostTs));
  }

  private void createSchema(ODatabaseDocumentTx dbDocumentTx) {
    ODatabaseRecordThreadLocal.INSTANCE.set(dbDocumentTx);
    dbDocumentTx.command(new OCommandSQL("create index mi notunique integer")).execute();
    dbDocumentTx.getMetadata().getIndexManager().reload();
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
        baseDB.close();
        testDB.close();
      }
    }
  }

}
