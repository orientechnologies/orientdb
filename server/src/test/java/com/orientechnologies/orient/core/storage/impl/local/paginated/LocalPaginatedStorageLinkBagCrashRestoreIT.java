package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OOneEntryPerKeyLockManager;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalPaginatedStorageLinkBagCrashRestoreIT {
  private static String URL_BASE;
  private static String URL_TEST;
  private final OOneEntryPerKeyLockManager<ORID> lockManager     = new OOneEntryPerKeyLockManager<ORID>(true, 30000, 10000);
  private final AtomicInteger                    positionCounter = new AtomicInteger();
  private final OPartitionedDatabasePoolFactory  poolFactory     = new OPartitionedDatabasePoolFactory();
  private File buildDir;
  private ExecutorService executorService = Executors.newCachedThreadPool();
  private          Process process;
  private          int     defaultClusterId;
  private volatile long    lastClusterPosition;

  @Before
  public void spawnServer() throws Exception {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(10);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageLinkBagCrashRestore";

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

    ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx4096m", "-XX:MaxDirectMemorySize=512g", "-classpath",
        System.getProperty("java.class.path"), "-DmutexFile=" + mutexFile.getCanonicalPath(), "-DORIENTDB_HOME=" + buildDirectory,
        RemoteDBRunner.class.getName());
    processBuilder.inheritIO();

    process = processBuilder.start();

    System.out.println(LocalPaginatedStorageLinkBagCrashRestoreIT.class.getSimpleName() + ": Wait for server start");
    boolean started = false;
    do {
      Thread.sleep(5000);
      mutex.seek(0);
      started = mutex.read() == 1;
    } while (!started);

    mutex.close();
    mutexFile.delete();
    System.out.println(LocalPaginatedStorageLinkBagCrashRestoreIT.class.getSimpleName() + ": Server was started");
  }

  @Test
  public void testDocumentCreation() throws Exception {

    final OServerAdmin serverAdmin = new OServerAdmin("remote:localhost:3500");
    serverAdmin.connect("root", "root");
    serverAdmin.createDatabase("testLocalPaginatedStorageLinkBagCrashRestore", "graph", "plocal");
    serverAdmin.close();

    ODatabaseDocumentTx base_db = new ODatabaseDocumentTx("plocal:" + buildDir + "/baseLocalPaginatedStorageLinkBagCrashRestore");
    if (base_db.exists()) {
      base_db.open("admin", "admin");
      base_db.drop();
    }

    base_db.create();

    URL_BASE = base_db.getURL();
    defaultClusterId = base_db.getDefaultClusterId();
    base_db.close();

    URL_TEST = "remote:localhost:3500/testLocalPaginatedStorageLinkBagCrashRestore";

    List<Future<Void>> futures = new ArrayList<Future<Void>>();
    futures.add(executorService.submit(new DocumentAdder()));
    TimeUnit.SECONDS.sleep(1);

    for (int i = 0; i < 5; i++)
      futures.add(executorService.submit(new RidAdder()));

    for (int i = 0; i < 5; i++)
      futures.add(executorService.submit(new RidDeleter()));

    System.out.println("Wait for 5 minutes");
    TimeUnit.MINUTES.sleep(5);
    long lastTs = System.currentTimeMillis();

    ODatabaseDocumentTx test_db = poolFactory.get(URL_TEST, "admin", "admin").acquire();
    System.out.println(test_db.countClusterElements(test_db.getDefaultClusterId()));
    test_db.close();

    System.out.println("Wait for process to destroy");
    process.destroyForcibly();

    process.waitFor();
    System.out.println("Process was destroyed");

    for (Future<Void> future : futures)
      try {
        future.get();
      } catch (ExecutionException e) {
        e.getCause().printStackTrace();
      }

    compareDocuments(lastTs);
  }

  @After
  public void afterClass() throws Exception {
    ODatabaseDocumentTx base_db = new ODatabaseDocumentTx("plocal:" + buildDir + "/baseLocalPaginatedStorageLinkBagCrashRestore");
    if (base_db.exists()) {
      base_db.open("admin", "admin");
      base_db.drop();
    }

    ODatabaseDocumentTx test_db = new ODatabaseDocumentTx(
        "plocal:" + new File(new File(buildDir, "databases"), "testLocalPaginatedStorageLinkBagCrashRestore").getCanonicalPath());
    if (test_db.exists()) {
      test_db.open("admin", "admin");
      test_db.drop();
    }

    OFileUtils.deleteRecursively(buildDir);
    Assert.assertFalse(buildDir.exists());

  }

  private void compareDocuments(long lastTs) throws IOException {
    ODatabaseDocumentTx base_db = new ODatabaseDocumentTx("plocal:" + buildDir + "/baseLocalPaginatedStorageLinkBagCrashRestore");
    base_db.open("admin", "admin");

    ODatabaseDocumentTx test_db = new ODatabaseDocumentTx(
        "plocal:" + new File(new File(buildDir, "databases"), "testLocalPaginatedStorageLinkBagCrashRestore").getCanonicalPath());

    test_db.open("admin", "admin");

    long minTs = Long.MAX_VALUE;

    OStorage baseStorage = base_db.getStorage();

    OPhysicalPosition[] physicalPositions = baseStorage.ceilingPhysicalPositions(defaultClusterId, new OPhysicalPosition(0));

    int recordsRestored = 0;
    int recordsTested = 0;
    while (physicalPositions.length > 0) {
      final ORecordId rid = new ORecordId(defaultClusterId);

      for (OPhysicalPosition physicalPosition : physicalPositions) {
        rid.setClusterPosition(physicalPosition.clusterPosition);

        ODatabaseRecordThreadLocal.INSTANCE.set(base_db);
        ODocument baseDocument = base_db.load(rid);
        baseDocument.setLazyLoad(false);

        ODatabaseRecordThreadLocal.INSTANCE.set(test_db);
        ODocument testDocument = test_db.load(rid);
        if (testDocument == null) {
          ODatabaseRecordThreadLocal.INSTANCE.set(base_db);
          if (((Long) baseDocument.field("ts")) < minTs)
            minTs = baseDocument.field("ts");
        } else {
          testDocument.setLazyLoad(false);
          long baseTs;
          long testTs;

          ODatabaseRecordThreadLocal.INSTANCE.set(base_db);
          baseTs = baseDocument.field("ts");

          ODatabaseRecordThreadLocal.INSTANCE.set(test_db);
          testTs = testDocument.field("ts");

          boolean equals = baseTs == testTs;

          if (equals) {
            Set<ORID> baseRids = new HashSet<ORID>();
            ODatabaseRecordThreadLocal.INSTANCE.set(base_db);
            ORidBag baseRidBag = baseDocument.field("ridBag");

            for (OIdentifiable baseIdentifiable : baseRidBag)
              baseRids.add(baseIdentifiable.getIdentity());

            Set<ORID> testRids = new HashSet<ORID>();
            ODatabaseRecordThreadLocal.INSTANCE.set(test_db);
            ORidBag testRidBag = testDocument.field("ridBag");

            for (OIdentifiable testIdentifiable : testRidBag)
              testRids.add(testIdentifiable.getIdentity());

            equals = baseRids.equals(testRids);
          }

          if (!equals) {
            if (((Long) baseDocument.field("ts")) < minTs)
              minTs = baseDocument.field("ts");
          } else
            recordsRestored++;
        }

        recordsTested++;

        if (recordsTested % 10000 == 0)
          System.out.println(recordsTested + " were tested, " + recordsRestored + " were restored ...");
      }

      physicalPositions = baseStorage.higherPhysicalPositions(defaultClusterId, physicalPositions[physicalPositions.length - 1]);
    }

    System.out.println(
        recordsRestored + " records were restored. Total records " + recordsTested + ". lost records " + (recordsTested
            - recordsRestored));
    long maxInterval = minTs == Long.MAX_VALUE ? 0 : lastTs - minTs;
    System.out.println("Lost records max interval (ms) : " + maxInterval);

    assertThat(maxInterval).isLessThan(2000);

    base_db.activateOnCurrentThread();
    base_db.close();
    test_db.activateOnCurrentThread();
    test_db.close();
  }

  public static final class RemoteDBRunner {
    public static void main(String[] args) throws Exception {
      OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(5);
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
      OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);

      OServer server = OServerMain.create();
      server.startup(RemoteDBRunner.class
          .getResourceAsStream("/com/orientechnologies/orient/core/storage/impl/local/paginated/db-linkbag-crash-config.xml"));
      server.activate();

      final String mutexFile = System.getProperty("mutexFile");
      final RandomAccessFile mutex = new RandomAccessFile(mutexFile, "rw");
      mutex.seek(0);
      mutex.write(1);
      mutex.close();
    }
  }

  public final class DocumentAdder implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (true) {
        final long ts = System.currentTimeMillis();

        try {
          ODatabaseDocumentTx base_db = poolFactory.get(URL_BASE, "admin", "admin").acquire();

          base_db.activateOnCurrentThread();
          ODocument base_document = addDocument(ts);

          ODatabaseDocumentTx test_db = poolFactory.get(URL_TEST, "admin", "admin").acquire();
          test_db.activateOnCurrentThread();
          ODocument test_document = addDocument(ts);

          Assert.assertTrue(ODocumentHelper.hasSameContentOf(base_document, base_db, test_document, test_db, null));

          base_db.activateOnCurrentThread();
          base_db.close();

          test_db.activateOnCurrentThread();
          test_db.close();

          lastClusterPosition = base_document.getIdentity().getClusterPosition();
        } catch (RuntimeException e) {
          e.printStackTrace();
          throw e;
        }
      }
    }

    private ODocument addDocument(long ts) {
      ODocument document = new ODocument();
      ORidBag ridBag = new ORidBag();
      document.field("ridBag", ridBag);
      document.field("ts", ts);
      document.save();
      return document;
    }
  }

  public class RidAdder implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      final Random random = new Random();
      while (true) {
        final long ts = System.currentTimeMillis();

        final int position = random.nextInt((int) lastClusterPosition);
        final ORID orid = new ORecordId(defaultClusterId, position);

        lockManager.acquireLock(orid, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
        try {

          try {
            final List<ORID> ridsToAdd = new ArrayList<ORID>(10);
            for (int i = 0; i < 10; i++)
              ridsToAdd.add(new ORecordId(0, positionCounter.incrementAndGet()));

            ODatabaseDocumentTx base_db = poolFactory.get(URL_BASE, "admin", "admin").acquire();
            addRids(orid, base_db, ridsToAdd, ts);
            base_db.close();

            ODatabaseDocumentTx test_db = poolFactory.get(URL_TEST, "admin", "admin").acquire();
            addRids(orid, test_db, ridsToAdd, ts);
            test_db.close();

          } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
          }
        } finally {
          lockManager.releaseLock(this, orid, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
        }
      }
    }

    private void addRids(ORID docRid, ODatabaseDocumentTx db, List<ORID> ridsToAdd, long ts) {
      ODocument document = db.load(docRid);
      document.field("ts", ts);
      document.setLazyLoad(false);

      ORidBag ridBag = document.field("ridBag");
      for (ORID rid : ridsToAdd)
        ridBag.add(rid);

      document.save();
    }
  }

  public class RidDeleter implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      final Random random = new Random();
      try {
        while (true) {
          if (lastClusterPosition <= 0)
            continue;

          final long ts = System.currentTimeMillis();
          final long position = random.nextInt((int) lastClusterPosition);
          final ORID orid = new ORecordId(defaultClusterId, position);

          lockManager.acquireLock(orid, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
          try {
            ODatabaseDocumentTx base_db = poolFactory.get(URL_BASE, "admin", "admin").acquire();
            final List<ORID> ridsToRemove = new ArrayList<ORID>();

            ODocument document = base_db.load(orid);
            document.setLazyLoad(false);
            ORidBag ridBag = document.field("ridBag");

            for (OIdentifiable identifiable : ridBag) {
              if (random.nextBoolean())
                ridsToRemove.add(identifiable.getIdentity());

              if (ridsToRemove.size() >= 5)
                break;
            }

            for (ORID ridToRemove : ridsToRemove)
              ridBag.remove(ridToRemove);

            document.field("ts", ts);
            document.save();

            base_db.close();

            ODatabaseDocumentTx test_db = poolFactory.get(URL_TEST, "admin", "admin").acquire();
            document = test_db.load(orid);
            document.setLazyLoad(false);

            ridBag = document.field("ridBag");
            for (ORID ridToRemove : ridsToRemove)
              ridBag.remove(ridToRemove);

            document.field("ts", ts);
            document.save();

            test_db.close();
          } finally {
            lockManager.releaseLock(this, orid, OOneEntryPerKeyLockManager.LOCK.EXCLUSIVE);
          }
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }
}
