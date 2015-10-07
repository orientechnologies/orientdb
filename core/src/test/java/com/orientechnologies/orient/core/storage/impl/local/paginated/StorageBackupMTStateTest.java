package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/6/2015
 */
@Test(enabled = false)
public class StorageBackupMTStateTest {
  static {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
  }

  private final OReadersWriterSpinLock               flowLock               = new OReadersWriterSpinLock();

  private final ConcurrentMap<String, AtomicInteger> classInstancesCounters = new ConcurrentHashMap<String, AtomicInteger>();

  private final AtomicInteger                        classCounter           = new AtomicInteger();

  private final String                               CLASS_PREFIX           = "StorageBackupMTStateTest";
  private String                                     dbURL;
  private File                                       backupDir;
  private volatile boolean                           stop                   = false;

  private volatile OPartitionedDatabasePool          pool;

  public void testRun() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    String dbDirectory = buildDirectory + File.separator + StorageBackupMTStateTest.class.getSimpleName();

    System.out.println("Clean up old data");

    OFileUtils.deleteRecursively(new File(dbDirectory));

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupMTStateTest.class.getSimpleName() + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    backupDir = new File(buildDirectory, StorageBackupMTStateTest.class.getSimpleName() + "BackupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    dbURL = "plocal:" + dbDirectory;

    System.out.println("Create database");
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
    databaseDocumentTx.create();

    System.out.println("Create schema");
    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    for (int i = 0; i < 3; i++) {
      createClass(schema);
    }

    databaseDocumentTx.close();

    pool = new OPartitionedDatabasePool(dbURL, "admin", "admin");

    System.out.println("Start insertion");
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final ScheduledExecutorService backupExecutor = Executors.newSingleThreadScheduledExecutor();
    backupExecutor.scheduleWithFixedDelay(new IncrementalBackupThread(), 1, 1, TimeUnit.MINUTES);

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    futures.add(executor.submit(new NonTxInserter()));
    futures.add(executor.submit(new NonTxInserter()));
    futures.add(executor.submit(new TxInserter()));
    futures.add(executor.submit(new TxInserter()));

    int k = 0;
    while (k < 10) {
      Thread.sleep(30 * 1000);
      k++;

      System.out.println(k * 0.5 + " minutes...");
    }

    stop = true;

    for (Future<Void> future : futures)
      future.get();

    backupExecutor.shutdown();
    backupExecutor.awaitTermination(15, TimeUnit.MINUTES);

    System.out.println("Stop insertion ");

    pool.close();

    databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.incrementalBackup(backupDir.getAbsolutePath());

    OStorage storage = databaseDocumentTx.getStorage();
    databaseDocumentTx.close();

    storage.close(true, false);

    System.out.println("Create backup database");
    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create();

    System.out.println("Restore database");
    backedUpDb.incrementalRestore(backupDir.getAbsolutePath());
    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true, false);

    System.out.println("Compare databases");

    final ODatabaseCompare compare = new ODatabaseCompare("plocal:" + dbDirectory, "plocal:" + backedUpDbDirectory, "admin",
        "admin", new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });

    Assert.assertTrue(compare.compare());

    System.out.println("Drop databases and backup directory");

    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.drop();

    backedUpDb.open("admin", "admin");
    backedUpDb.drop();

    OFileUtils.deleteRecursively(backupDir);
  }

  private OClass createClass(OSchema schema) {
    OClass cls = schema.createClass(CLASS_PREFIX + classCounter.getAndIncrement());

    cls.createProperty("id", OType.LONG);
    cls.createProperty("intValue", OType.INTEGER);
    cls.createProperty("stringValue", OType.STRING);
    cls.createProperty("linkedDocuments", OType.LINKBAG);

    cls.createIndex(cls.getName() + "IdIndex", OClass.INDEX_TYPE.UNIQUE, "id");
    cls.createIndex(cls.getName() + "IntValueIndex", OClass.INDEX_TYPE.NOTUNIQUE, "intValue");

    classInstancesCounters.put(cls.getName(), new AtomicInteger());

    return cls;
  }

  private final class NonTxInserter extends Inserter {
    @Override
    public Void call() throws Exception {
      while (!stop) {
        while (true) {
          ODatabaseDocumentTx db = pool.acquire();
          try {
            flowLock.acquireReadLock();
            try {
              insertRecord(db);
              break;
            } finally {
              flowLock.releaseReadLock();
            }
          } catch (ORecordNotFoundException rne) {
            // retry
          } catch (OConcurrentModificationException cme) {
            // retry
          } catch (OModificationOperationProhibitedException e) {
            System.out.println("Modification prohibited , wait 5s ...");
            Thread.sleep(5000);
            // retry
          } catch (Exception e) {
            e.printStackTrace();
            throw e;
          } finally {
            db.close();
          }
        }
      }

      return null;
    }
  }

  private final class TxInserter extends Inserter {
    @Override
    public Void call() throws Exception {

      while (!stop) {
        while (true) {
          ODatabaseDocumentTx db = pool.acquire();
          try {
            flowLock.acquireReadLock();
            try {
              db.begin();
              insertRecord(db);
              db.commit();
              break;
            } finally {
              flowLock.releaseReadLock();
            }
          } catch (ORecordNotFoundException rne) {
            // retry
          } catch (OConcurrentModificationException cme) {
            // retry
          } catch (OModificationOperationProhibitedException e) {
            System.out.println("Modification prohibited , wait 5s ...");
            Thread.sleep(5000);
            // retry
          } catch (Exception e) {
            e.printStackTrace();
            throw e;
          } finally {
            db.close();
          }
        }
      }

      return null;
    }
  }

  private abstract class Inserter implements Callable<Void> {
    protected final Random random = new Random();

    protected void insertRecord(ODatabaseDocumentTx db) {
      final int classes = classCounter.get();

      String className;
      AtomicInteger classCounter;

      do {
        className = CLASS_PREFIX + random.nextInt(classes);
        classCounter = classInstancesCounters.get(className);
      } while (classCounter == null);

      final ODocument doc = new ODocument(className);
      doc.field("id", classCounter.getAndIncrement());
      doc.field("stringValue", "value");
      doc.field("intValue", random.nextInt(1024));

      String linkedClassName;
      AtomicInteger linkedClassCounter = null;

      do {
        linkedClassName = CLASS_PREFIX + random.nextInt(classes);

        if (linkedClassName.equalsIgnoreCase(className))
          continue;

        linkedClassCounter = classInstancesCounters.get(linkedClassName);
      } while (linkedClassCounter == null);

      ORidBag linkedDocuments = new ORidBag();

      while (linkedDocuments.size() < 5 && linkedDocuments.size() < linkedClassCounter.get()) {
        List<ODocument> docs = db.query(new OSQLSynchQuery<ODocument>("select * from " + linkedClassName + " where id="
            + random.nextInt(linkedClassCounter.get())));

        if (docs.size() > 0)
          linkedDocuments.add(docs.get(0));

      }

      doc.field("linkedDocuments", linkedDocuments);
      doc.save();
    }
  }

  private final class IncrementalBackupThread implements Runnable {
    @Override
    public void run() {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbURL);
      db.open("admin", "admin");
      try {
        System.out.println("Start backup");
        db.incrementalBackup(backupDir.getAbsolutePath());
        System.out.println("End backup");
      } finally {
        db.close();
      }
    }
  }
}
