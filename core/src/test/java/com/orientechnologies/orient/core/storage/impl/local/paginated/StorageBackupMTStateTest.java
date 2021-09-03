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
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/6/2015
 */
public class StorageBackupMTStateTest {
  static {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(3);
    OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(10);
  }

  private final OReadersWriterSpinLock flowLock = new OReadersWriterSpinLock();

  private final ConcurrentMap<String, AtomicInteger> classInstancesCounters =
      new ConcurrentHashMap<String, AtomicInteger>();

  private final AtomicInteger classCounter = new AtomicInteger();

  private final String CLASS_PREFIX = "StorageBackupMTStateTest";
  private String dbURL;
  private File backupDir;
  private volatile boolean stop = false;

  private volatile OPartitionedDatabasePool pool;

  @Test
  @Ignore
  public void testRun() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    String dbDirectory =
        buildDirectory + File.separator + StorageBackupMTStateTest.class.getSimpleName();

    System.out.println("Clean up old data");

    OFileUtils.deleteRecursively(new File(dbDirectory));

    final String backedUpDbDirectory =
        buildDirectory + File.separator + StorageBackupMTStateTest.class.getSimpleName() + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    backupDir =
        new File(buildDirectory, StorageBackupMTStateTest.class.getSimpleName() + "BackupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists()) Assert.assertTrue(backupDir.mkdirs());

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

    System.out.println("Start data modification");
    final ExecutorService executor = Executors.newFixedThreadPool(5);
    final ScheduledExecutorService backupExecutor = Executors.newSingleThreadScheduledExecutor();
    final ScheduledExecutorService classCreatorExecutor =
        Executors.newSingleThreadScheduledExecutor();
    final ScheduledExecutorService classDeleterExecutor =
        Executors.newSingleThreadScheduledExecutor();

    classDeleterExecutor.scheduleWithFixedDelay(new ClassDeleter(), 10, 10, TimeUnit.MINUTES);
    backupExecutor.scheduleWithFixedDelay(new IncrementalBackupThread(), 5, 5, TimeUnit.MINUTES);
    classCreatorExecutor.scheduleWithFixedDelay(new ClassAdder(), 7, 5, TimeUnit.MINUTES);

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    futures.add(executor.submit(new NonTxInserter()));
    futures.add(executor.submit(new NonTxInserter()));
    futures.add(executor.submit(new TxInserter()));
    futures.add(executor.submit(new TxInserter()));
    futures.add(executor.submit(new RecordsDeleter()));

    int k = 0;
    while (k < 180) {
      Thread.sleep(30 * 1000);
      k++;

      System.out.println(k * 0.5 + " minutes...");
    }

    stop = true;

    System.out.println("Stop backup");
    backupExecutor.shutdown();

    System.out.println("Stop class creation/deletion");
    classCreatorExecutor.shutdown();
    classDeleterExecutor.shutdown();

    backupExecutor.awaitTermination(15, TimeUnit.MINUTES);
    classCreatorExecutor.awaitTermination(15, TimeUnit.MINUTES);
    classDeleterExecutor.awaitTermination(15, TimeUnit.MINUTES);

    System.out.println("Stop data threads");

    for (Future<Void> future : futures) future.get();

    System.out.println("All threads are stopped");

    pool.close();

    System.out.println("Final incremental  backup");
    databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.incrementalBackup(backupDir.getAbsolutePath());

    OStorage storage = databaseDocumentTx.getStorage();
    databaseDocumentTx.close();

    storage.close(true, false);

    System.out.println("Create backup database");
    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create(backupDir.getAbsolutePath());

    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true, false);

    System.out.println("Compare databases");

    final ODatabaseCompare compare =
        new ODatabaseCompare(
            "plocal:" + dbDirectory,
            "plocal:" + backedUpDbDirectory,
            "admin",
            "admin",
            new OCommandOutputListener() {
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
    cls.createIndex(
        cls.getName() + "IntValueIndex", OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "intValue");

    classInstancesCounters.put(cls.getName(), new AtomicInteger());

    System.out.println("Class " + cls.getName() + " is added");

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
            Thread.sleep(2000);
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
            Thread.sleep(2000);
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
      final int docId;
      final int classes = classCounter.get();

      String className;
      AtomicInteger classCounter;

      do {
        className = CLASS_PREFIX + random.nextInt(classes);
        classCounter = classInstancesCounters.get(className);
      } while (classCounter == null);

      final ODocument doc = new ODocument(className);
      docId = classCounter.getAndIncrement();

      doc.field("id", docId);
      doc.field("stringValue", "value");
      doc.field("intValue", random.nextInt(1024));

      String linkedClassName;
      AtomicInteger linkedClassCounter = null;

      do {
        linkedClassName = CLASS_PREFIX + random.nextInt(classes);

        if (linkedClassName.equalsIgnoreCase(className)) continue;

        linkedClassCounter = classInstancesCounters.get(linkedClassName);
      } while (linkedClassCounter == null);

      ORidBag linkedDocuments = new ORidBag();

      long linkedClassCount = db.countClass(linkedClassName);
      long tCount = 0;

      while (linkedDocuments.size() < 5 && linkedDocuments.size() < linkedClassCount) {
        List<ODocument> docs =
            db.query(
                new OSQLSynchQuery<ODocument>(
                    "select * from "
                        + linkedClassName
                        + " where id="
                        + random.nextInt(linkedClassCounter.get())));

        if (docs.size() > 0) linkedDocuments.add(docs.get(0));

        tCount++;

        if (tCount % 10 == 0) linkedClassCount = db.countClass(linkedClassName);
      }

      doc.field("linkedDocuments", linkedDocuments);
      doc.save();

      if (docId % 10000 == 0) {
        System.out.println(docId + " documents of class " + className + " were inserted");
      }
    }
  }

  private final class IncrementalBackupThread implements Runnable {
    @Override
    public void run() {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbURL);
      db.open("admin", "admin");
      try {
        flowLock.acquireReadLock();
        try {
          System.out.println("Start backup");
          db.incrementalBackup(backupDir.getAbsolutePath());
          System.out.println("End backup");
        } finally {
          flowLock.releaseReadLock();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        db.close();
      }
    }
  }

  private final class ClassAdder implements Runnable {
    @Override
    public void run() {
      ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
      databaseDocumentTx.open("admin", "admin");
      try {
        flowLock.acquireReadLock();
        try {
          OSchema schema = databaseDocumentTx.getMetadata().getSchema();
          createClass(schema);
        } finally {
          flowLock.releaseReadLock();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        databaseDocumentTx.close();
      }
    }
  }

  private final class RecordsDeleter implements Callable<Void> {
    private final Random random = new Random();

    @Override
    public Void call() throws Exception {
      int counter = 0;
      while (!stop) {
        while (true) {
          ODatabaseDocumentTx databaseDocumentTx = pool.acquire();
          try {
            flowLock.acquireReadLock();
            try {
              final int classes = classCounter.get();

              String className;
              AtomicInteger classCounter;

              long countClasses;
              do {
                className = CLASS_PREFIX + random.nextInt(classes);
                classCounter = classInstancesCounters.get(className);

                if (classCounter != null) countClasses = databaseDocumentTx.countClass(className);
                else countClasses = 0;
              } while (classCounter == null || countClasses == 0);

              boolean deleted = false;
              do {
                List<ODocument> docs =
                    databaseDocumentTx.query(
                        new OSQLSynchQuery<ODocument>(
                            "select * from "
                                + className
                                + " where id="
                                + random.nextInt(classCounter.get())));

                if (docs.size() > 0) {
                  final ODocument document = docs.get(0);
                  document.delete();
                  deleted = true;
                }
              } while (!deleted);

              counter++;

              if (counter % 1000 == 0) {
                System.out.println(counter + " documents are deleted");
                System.out.println("Pause for 1 second...");
                Thread.sleep(1000);
              }

              break;
            } finally {
              flowLock.releaseReadLock();
            }
          } catch (OModificationOperationProhibitedException mope) {
            System.out.println("Modification was prohibited ... wait 3s.");
            Thread.sleep(3 * 1000);
          } catch (ORecordNotFoundException rnfe) {
            // retry
          } catch (OConcurrentModificationException cme) {
            // retry
          } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
          } finally {
            databaseDocumentTx.close();
          }
        }
      }

      return null;
    }
  }

  private final class ClassDeleter implements Runnable {
    private final Random random = new Random();

    @Override
    public void run() {
      ODatabaseDocumentTx db = pool.acquire();
      try {
        flowLock.acquireWriteLock();
        try {
          final OSchema schema = db.getMetadata().getSchema();
          final int classes = classCounter.get();

          String className;
          AtomicInteger classCounter;

          do {
            className = CLASS_PREFIX + random.nextInt(classes);
            classCounter = classInstancesCounters.get(className);
          } while (classCounter == null);

          schema.dropClass(className);
          classInstancesCounters.remove(className);
          System.out.println("Class " + className + " was deleted");

        } catch (RuntimeException e) {
          e.printStackTrace();
          throw e;
        } finally {
          flowLock.releaseWriteLock();
        }
      } finally {
        db.close();
      }
    }
  }
}
