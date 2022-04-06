package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/17/2015
 */
public class StorageBackupMTTest {
  private final CountDownLatch started = new CountDownLatch(1);
  private final Stack<CountDownLatch> backupIterationRecordCount = new Stack<>();
  private final CountDownLatch finished = new CountDownLatch(1);

  private OrientDB orientDB;
  private String dbName;

  @Test
  public void testParallelBackup() throws Exception {
    backupIterationRecordCount.clear();
    for (int i = 0; i < 100; i++) {
      CountDownLatch latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }
    String buildDirectory = System.getProperty("buildDirectory", "./target") + "/backupTest";
    OFileUtils.createDirectoryTree(buildDirectory);
    dbName = StorageBackupMTTest.class.getSimpleName();
    final String dbDirectory = buildDirectory + File.separator + dbName;

    final File backupDir = new File(buildDirectory, "backupDir");
    final String backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";

    OFileUtils.deleteRecursively(new File(dbDirectory));

    try {

      orientDB = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
      orientDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

      ODatabaseDocument db = orientDB.open(dbName, "admin", "admin");

      final OSchema schema = db.getMetadata().getSchema();
      final OClass backupClass = schema.createClass("BackupClass");
      backupClass.createProperty("num", OType.INTEGER);
      backupClass.createProperty("data", OType.BINARY);

      backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

      OFileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) Assert.assertTrue(backupDir.mkdirs());

      final ExecutorService executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 4; i++) {
        Stack<CountDownLatch> producerIterationRecordCount = new Stack<>();
        for (CountDownLatch l : backupIterationRecordCount) {
          producerIterationRecordCount.add(l);
        }
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.getAbsolutePath());

      orientDB.close();

      final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
      OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      OrientDBEmbedded embedded =
          (OrientDBEmbedded)
              OrientDBInternal.embedded(buildDirectory, OrientDBConfig.defaultConfig());
      embedded.restore(
          backupDbName,
          null,
          null,
          null,
          backupDir.getAbsolutePath(),
          OrientDBConfig.defaultConfig());
      embedded.close();

      final ODatabaseCompare compare =
          new ODatabaseCompare(
              "plocal:" + dbDirectory,
              "plocal:" + backedUpDbDirectory,
              "admin",
              "admin",
              System.out::println);
      System.out.println("compare");

      boolean areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {

      try {
        ODatabaseDocumentTx.closeAll();
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
      }
      if (orientDB.isOpen()) {
        try {
          orientDB.close();
        } catch (Exception ex) {
          OLogManager.instance().error(this, "", ex);
        }
      }
      try {
        orientDB = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
        orientDB.drop(dbName);
        orientDB.drop(backupDbName);

        orientDB.close();

        OFileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
      }
    }
  }

  @Test
  public void testParallelBackupEncryption() throws Exception {
    backupIterationRecordCount.clear();
    for (int i = 0; i < 100; i++) {
      CountDownLatch latch = new CountDownLatch(4);
      backupIterationRecordCount.add(latch);
    }
    String buildDirectory = System.getProperty("buildDirectory", "./target") + "/backupTest";
    OFileUtils.createDirectoryTree(buildDirectory);

    final String backupDbName = StorageBackupMTTest.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
    final File backupDir = new File(buildDirectory, "backupDir");

    dbName = StorageBackupMTTest.class.getSimpleName();
    String dbDirectory = buildDirectory + File.separator + dbName;

    final OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();

    try {

      OFileUtils.deleteRecursively(new File(dbDirectory));

      orientDB = new OrientDB("embedded:" + buildDirectory, config);

      orientDB.execute(
          "create database `" + dbName + "` plocal users(admin identified by 'admin' role admin)");

      ODatabaseDocument db = orientDB.open(dbName, "admin", "admin");

      final OSchema schema = db.getMetadata().getSchema();
      final OClass backupClass = schema.createClass("BackupClass");
      backupClass.createProperty("num", OType.INTEGER);
      backupClass.createProperty("data", OType.BINARY);

      backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

      OFileUtils.deleteRecursively(backupDir);

      if (!backupDir.exists()) Assert.assertTrue(backupDir.mkdirs());

      final ExecutorService executor = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 4; i++) {
        Stack<CountDownLatch> producerIterationRecordCount = new Stack<>();
        for (CountDownLatch l : backupIterationRecordCount) {
          producerIterationRecordCount.add(l);
        }
        futures.add(executor.submit(new DataWriterCallable(producerIterationRecordCount, 1000)));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      started.countDown();

      finished.await();

      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.println("do inc backup last time");
      db.incrementalBackup(backupDir.getAbsolutePath());

      orientDB.close();

      OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

      System.out.println("create and restore");

      OrientDBEmbedded embedded =
          (OrientDBEmbedded) OrientDBInternal.embedded(buildDirectory, config);
      embedded.restore(backupDbName, null, null, null, backupDir.getAbsolutePath(), config);
      embedded.close();

      OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
      final ODatabaseCompare compare =
          new ODatabaseCompare(
              "plocal:" + dbDirectory,
              "plocal:" + backedUpDbDirectory,
              "admin",
              "admin",
              System.out::println);
      System.out.println("compare");

      boolean areSame = compare.compare();
      Assert.assertTrue(areSame);

    } finally {
      try {
        ODatabaseDocumentTx.closeAll();
        OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.setValue(null);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
      }
      if (orientDB.isOpen()) {
        try {
          orientDB.close();
        } catch (Exception ex) {
          OLogManager.instance().error(this, "", ex);
        }
      }
      try {
        orientDB = new OrientDB("embedded:" + buildDirectory, config);
        orientDB.drop(dbName);
        orientDB.drop(backupDbName);

        orientDB.close();

        OFileUtils.deleteRecursively(backupDir);
      } catch (Exception ex) {
        OLogManager.instance().error(this, "", ex);
      }
    }
  }

  private final class DataWriterCallable implements Callable<Void> {
    private final Stack<CountDownLatch> producerIterationRecordCount;
    private int count;

    public DataWriterCallable(Stack<CountDownLatch> producerIterationRecordCount, int count) {
      this.producerIterationRecordCount = producerIterationRecordCount;
      this.count = count;
    }

    @Override
    public Void call() throws Exception {
      started.await();

      System.out.println(Thread.currentThread() + " - start writing");

      try (ODatabaseDocument db = orientDB.open(dbName, "admin", "admin")) {

        Random random = new Random();
        List<ORID> ids = new ArrayList<>();
        while (!producerIterationRecordCount.isEmpty()) {

          for (int i = 0; i < count; i++) {
            try {
              final byte[] data = new byte[random.nextInt(1024)];
              random.nextBytes(data);

              final int num = random.nextInt();
              if (!ids.isEmpty() && i % 8 == 0) {
                ORID id = ids.remove(0);
                db.delete(id);
              } else if (!ids.isEmpty() && i % 4 == 0) {
                ORID id = ids.remove(0);
                final ODocument document = db.load(id);
                document.field("data", data);
              } else {
                final ODocument document = new ODocument("BackupClass");
                document.field("num", num);
                document.field("data", data);

                ORID id = document.save().getIdentity();
                if (ids.size() < 100) {
                  ids.add(id);
                }
              }

            } catch (OModificationOperationProhibitedException e) {
              System.out.println("Modification prohibited ... wait ...");
              Thread.sleep(1000);
            } catch (RuntimeException e) {
              e.printStackTrace();
              throw e;
            } catch (Exception e) {
              e.printStackTrace();
              throw e;
            } catch (Error e) {
              e.printStackTrace();
              throw e;
            }
          }
          producerIterationRecordCount.pop().countDown();
          System.out.println(Thread.currentThread() + " writing of a batch done");
        }

        System.out.println(Thread.currentThread() + " - done writing");
        finished.countDown();
        return null;
      }
    }
  }

  public final class DBBackupCallable implements Callable<Void> {
    private final String backupPath;

    public DBBackupCallable(String backupPath) {
      this.backupPath = backupPath;
    }

    @Override
    public Void call() throws Exception {
      started.await();

      final ODatabaseDocument db = orientDB.open(dbName, "admin", "admin");

      System.out.println(Thread.currentThread() + " - start backup");

      try {
        while (!backupIterationRecordCount.isEmpty()) {
          CountDownLatch latch = backupIterationRecordCount.pop();
          latch.await();

          System.out.println(Thread.currentThread() + " do inc backup");
          db.incrementalBackup(backupPath);
          System.out.println(Thread.currentThread() + " done inc backup");
        }
      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "", e);
        throw e;
      } catch (Exception e) {
        OLogManager.instance().error(this, "", e);
        throw e;
      } catch (Error e) {
        OLogManager.instance().error(this, "", e);
        throw e;
      } finally {
        db.close();
      }
      finished.countDown();

      System.out.println(Thread.currentThread() + " - stop backup");

      return null;
    }
  }
}
