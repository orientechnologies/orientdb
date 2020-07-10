package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/17/2015
 */
public class StorageBackupMTIT {
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile boolean stop = false;
  private OrientDB orientDB;
  private String dbName;

  @Test
  public void testParallelBackup() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    dbName = StorageBackupMTIT.class.getSimpleName();
    final String dbDirectory = buildDirectory + File.separator + dbName;
    final File backupDir = new File(buildDirectory, "backupDir");
    final String backupDbName = StorageBackupMTIT.class.getSimpleName() + "BackUp";

    OFileUtils.deleteRecursively(new File(dbDirectory));

    try {

      orientDB = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
      orientDB.create(dbName, ODatabaseType.PLOCAL);

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
        futures.add(executor.submit(new DataWriterCallable()));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      latch.countDown();

      TimeUnit.MINUTES.sleep(15);

      stop = true;

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
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String backupDbName = StorageBackupMTIT.class.getSimpleName() + "BackUp";
    final String backedUpDbDirectory = buildDirectory + File.separator + backupDbName;
    final File backupDir = new File(buildDirectory, "backupDir");

    dbName = StorageBackupMTIT.class.getSimpleName();
    String dbDirectory = buildDirectory + File.separator + dbName;

    final OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();

    try {

      OFileUtils.deleteRecursively(new File(dbDirectory));

      orientDB = new OrientDB("embedded:" + buildDirectory, config);
      orientDB.create(dbName, ODatabaseType.PLOCAL);

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
        futures.add(executor.submit(new DataWriterCallable()));
      }

      futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

      latch.countDown();

      TimeUnit.MINUTES.sleep(5);

      stop = true;

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
    @Override
    public Void call() throws Exception {
      latch.await();

      System.out.println(Thread.currentThread() + " - start writing");

      try (ODatabaseDocument db = orientDB.open(dbName, "admin", "admin")) {
        final Random random = new Random();
        while (!stop) {
          try {
            final byte[] data = new byte[16];
            random.nextBytes(data);

            final int num = random.nextInt();

            final ODocument document = new ODocument("BackupClass");
            document.field("num", num);
            document.field("data", data);

            document.save();
          } catch (OModificationOperationProhibitedException e) {
            System.out.println("Modification prohibited ... wait ...");
            Thread.sleep(1000);
          } catch (Exception | Error e) {
            OLogManager.instance().error(this, "", e);
            throw e;
          }
        }
      }

      System.out.println(Thread.currentThread() + " - done writing");

      return null;
    }
  }

  public final class DBBackupCallable implements Callable<Void> {
    private final String backupPath;

    public DBBackupCallable(String backupPath) {
      this.backupPath = backupPath;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      final ODatabaseDocument db = orientDB.open(dbName, "admin", "admin");

      System.out.println(Thread.currentThread() + " - start backup");

      try {
        while (!stop) {
          TimeUnit.MINUTES.sleep(1);

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

      System.out.println(Thread.currentThread() + " - stop backup");

      return null;
    }
  }
}
