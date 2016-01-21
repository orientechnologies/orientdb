package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/17/2015
 */
@Test(enabled = false)
public class StorageBackupMTTest {
  private ODatabaseDocumentTx  databaseDocumentTx;
  private volatile boolean     stop  = false;
  private final CountDownLatch latch = new CountDownLatch(1);
  private String               dbURL;

  public void testParallelBackup() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    String dbDirectory = buildDirectory + File.separator + StorageBackupMTTest.class.getSimpleName();

    OFileUtils.deleteRecursively(new File(dbDirectory));

    dbURL = "plocal:" + dbDirectory;

    databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
    databaseDocumentTx.create();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("data", OType.BINARY);

    backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    final ExecutorService executor = Executors.newCachedThreadPool();
    final List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < 4; i++) {
      futures.add(executor.submit(new DataWriterCallable()));
    }

    futures.add(executor.submit(new DBBackupCallable(backupDir.getAbsolutePath())));

    latch.countDown();

    Thread.sleep(15 * 1000 * 60);

    stop = true;

    for (Future<Void> future : futures) {
      future.get();
    }

    databaseDocumentTx.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = databaseDocumentTx.getStorage();
    databaseDocumentTx.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupMTTest.class.getSimpleName() + "BackUp";
    OFileUtils.deleteRecursively(new File(backedUpDbDirectory));

    final ODatabaseDocumentTx backedUpDb = new ODatabaseDocumentTx("plocal:" + backedUpDbDirectory);
    backedUpDb.create();

    backedUpDb.incrementalRestore(backupDir.getAbsolutePath());
    final OStorage backupStorage = backedUpDb.getStorage();
    backedUpDb.close();

    backupStorage.close(true, false);

    final ODatabaseCompare compare = new ODatabaseCompare("plocal:" + dbDirectory, "plocal:" + backedUpDbDirectory, "admin",
        "admin", new OCommandOutputListener() {
          @Override
          public void onMessage(String iText) {
            System.out.println(iText);
          }
        });

    Assert.assertTrue(compare.compare());

    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.drop();

    backedUpDb.open("admin", "admin");
    backedUpDb.drop();

    OFileUtils.deleteRecursively(backupDir);
  }

  private final class DataWriterCallable implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      latch.await();
      final ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
      databaseDocumentTx.open("admin", "admin");

      final Random random = new Random();
      try {
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
          } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
          }
        }
      } finally {
        databaseDocumentTx.close();
      }

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

      final ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx(dbURL);
      databaseDocumentTx.open("admin", "admin");
      try {
        while (!stop) {
          Thread.sleep(1000 * 60 * 5);

          databaseDocumentTx.incrementalBackup(backupPath);
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      } finally {
        databaseDocumentTx.close();
      }

      return null;
    }
  }

}
