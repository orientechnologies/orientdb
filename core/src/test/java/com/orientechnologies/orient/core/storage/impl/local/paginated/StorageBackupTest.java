package com.orientechnologies.orient.core.storage.impl.local.paginated;

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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/9/2015
 */
@Test(enabled = false)
public class StorageBackupTest {
  private String buildDirectory;

  @BeforeClass
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".");
  }

  public void testSingeThreadFullBackup() throws IOException {
    final String dbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("data", OType.BINARY);

    backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());
    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName() + "BackUp";
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

    db.open("admin", "admin");
    db.drop();

    backedUpDb.open("admin", "admin");
    backedUpDb.drop();

    OFileUtils.deleteRecursively(backupDir);
  }

  public void testSingeThreadIncrementalBackup() throws IOException {
    final String dbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(dbDirectory));

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbDirectory);
    db.create();

    final OSchema schema = db.getMetadata().getSchema();
    final OClass backupClass = schema.createClass("BackupClass");
    backupClass.createProperty("num", OType.INTEGER);
    backupClass.createProperty("data", OType.BINARY);

    backupClass.createIndex("backupIndex", OClass.INDEX_TYPE.NOTUNIQUE, "num");

    final Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      final byte[] data = new byte[16];
      random.nextBytes(data);

      final int num = random.nextInt();

      final ODocument document = new ODocument("BackupClass");
      document.field("num", num);
      document.field("data", data);

      document.save();
    }

    final File backupDir = new File(buildDirectory, "backupDir");
    OFileUtils.deleteRecursively(backupDir);

    if (!backupDir.exists())
      Assert.assertTrue(backupDir.mkdirs());

    db.incrementalBackup(backupDir.getAbsolutePath());

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < 1000; i++) {
        final byte[] data = new byte[16];
        random.nextBytes(data);

        final int num = random.nextInt();

        final ODocument document = new ODocument("BackupClass");
        document.field("num", num);
        document.field("data", data);

        document.save();
      }

      db.incrementalBackup(backupDir.getAbsolutePath());
    }

    db.incrementalBackup(backupDir.getAbsolutePath());

    final OStorage storage = db.getStorage();
    db.close();

    storage.close(true, false);

    final String backedUpDbDirectory = buildDirectory + File.separator + StorageBackupTest.class.getSimpleName() + "BackUp";
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

    // db.open("admin", "admin");
    // db.drop();
    //
    // backedUpDb.open("admin", "admin");
    // backedUpDb.drop();
    //
    // OFileUtils.deleteRecursively(backupDir);
  }

}