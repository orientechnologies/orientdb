package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

@Test
public class OStorageLocalTest {
  private boolean oldStorageOpen;

  @BeforeMethod
  public void beforeMethod() {
    oldStorageOpen = OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean();
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
  }

  @AfterMethod
  public void afterMethod() {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(oldStorageOpen);
  }

  public void withLegacyPath() {
    String dbPath = getDatabasePath();

    System.out.println("Using db = plocal:" + dbPath);
    File dbDir = new File(dbPath);
    System.out.println("Clean db directory for test...");
    delTree(dbDir);
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath);
    db.create();
    db.close();

    System.out.println("Reopen it...");
    // Something was added to dbPath so the legacy situation was simulated
    dbPath += "/foo";
    db = new ODatabaseDocumentTx("plocal:" + dbPath).open("admin", "admin");
    db.drop();
  }

  public void withNormalPath() {
    String dbPath = getDatabasePath();

    System.out.println("Using db = plocal:" + dbPath);
    File dbDir = new File(dbPath);
    System.out.println("Clean db directory for test...");
    delTree(dbDir);
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath);
    db.create();
    db.close();

    System.out.println("Reopen it...");
    db = new ODatabaseDocumentTx("plocal:" + dbPath).open("admin", "admin");
    db.drop();
  }

  public void dbOperations() {
    String dbPath = getDatabasePath();

    System.out.println("Using db = plocal:" + dbPath);
    File dbDir = new File(dbPath);
    System.out.println("Clean db directory for test...");
    delTree(dbDir);
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + dbPath);
    db.create();
    db.close();

    System.out.println("Create OK!");
    db = new ODatabaseDocumentTx("plocal:" + dbPath).open("admin", "admin");
    System.out.println("Open OK!");
    Assert.assertTrue(db.exists());
    System.out.println("Exists OK!");
    db.drop();
    System.out.println("Delete OK!");
  }

  public void contextConfiguration() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:testCtxCfg").create();
    db.getConfiguration().setValue(OGlobalConfiguration.USE_WAL, false);
    Assert.assertFalse(db.getConfiguration().getValueAsBoolean(OGlobalConfiguration.USE_WAL));
  }

  private boolean delTree(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          delTree(files[i]);
        } else {
          files[i].delete();
        }
      }
    }
    return directory.delete();
  }

  private String getDatabasePath() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = "./target";

    return buildDirectory + File.separator + "OStorageLocalTestDB__42";
  }
}
