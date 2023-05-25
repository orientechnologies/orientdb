package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.io.OFileUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.AfterClass;
import org.junit.Test;

public class BackupRestoreTest {
  public static final int repeat = 5;

  public static final String basePath = "target/backupRestoreTest";

  public final File backupFile = new File(basePath, "backup.zip");

  @AfterClass
  public static void clean() throws IOException {
    OFileUtils.deleteRecursively(new File(basePath));
  }

  @Test
  public void testBackupRestore() throws IOException {
    try (OrientDB db = new OrientDB("embedded:" + basePath, OrientDBConfig.defaultConfig())) {
      db.execute(
          "create database testdb plocal users (admin identified by 'adminpwd' role admin) ");

      backup(db);

      for (int i = 0; i < repeat; i++) {
        restore(db);
      }
    }
  }

  protected void backup(OrientDB db) throws IOException {
    try (ODatabaseSession session = db.open("testdb", "admin", "adminpwd")) {
      try (OutputStream out = new FileOutputStream(backupFile)) {
        session.backup(out, null, null, null, 1, 2048);
      }
    }
  }

  protected void restore(OrientDB db) throws IOException {
    try (ODatabaseSession session = db.open("testdb", "admin", "adminpwd")) {
      try (InputStream in = new FileInputStream(backupFile)) {
        session.restore(in, null, null, null);
      }
    }
  }
}
