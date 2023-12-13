package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.record.OElement;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

public class BackupRestoreTest {
  public static final int repeat = 5;

  public static final String basePath = "target/backupRestoreTest";

  public final File backupFile = new File(basePath, "backup.zip");

  @AfterClass
  public static void clean() throws IOException {
    OFileUtils.deleteRecursively(new File(basePath));
  }

  private void simpleFill(OrientDB ctx) {
    try (ODatabaseSession session = ctx.open("testdb", "admin", "adminpwd")) {
      session.createClass("testClass");
      OElement element = session.newElement("testClass");
      element.setProperty("name", "alpha");
      session.save(element);
    }
  }

  private void simpleCheck(OrientDB ctx) {
    try (ODatabaseSession session = ctx.open("testdb", "admin", "adminpwd")) {
      assertEquals(session.query("select * from testClass").stream().count(), 1);
    }
  }

  @Test
  public void testBackupRestore() throws IOException {
    try (OrientDB db = new OrientDB("embedded:" + basePath, OrientDBConfig.defaultConfig())) {
      db.execute(
          "create database testdb plocal users (admin identified by 'adminpwd' role admin) ");
      simpleFill(db);
      backup(db);

      for (int i = 0; i < repeat; i++) {
        restore(db);
        simpleCheck(db);
      }
    }
  }

  @Test
  public void testBackupRestoreMemory() throws IOException {
    try (OrientDB db = new OrientDB("embedded:" + basePath, OrientDBConfig.defaultConfig())) {
      db.execute(
          "create database testdb memory users (admin identified by 'adminpwd' role admin) ");
      simpleFill(db);
      backup(db);

      for (int i = 0; i < repeat; i++) {
        restore(db);
        simpleCheck(db);
      }
    }
  }

  @Test
  @Ignore
  public void testBackupLocalToMemory() throws IOException {
    try (OrientDB db = new OrientDB("embedded:" + basePath, OrientDBConfig.defaultConfig())) {
      db.execute("create database testdb plocal users (admin identified by 'adminpwd' role admin) ")
          .close();
      simpleFill(db);
      backup(db);
      db.execute("drop database testdb").close();
      db.execute("create database testdb memory users (admin identified by 'adminpwd' role admin) ")
          .close();
      for (int i = 0; i < repeat; i++) {
        restore(db);
        simpleCheck(db);
      }
    }
  }

  @Test
  @Ignore
  public void testBackupMemoryToLocal() throws IOException {
    try (OrientDB db = new OrientDB("embedded:" + basePath, OrientDBConfig.defaultConfig())) {
      db.execute("create database testdb memory users (admin identified by 'adminpwd' role admin) ")
          .close();
      simpleFill(db);
      backup(db);
      db.execute("drop database testdb").close();
      db.execute("create database testdb plocal users (admin identified by 'adminpwd' role admin) ")
          .close();
      for (int i = 0; i < repeat; i++) {
        restore(db);
        simpleCheck(db);
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
