package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class ODropDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    String dbName = "ODropDatabaseStatementExecutionTest_testPlain";
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    try {
      try (OResultSet result = orientDb.execute("create database " + dbName + " plocal")) {
        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        Assert.assertEquals(true, item.getProperty("created"));
      }
      Assert.assertTrue(orientDb.exists(dbName));

      ODatabaseSession session = orientDb.open(dbName, "admin", "admin");
      session.close();

      orientDb.execute("drop database " + dbName);
      Assert.assertFalse(orientDb.exists(dbName));
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void testIfExists1() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists1";
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    try {
      try (OResultSet result = orientDb.execute("create database " + dbName + " plocal")) {
        Assert.assertTrue(result.hasNext());
        OResult item = result.next();
        Assert.assertEquals(true, item.getProperty("created"));
      }
      Assert.assertTrue(orientDb.exists(dbName));

      ODatabaseSession session = orientDb.open(dbName, "admin", "admin");
      session.close();

      orientDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(orientDb.exists(dbName));
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void testIfExists2() {
    String dbName = "ODropDatabaseStatementExecutionTest_testIfExists2";
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    try {
      orientDb.execute("drop database " + dbName + " if exists");
      Assert.assertFalse(orientDb.exists(dbName));
    } finally {
      orientDb.close();
    }
  }
}
