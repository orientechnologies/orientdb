package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import org.junit.Assert;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateDatabaseStatementExecutionTest {

  @Test
  public void testPlain() {
    final String dbName = "OCreateDatabaseStatementExecutionTest_testPlain";

    final OrientDB orientDb =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try (OResultSet result =
        orientDb.execute(
            "create database "
                + dbName
                + " plocal"
                + " users ( admin identified by '"
                + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
                + "' role admin)")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(orientDb.exists(dbName));

    try {
      final ODatabaseSession session =
          orientDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      session.close();
    } finally {
      orientDb.drop(dbName);
      orientDb.close();
    }
  }

  @Test
  public void testNoDefaultUsers() {
    String dbName = "OCreateDatabaseStatementExecutionTest_testNoDefaultUsers";
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    try (OResultSet result =
        orientDb.execute(
            "create database "
                + dbName
                + " plocal {'config':{'security.createDefaultUsers': false}}")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(orientDb.exists(dbName));

    try {
      ODatabaseSession session =
          orientDb.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      Assert.fail();
    } catch (OSecurityAccessException e) {
    } finally {
      orientDb.drop(dbName);
      orientDb.close();
    }
  }
}
