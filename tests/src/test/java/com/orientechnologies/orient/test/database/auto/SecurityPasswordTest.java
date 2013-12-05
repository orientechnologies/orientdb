package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;

public class SecurityPasswordTest {

  @Test
  public void testChangeAdminPassword() throws Exception {

    // TODO: merge this test to com.orientechnologies.orient.test.database.auto.SecurityTest

    // set ORIENTDB_HOME
    System.setProperty("ORIENTDB_HOME", System.getProperty("java.io.tmpdir") + "/orientdb");
    OLogManager.instance().info(this, "ORIENTDB_HOME: " + System.getProperty("ORIENTDB_HOME"));

    OServer server = OServerMain.create().startup().activate();
    try {
      // create database if does not exist
      OObjectDatabaseTx database = new OObjectDatabaseTx("local:" + System.getProperty("ORIENTDB_HOME") + "/test-db");
      if(!database.exists()) database.create();

      database.open("admin", "admin");
      String newPassword = "password";
      database.begin();
      database.command(new OCommandSQL("update ouser set password = '" + newPassword + "' where name = 'admin'")).execute();
      database.commit();
      database.close();

      database.open("admin", newPassword);
      try {
        database.countClass("ouser");
      } catch(OSecurityAccessException e) {
        Assert.fail("Should not throw OSecurityAccessException", e);
      }

    } finally {
      server.shutdown();
    }
  }
}
