package com.orientechnologies.orient.test.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.testng.annotations.Test;

public class OServerTest {

  /** Test for https://github.com/orientechnologies/orientdb/issues/1667 */
  @Test
  public void testRestart() throws Exception {
    // set ORIENTDB_HOME
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    System.setProperty(
        "ORIENTDB_HOME", buildDirectory + File.separator + OServerTest.class.getSimpleName());

    OLogManager.instance().info(this, "ORIENTDB_HOME: " + System.getProperty("ORIENTDB_HOME"));

    // loop for start & stop server
    for (int i = 0; i < 5; i++) {
      OLogManager.instance().info(this, "Iteration " + i);
      OServer server = new OServer(false).activate();
      // create database if does not exist
      OObjectDatabaseTx database =
          new OObjectDatabaseTx("plocal:" + System.getProperty("ORIENTDB_HOME") + "/test-db");
      if (!database.exists()) database.create();
      database.open("admin", "admin");
      database.countClass("ouser");
      database.close();
      server.shutdown();
    }
  }
}
