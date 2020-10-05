package com.orientechnologies.orient.test.database.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.test.server.OServerTest;
import org.junit.Test;

import java.io.File;

public class SystemUsersTest {

//  @Test
  public void test() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    System.setProperty(
            "ORIENTDB_HOME", buildDirectory + File.separator + SystemUsersTest.class.getSimpleName());

    OLogManager.instance().info(this, "ORIENTDB_HOME: " + System.getProperty("ORIENTDB_HOME"));

    OrientDB orient = new OrientDB("plocal:target/" + SystemUsersTest.class.getSimpleName(), OrientDBConfig.defaultConfig());

    try {
      orient.create("test", ODatabaseType.MEMORY);
      orient.execute("create system user systemxx identified by systemxx role admin").close();
      ODatabaseSession db = orient.open("test", "systemxx", "systemxx");

      db.close();
    } finally {
      orient.close();
    }
  }
}
