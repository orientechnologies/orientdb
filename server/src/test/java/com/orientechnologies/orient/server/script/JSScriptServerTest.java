package com.orientechnologies.orient.server.script;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class JSScriptServerTest {

  @Rule public TestName name = new TestName();

  private OServer server;

  @Before
  public void before() throws Exception {

    server =
        OServer.startFromStreamConfig(
            getClass().getResourceAsStream("orientdb-server-javascript-config.xml"));
  }

  @Test
  public void jsPackagesFromConfigTest() {

    OrientDB orientDB =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        name.getMethodName());
    try (ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin")) {

      try (OResultSet resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

      try (OResultSet resultSet = db.execute("javascript", "new java.util.ArrayList();")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

    } finally {

      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
