package com.orientechnologies.orient.test.database.auto;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.orientechnologies.orient.server.OServer;

public class RunServerTest {

  private OServer server;

  @BeforeSuite
  public void before() throws Exception {
    server = new OServer();
    server.startup(RunServerTest.class.getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @AfterSuite
  public void after() {
    server.shutdown();
  }

}
