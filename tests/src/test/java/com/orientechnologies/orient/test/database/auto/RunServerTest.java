package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.server.OServer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

public class RunServerTest {

  private OServer server;

  @BeforeSuite
  public void before() throws Exception {
    server = new OServer();
    server.startup(RunServerTest.class.getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void test(){

  }
  @AfterSuite
  public void after() {
    server.shutdown();
  }

}
