package com.orientechnologies.security;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class BaseServerSecurityTest {

  @Rule public TestName name = new TestName();

  protected OServer server;

  protected OrientDB remote;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-simple-config.xml");

    remote = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());

    if (shouldCreateDatabase()) {
      remote.execute(
          "create database `"
              + name.getMethodName()
              + "` plocal users(admin identified by 'admin' role admin, reader identified by"
              + " 'reader' role reader, writer identified by 'writer' role writer)");
    }
  }

  @After
  public void after() {

    remote.drop(name.getMethodName());
    remote.close();

    server.shutdown();
  }

  protected boolean shouldCreateDatabase() {
    return true;
  }
}
