package com.orientechnologies.security;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class EEBaseServerHttpTest {

  @Rule public TestName name = new TestName();

  protected OServer server;

  private String realm = "OrientDB-";
  private String userName = "root";
  private String userPassword = "root";
  private Boolean keepAlive = null;
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

  protected boolean isInDevelopmentMode() {
    final String env = System.getProperty("orientdb.test.env");
    return env == null || env.equals("dev");
  }

  protected EEBaseServerHttpTest setKeepAlive(final boolean iValue) {
    keepAlive = iValue;
    return this;
  }

  public String getUserName() {
    return userName;
  }

  protected EEBaseServerHttpTest setUserName(final String userName) {
    this.userName = userName;
    return this;
  }

  protected String getUserPassword() {
    return userPassword;
  }

  protected EEBaseServerHttpTest setUserPassword(final String userPassword) {
    this.userPassword = userPassword;
    return this;
  }

  protected String getRealm() {
    return realm;
  }

  protected EEBaseServerHttpTest setRealm(String realm) {
    this.realm = realm;
    return this;
  }

  protected boolean shouldCreateDatabase() {
    return true;
  }
}
