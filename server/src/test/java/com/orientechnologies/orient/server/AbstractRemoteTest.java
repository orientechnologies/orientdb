package com.orientechnologies.orient.server;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import java.io.File;
import java.io.InputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/** Created by Enrico Risa on 14/03/17. */
public class AbstractRemoteTest {

  protected static final String SERVER_DIRECTORY = "./target/remotetest";

  private OServer server;

  @Rule public TestName name = new TestName();

  @Before
  public void setup() throws Exception {

    System.setProperty("ORIENTDB_HOME", SERVER_DIRECTORY);

    InputStream stream =
        AbstractRemoteTest.class
            .getClassLoader()
            .getSystemResourceAsStream("abstract-orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);
    server.activate();

    final String dbName = name.getMethodName();
    if (dbName != null) {
      server
          .getContext()
          .execute(
              "create database ? memory users (admin identified by 'admin' role admin)", dbName);
    }
  }

  @After
  public void teardown() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
