package com.orientechnologies.orient.server;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.InputStream;

/**
 * Created by Enrico Risa on 14/03/17.
 */
public class AbstractRemoteTest {

  protected static final String SERVER_DIRECTORY = "./target/remotetest";

  private OServer server;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws Exception {

    System.setProperty("ORIENTDB_HOME", SERVER_DIRECTORY);

    InputStream stream = AbstractRemoteTest.class.getClassLoader().getSystemResourceAsStream("abstract-orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);
    server.activate();

    server.createDatabase(name.getMethodName(), ODatabaseType.MEMORY, OrientDBConfig.defaultConfig());

  }

  @After
  public void teardown() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
