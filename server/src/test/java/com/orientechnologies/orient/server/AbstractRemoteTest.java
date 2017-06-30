package com.orientechnologies.orient.server;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.InputStream;

/**
 * Created by Enrico Risa on 14/03/17.
 */
public class AbstractRemoteTest {

  protected static final String SERVER_DIRECTORY = "./target";
  @Rule
  public TestName name = new TestName();
  private OServer server;

  @Before
  public void setup() throws Exception {

    System.setProperty("ORIENTDB_HOME", SERVER_DIRECTORY);

    InputStream stream = AbstractRemoteTest.class.getClassLoader().getSystemResourceAsStream("abstract-orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);
    server.activate();

//    server.createDatabase(name.getMethodName(), ODatabaseType.MEMORY, OrientDBConfig.defaultConfig());

    OServerAdmin serverAdmin = new OServerAdmin("remote:localhost");

    serverAdmin.connect("root", "root");

    serverAdmin.createDatabase(name.getMethodName(), "graph", "memory");

    serverAdmin.close();

  }

  @After
  public void teardown() {
    server.shutdown();
    Orient.instance().startup();
  }
}
