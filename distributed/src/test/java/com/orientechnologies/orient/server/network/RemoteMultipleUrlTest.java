package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Created by tglman on 27/07/17.
 */
public class RemoteMultipleUrlTest {

  private static final String SERVER_DIRECTORY = "./target/db";
  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-dserver-config-0.xml"));
    server.activate();

    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root", "test");
    server.createDatabase(RemoteMultipleUrlTest.class.getSimpleName(), "graph", "plocal");

  }

  @Test
  public void testMultipleUrl() {
    ODatabaseDocument databaseDocument = new ODatabaseDocumentTx(
        "remote:localhost:2424;localhost:2425/" + RemoteMultipleUrlTest.class.getSimpleName());
    databaseDocument.open("admin", "admin");
    databaseDocument.close();
  }

  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
  }

}
