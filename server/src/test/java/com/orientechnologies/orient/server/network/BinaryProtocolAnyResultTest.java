package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 26/04/16.
 */
public class BinaryProtocolAnyResultTest {

  private static final String SERVER_DIRECTORY = "./target/db";
  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void scriptReturnValueTest() throws IOException {
    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root","D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    server.createDatabase("test","graph","memory");
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test");
    db.open("admin","admin");

    Object res = db.command(new OCommandScript("SQL", " let $one = select from OUser limit 2; return [$one,1]")).execute();

    assertTrue(res instanceof List);
    assertTrue(((List)res).get(0) instanceof Collection);
    assertTrue(((List)res).get(1) instanceof String);
    db.close();

  }


  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
  }

}
