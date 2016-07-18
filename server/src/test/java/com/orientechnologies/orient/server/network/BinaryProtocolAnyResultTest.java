package com.orientechnologies.orient.server.network;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 26/04/16.
 */
public class BinaryProtocolAnyResultTest {

  private static final String SERVER_DIRECTORY = "./target/db";
  private OServer             server;

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    server.createDatabase("test", "graph", "memory");

  }

  @Test
  public void scriptReturnValueTest() throws IOException {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test");
    db.open("admin", "admin");

    Object res = db.command(new OCommandScript("SQL", " let $one = select from OUser limit 2; return [$one,1]")).execute();

    assertTrue(res instanceof List);
    assertTrue(((List) res).get(0) instanceof Collection);
    assertTrue(((List) res).get(1) instanceof Number);
    db.close();

  }

  @Test
  public void scriptReturnCustomTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test");
    db.open("admin", "admin");

    Object res = db.command(new OCommandScript("SQL", " return [1,2,'ciao']")).execute();

    assertTrue(res instanceof List);
    assertTrue(((List) res).get(0) instanceof Number);
    assertTrue(((List) res).get(1) instanceof Number);
    assertTrue(((List) res).get(2) instanceof String);
    db.close();
  }

//  @Test
//  public void scriptReturnCustomObjectTest() {
//    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test");
//    db.open("admin", "admin");
//
//    Object res = db.command(new OCommandScript("SQL", "let $a = select 1 as one ; return { 'name' : 'Foo' }")).execute();
//
//    assertTrue(res instanceof Map);
//    assertTrue(((Map) res).get("name") instanceof String);
//    assertEquals("Foo", ((Map) res).get("name"));
//    db.close();
//  }

  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
  }

}
