package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;

public class TestConfigurationReloadOnRemote {

  private OServer             server;
  private static final String SERVER_DIRECTORY = "./target/db";

  @Before
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    createDatabase();
  }

  @Test
  public void testSetConfiguration() {
    ODatabaseDocumentInternal db = new ODatabaseDocumentTx("remote:localhost/test");
    try {
      db.open("admin", "admin");
      db.command(new OCommandSQL("alter database DATETIMEFORMAT 'yyyy-MM-dd'T'HH:mm:ssXXX'")).execute();
      db.reload();
      assertEquals(db.getStorage().getConfiguration().getDateTimeFormat(), "yyyy-MM-dd'T'HH:mm:ssXXX");
    } finally {
      db.close();
    }
  }

  private void dropDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    admin.dropDatabase("plocal");
  }

  private void createDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    admin.createDatabase("document", "plocal");
  }

  @After
  public void after() throws IOException {
    dropDatabase();
    server.shutdown();
    Orient.instance().startup();
  }

}
