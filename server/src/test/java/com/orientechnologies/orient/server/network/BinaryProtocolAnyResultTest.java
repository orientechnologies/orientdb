package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
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

  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  @Ignore
  public void scriptReturnValueTest() throws IOException {
    OrientDB orient = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());

    if (orient.exists("test")) {
      orient.drop("test");
    }

    orient.create("test", ODatabaseType.MEMORY);
    ODatabaseSession db = orient.open("test", "admin", "admin");

    Object res = db.execute("SQL", " let $one = select from OUser limit 1; return [$one,1]");

    assertTrue(res instanceof List);
    assertTrue(((List) res).get(0) instanceof Collection);
    assertTrue(((List) res).get(1) instanceof Integer);
    db.close();

    orient.drop("test");
    orient.close();
  }

  @After
  public void after() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }
}
