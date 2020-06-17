package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleMultiNodeConnectIT {

  private OServer server0;
  private OServer server1;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    ODatabaseSession session = remote.open("test", "admin", "admin");
    session.createClass("test");
    OElement doc = session.newElement("test");
    doc.setProperty("name", "value");
    session.save(doc);
    session.close();
    remote.close();
  }

  @Test
  public void testLiveQueryDifferentNode() {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost:2424;localhost:2425", "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session = remote1.open("test", "admin", "admin");
    try (OResultSet result = session.query("select from test")) {
      assertEquals(1, result.stream().count());
    }
    server0.shutdown();
    try (OResultSet result = session.query("select from test")) {
      assertEquals(1, result.stream().count());
    }
    remote1.close();
  }

  @After
  public void after()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException,
          NoSuchMethodException, IOException, InvocationTargetException {
    server0.startupFromConfiguration();
    server0.activate();
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
  }
}
