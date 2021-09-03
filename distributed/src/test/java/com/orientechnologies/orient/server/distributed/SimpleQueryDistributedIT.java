package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleQueryDistributedIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;

  private OrientDB remote;
  private ODatabaseSession session;

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = TestSetupUtil.create(config);
    setup.setup();

    remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "test");
    session = remote.open("test", "admin", "admin");
  }

  @Test
  public void test() {
    OVertex vertex = session.newVertex("V");
    vertex.setProperty("name", "one");
    session.save(vertex);

    // Query with SQL
    OResultSet res = session.query("select from V");
    assertTrue(res.hasNext());
    assertEquals(res.next().getProperty("name"), "one");

    // Query with script
    res = session.execute("sql", "select from V");
    assertTrue(res.hasNext());
    assertEquals(res.next().getProperty("name"), "one");

    // Query order by
    OClass v2 = session.createVertexClass("V2");
    int records = (OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger() + 10);
    for (int i = 0; i < records; i++) {
      vertex = session.newVertex("V2");
      vertex.setProperty("name", "one");
      vertex.setProperty("pos", i);
      session.save(vertex);
    }

    res = session.query("select from V2 order by pos");
    for (int i = 0; i < records; i++) {
      assertTrue(res.hasNext());
      OResult ele = res.next();
      assertEquals((int) ele.getProperty("pos"), i);
      assertEquals(ele.getProperty("name"), "one");
    }
  }

  @After
  public void after() throws InterruptedException {
    System.out.println("Tearing down test setup.");
    try {
      if (remote != null) {
        remote.drop("test");
        remote.close();
      }
    } finally {
      setup.teardown();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
