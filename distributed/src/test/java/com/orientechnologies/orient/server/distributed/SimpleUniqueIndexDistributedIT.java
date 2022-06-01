package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleUniqueIndexDistributedIT {

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
    OClass clazz = session.createClass("Edg", "E");
    clazz.createProperty("out", OType.LINK);
    clazz.createProperty("in", OType.LINK);
    clazz.createIndex("out_in", "UNIQUE", "out", "in");
  }

  @Test
  public void test() {
    session.begin();
    OVertex doc = session.newVertex("V");
    doc.setProperty("test", "a value");
    doc = session.save(doc);

    OVertex doc1 = session.newVertex("V");
    doc1.setProperty("test", "a value1");
    doc1 = session.save(doc1);
    OEdge edge = doc1.addEdge(doc, "Edg");
    session.save(edge);
    session.commit();

    try (OResultSet res = session.query("select from Edg ")) {
      assertTrue(res.hasNext());
    }

    OrientDB remote1 = setup.createRemote(server1, OrientDBConfig.defaultConfig());
    ODatabaseSession session1 = remote1.open("test", "admin", "admin");
    try (OResultSet res1 = session1.query("select from Edg")) {
      assertTrue(res1.hasNext());
    }
    session1.close();
  }

  @After
  public void after() throws InterruptedException {
    try {
      session.activateOnCurrentThread();
      session.close();
      remote.drop("test");
      remote.close();
    } finally {
      setup.teardown();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
