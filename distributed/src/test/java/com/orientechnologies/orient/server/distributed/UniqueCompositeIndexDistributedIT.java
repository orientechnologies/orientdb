package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UniqueCompositeIndexDistributedIT {

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
    remote.execute("create database test plocal users(admin identified by 'admin' role admin)");
    session = remote.open("test", "admin", "admin");
    OClass clazz = session.createClass("Test");
    clazz.createProperty("test", OType.STRING);
    clazz.createProperty("testa", OType.STRING);
    OIndex idx = clazz.createIndex("cu", OClass.INDEX_TYPE.UNIQUE, "test", "testa");
  }

  @Test
  public void test() {
    OElement doc = session.newElement("test");
    doc.setProperty("test", "1");
    doc.setProperty("testa", "2");
    doc = session.save(doc);
    session.begin();
    session.delete(doc.getIdentity());
    OElement doc1 = session.newElement("test");
    doc1.setProperty("test", "1");
    doc1.setProperty("testa", "2");
    doc1 = session.save(doc1);
    session.commit();

    try (OResultSet res = session.query("select from test")) {
      assertTrue(res.hasNext());
      assertEquals(res.next().getIdentity().get(), doc1.getIdentity());
    }
  }

  @Test
  public void testUniqueInQuorum() throws InterruptedException {
    OElement doc = session.newElement("test");
    doc.setProperty("test", "1");
    doc.setProperty("testa", "2");
    doc = session.save(doc);
    session.begin();
    session.delete(doc.getIdentity());

    setup.shutdownServer(SimpleDServerConfig.SERVER2);

    Thread.sleep(1000);
    OElement doc1 = session.newElement("test");
    doc1.setProperty("test", "1");
    doc1.setProperty("testa", "2");
    doc1 = session.save(doc1);
    session.commit();

    try (OResultSet res = session.query("select from test")) {
      assertTrue(res.hasNext());
      assertEquals(res.next().getIdentity().get(), doc1.getIdentity());
    }

    setup.startServer(SimpleDServerConfig.SERVER2);
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
