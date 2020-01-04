package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UniqueCompositeIndexDistributedIT {

  private OServer          server0;
  private OServer          server1;
  private OServer          server2;
  private OrientDB         remote;
  private ODatabaseSession session;
  private String           indexName;

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    session = remote.open("test", "admin", "admin");
    OClass clazz = session.createClass("Test");
    clazz.createProperty("test", OType.STRING);
    clazz.createProperty("testa", OType.STRING);
    OIndex idx = clazz.createIndex("cu", OClass.INDEX_TYPE.UNIQUE, "test", "testa");
    indexName = idx.getName();
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

  @After
  public void after() throws InterruptedException {
    session.activateOnCurrentThread();
    session.close();
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

}