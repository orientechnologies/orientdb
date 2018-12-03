package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleIndexDistributedIT {

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
    OIndex<?> idx = clazz.createProperty("test", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    indexName = idx.getName();
  }

  @Test
  public void test() {
    OElement doc = session.newElement("test");
    doc.setProperty("test", "a value");
    doc = session.save(doc);
    doc.setProperty("test", "some");
    session.save(doc);
    try (OResultSet res = session.query("select from index:" + indexName + " where key =\"some\"")) {
      assertTrue(res.hasNext());
    }

    OrientDB remote1 = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig());
    ODatabaseSession session1 = remote1.open("test", "admin", "admin");
    try (OResultSet res1 = session1.query("select from index:" + indexName + " where key =\"some\"")) {
      assertTrue(res1.hasNext());
    }
    session1.close();
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