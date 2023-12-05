package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleIndexFailDistributedIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;
  private OrientDB remote;
  private ODatabaseSession session;
  private OProperty prop;

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
    OClass clazz = session.createClass("Test");
    prop = clazz.createProperty("test", OType.STRING);
  }

  @Test(expected = ORecordDuplicatedException.class)
  public void test() {
    OElement doc = session.newElement("test");
    doc.setProperty("test", "value");
    doc = session.save(doc);
    OElement doc2 = session.newElement("test");
    doc2.setProperty("test", "value");
    doc2 = session.save(doc2);
    prop.createIndex(INDEX_TYPE.UNIQUE);
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
