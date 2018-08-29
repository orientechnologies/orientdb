package com.orientechnologies.orient.server.lock;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

public class OPessimisticLockTest {

  private static final String            SERVER_DIRECTORY = "./target/lock";
  private              OServer           server;
  private              OrientDB          orientDB;
  private              ODatabaseDocument session;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create(OPessimisticLockTest.class.getSimpleName(), ODatabaseType.MEMORY);
    session = orientDB.open(OPessimisticLockTest.class.getSimpleName(), "admin", "admin");
    session.createVertexClass("ToLock");
  }

  @Test
  @Ignore
  public void lockHappyPathNoCrashNoTx() {
    ORecord rid = session.save(new ODocument("ToLock"));
    session.getTransaction().lockRecord(rid, OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK);
    ODocument record = session.load(rid);
    record.setProperty("one", "value");
    session.save(record);
    session.getTransaction().unlockRecord(rid);
  }

  @Test
  public void lockHappyPathNoCrashTx() {
    ORecord rid = session.save(new ODocument("ToLock"));
    session.begin();
    session.getTransaction().lockRecord(rid, OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK);
    ODocument record = session.load(rid);
    record.setProperty("one", "value");
    session.save(record);
    session.commit();

  }

  @After
  public void after() {
    session.close();
    orientDB.drop(OPessimisticLockTest.class.getSimpleName());
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
