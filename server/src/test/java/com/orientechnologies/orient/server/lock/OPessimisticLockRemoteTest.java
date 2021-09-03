package com.orientechnologies.orient.server.lock;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING;
import static com.orientechnologies.orient.core.db.OrientDBConfig.LOCK_TYPE_READWRITE;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OPessimisticLockRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/lock";
  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument session;

  @Before
  public void before() throws Exception {
    SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    STORAGE_PESSIMISTIC_LOCKING.setValue(LOCK_TYPE_READWRITE);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getClassLoader().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OPessimisticLockRemoteTest.class.getSimpleName());
    session = orientDB.open(OPessimisticLockRemoteTest.class.getSimpleName(), "admin", "admin");
    session.createVertexClass("ToLock");
  }

  @Test
  public void lockHappyPathNoCrashNoTx() {
    ORecord rid = session.save(new ODocument("ToLock"));
    OElement record = session.lock(rid.getIdentity());
    record.setProperty("one", "value");
    session.save(record);
    session.unlock(record.getIdentity());
  }

  @Test
  public void lockHappyPathNoCrashTx() {
    ORecord rid = session.save(new ODocument("ToLock"));
    session.begin();
    ODocument record = session.lock(rid.getIdentity());
    record.setProperty("one", "value");
    session.save(record);
    session.commit();
  }

  @After
  public void after() {
    session.close();
    orientDB.drop(OPessimisticLockRemoteTest.class.getSimpleName());
    orientDB.close();
    server.shutdown();

    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
