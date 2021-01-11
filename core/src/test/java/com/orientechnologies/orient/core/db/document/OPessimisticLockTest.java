package com.orientechnologies.orient.core.db.document;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING;
import static com.orientechnologies.orient.core.db.OrientDBConfig.LOCK_TYPE_READWRITE;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OPessimisticLockTest {

  private static final String SERVER_DIRECTORY = "./target/lock";
  private OrientDB orientDB;
  private ODatabaseDocument session;

  @Before
  public void before() throws Exception {

    orientDB =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(STORAGE_PESSIMISTIC_LOCKING, LOCK_TYPE_READWRITE)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OCreateDatabaseUtil.createDatabase(
        OPessimisticLockTest.class.getSimpleName(), orientDB, OCreateDatabaseUtil.TYPE_MEMORY);
    session =
        orientDB.open(
            OPessimisticLockTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    orientDB.drop(OPessimisticLockTest.class.getSimpleName());
    orientDB.close();
  }
}
