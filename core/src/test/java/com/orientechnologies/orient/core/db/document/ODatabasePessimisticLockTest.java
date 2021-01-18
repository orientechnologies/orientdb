package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ODatabasePessimisticLockTest {

  private OrientDB orientDB;

  @Before
  public void before() {
    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING,
                OrientDBConfig.LOCK_TYPE_READWRITE)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    orientDB = new OrientDB("embedded:", config);
    orientDB.execute(
        "create database "
            + "test"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    final ODatabaseSession session =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    session.createClass("test");
    session.save(new ODocument("test")).getIdentity();
  }

  @Test
  public void testPessimistic() throws InterruptedException {
    final CountDownLatch pessimisticQueryDone = new CountDownLatch(1);
    final CountDownLatch finished = new CountDownLatch(1);
    final CountDownLatch firstCommitted = new CountDownLatch(1);
    final CountDownLatch checkPreSecondCommit = new CountDownLatch(1);
    new Thread(
            () -> {
              ODatabaseSession session =
                  orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
              session.begin();
              try (OResultSet set = session.query("select from test lock record")) {
                OElement element = set.next().getElement().get();
                element.setProperty("a", "a");
                pessimisticQueryDone.countDown();
                session.save(element);
                try {
                  // should timeout if locking is working
                  assertFalse(checkPreSecondCommit.await(100, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                  // should not happen
                }
              }
              session.commit();
              firstCommitted.countDown();
              session.close();
            })
        .start();

    new Thread(
            () -> {
              ODatabaseSession session =
                  orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
              session.begin();
              try {
                pessimisticQueryDone.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }

              try (OResultSet set = session.query("select from test")) {
                OElement element = set.next().getElement().get();
                element.setProperty("a", "b");
                session.save(element);
              }
              try {
                checkPreSecondCommit.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              session.commit();

              session.close();
              finished.countDown();
            })
        .start();

    firstCommitted.await();
    ODatabaseSession session =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    try (OResultSet set = session.query("select from test")) {
      assertEquals(set.next().getProperty("a"), "a");
    }
    checkPreSecondCommit.countDown();
    session.close();
    finished.await();
    ODatabaseSession session1 =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try (OResultSet set = session1.query("select from test")) {
      assertEquals(set.next().getProperty("a"), "b");
    }
    session1.close();
  }

  @After
  public void after() {
    orientDB.close();
  }
}
