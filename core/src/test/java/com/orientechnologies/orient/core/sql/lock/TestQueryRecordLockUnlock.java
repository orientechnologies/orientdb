package com.orientechnologies.orient.core.sql.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestQueryRecordLockUnlock {

  @Before
  public void before() {
    OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING.setValue(OrientDBConfig.LOCK_TYPE_READWRITE);
  }

  @Test
  public void testLockReleaseAfterIncrement() throws InterruptedException {
    final ORID id;
    ODatabaseDocumentTx db = null;
    try {

      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.create();
      ODocument doc = new ODocument();
      doc.field("count", 0);
      doc = db.save(doc, db.getClusterNameById(db.getDefaultClusterId()));
      id = doc.getIdentity();
      db.commit();
    } finally {
      if (db != null) db.close();
    }
    int thread = 10;

    ExecutorService pool = Executors.newFixedThreadPool(thread);
    for (int i = 0; i < 10; i++) {
      pool.submit(
          new Runnable() {

            @Override
            public void run() {
              ODatabaseDocumentTx db = null;
              try {
                db =
                    new ODatabaseDocumentTx(
                        "memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
                db.open("admin", "admin");
                for (int j = 0; j < 10; j++) {
                  db.getLocalCache().deleteRecord(id);
                  String asql =
                      "update "
                          + id.toString()
                          + " INCREMENT count = 1 where count < 50 lock record";
                  db.command(new OCommandSQL(asql)).execute(id);
                }
              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                if (db != null) {
                  db.close();
                }
              }
            }
          });
    }
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.HOURS);
    try {
      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.open("admin", "admin");
      ODocument doc = db.load(id);
      //      assertEquals(50, doc.field("count"));

      assertThat(doc.<Integer>field("count")).isEqualTo(50);

    } finally {
      if (db != null) {
        db.drop();
      }
    }
  }

  @Test
  @Ignore
  public void testLockWithSubqueryRecord() throws InterruptedException {
    final ORID id;
    ODatabaseDocumentTx db = null;
    try {

      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.create();
      ODocument doc = new ODocument();
      doc.field("count", 0);
      doc = db.save(doc, db.getClusterNameById(db.getDefaultClusterId()));
      id = doc.getIdentity();
      db.commit();
    } finally {
      if (db != null) db.close();
    }
    int thread = 10;

    ExecutorService pool = Executors.newFixedThreadPool(thread);
    for (int i = 0; i < 10; i++) {
      pool.submit(
          new Runnable() {

            @Override
            public void run() {
              ODatabaseDocumentTx db = null;
              try {
                db =
                    new ODatabaseDocumentTx(
                        "memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
                db.open("admin", "admin");
                for (int j = 0; j < 10; j++) {
                  String asql =
                      "update (select from "
                          + id.toString()
                          + ") INCREMENT count = 1 where count < 50 lock record";
                  db.command(new OCommandSQL(asql)).execute(id);
                }
              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                if (db != null) {
                  db.close();
                }
              }
            }
          });
    }
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.HOURS);
    try {
      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.open("admin", "admin");
      ODocument doc = db.load(id);
      //      assertEquals(50, doc.field("count"));

      assertThat(doc.<Integer>field("count")).isEqualTo(50);

    } finally {
      if (db != null) {
        db.drop();
      }
    }
  }

  @Test
  public void testLockReleaseAfterIncrementOpenClose() throws InterruptedException {
    final ORID id;
    ODatabaseDocumentTx db = null;
    try {

      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.create();
      ODocument doc = new ODocument();
      doc.field("count", 0);
      doc = db.save(doc, db.getClusterNameById(db.getDefaultClusterId()));
      id = doc.getIdentity();
      db.commit();
    } finally {
      if (db != null) db.close();
    }
    int thread = 10;

    ExecutorService pool = Executors.newFixedThreadPool(thread);
    for (int i = 0; i < 10; i++) {
      pool.submit(
          new Runnable() {

            @Override
            public void run() {
              ODatabaseDocumentTx db = null;
              for (int j = 0; j < 10; j++) {
                try {
                  db =
                      new ODatabaseDocumentTx(
                          "memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
                  db.open("admin", "admin");
                  String asql =
                      "update "
                          + id.toString()
                          + " INCREMENT count = 1 where count < 50 lock record";
                  db.command(new OCommandSQL(asql)).execute(id);
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  if (db != null) {
                    db.close();
                  }
                }
              }
            }
          });
    }
    pool.shutdown();
    pool.awaitTermination(1, TimeUnit.HOURS);
    try {
      db = new ODatabaseDocumentTx("memory:" + TestQueryRecordLockUnlock.class.getSimpleName());
      db.open("admin", "admin");
      ODocument doc = db.load(id);
      //      assertEquals(50, doc.field("count"));

      assertThat(doc.<Integer>field("count")).isEqualTo(50);

    } finally {
      if (db != null) {
        db.drop();
      }
    }
  }

  @After
  public void after() {
    OGlobalConfiguration.STORAGE_PESSIMISTIC_LOCKING.setValue("none");
  }
}
