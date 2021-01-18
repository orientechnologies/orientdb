package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OSessionRetryTest {

  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            OSessionRetryTest.class.getSimpleName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
  }

  @Test
  public void testRetry() {
    ODatabaseSession session =
        orientDB.open(
            OSessionRetryTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    session.createClass("Test");
    OElement doc = session.newElement("Test");
    doc.setProperty("one", "tas");
    ORID id = session.save(doc).getIdentity();

    CountDownLatch wrote = new CountDownLatch(1);
    CountDownLatch read = new CountDownLatch(1);

    Executors.newCachedThreadPool()
        .execute(
            () -> {
              ODatabaseSession session1 =
                  orientDB.open(
                      OSessionRetryTest.class.getSimpleName(),
                      "admin",
                      OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
              OElement loaded = session1.load(id);
              try {
                read.await();
              } catch (InterruptedException e) {
                //
              }
              loaded.setProperty("one", "two");
              session1.save(loaded);
              wrote.countDown();
              session1.close();
            });

    OModifiableInteger integer = new OModifiableInteger(0);
    session.executeWithRetry(
        2,
        (session1) -> {
          integer.increment();
          session1.begin();
          OElement loaded = session1.load(id);
          read.countDown();

          try {
            wrote.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          loaded.setProperty("two", "three");
          session1.save(loaded);
          session1.commit();
          return null;
        });

    assertEquals(integer.getValue(), 2);

    session.close();
  }

  @After
  public void after() {
    orientDB.close();
  }
}
