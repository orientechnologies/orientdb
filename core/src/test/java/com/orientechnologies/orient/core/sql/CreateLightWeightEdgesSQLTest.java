package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CreateLightWeightEdgesSQLTest {
  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "embedded:",
            OCreateDatabaseUtil.TYPE_MEMORY);
  }

  @Test
  public void test() {
    ODatabaseSession session =
        orientDB.open(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");
    session.command("create vertex v set name='a' ");
    session.command("create vertex v set name='b' ");
    session.command(
        "create edge e from (select from v where name='a') to (select from v where name='a') ");
    try (OResultSet res = session.query("select expand(out()) from v where name='a' ")) {
      assertEquals(res.stream().count(), 1);
    }
    session.close();
  }

  @Test
  public void mtTest() throws InterruptedException {

    ODatabasePool pool =
        new ODatabasePool(
            orientDB,
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    ODatabaseSession session = pool.acquire();

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");
    session.command("create vertex v set id = 1 ");
    session.command("create vertex v set id = 2 ");

    session.close();

    CountDownLatch latch = new CountDownLatch(10);

    IntStream.range(0, 10)
        .forEach(
            (i) -> {
              new Thread(
                      () -> {
                        ODatabaseSession session1 = pool.acquire();

                        try {
                          for (int j = 0; j < 100; j++) {

                            try {
                              session1.command(
                                  "create edge e from (select from v where id=1) to (select from v where id=2) ");
                            } catch (OConcurrentModificationException e) {

                            }
                          }
                        } finally {
                          session1.close();
                          latch.countDown();
                        }
                      })
                  .start();
            });

    latch.await();

    session = pool.acquire();
    try (OResultSet res = session.query("select sum(out().size()) as size from V where id = 1");
        OResultSet res1 = session.query("select sum(in().size()) as size from V where id = 2")) {

      Integer s1 = res.stream().findFirst().get().getProperty("size");
      Integer s2 = res1.stream().findFirst().get().getProperty("size");
      assertEquals(s1, s2);

    } finally {
      session.close();
      pool.close();
    }
  }

  @After
  public void after() {
    orientDB.close();
  }
}
