package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class CreateLightWeightEdgesSQLTest {

  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(CreateLightWeightEdgesSQLTest.class.getSimpleName(), ODatabaseType.MEMORY);
  }

  @Test
  public void test() {
    ODatabaseSession session = orientDB.open(CreateLightWeightEdgesSQLTest.class.getSimpleName(), "admin", "admin");

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");
    session.command("create vertex v set name='a' ");
    session.command("create vertex v set name='b' ");
    session.command("create edge e from (select from v where name='a') to (select from v where name='a') ");
    try (OResultSet res = session.query("select expand(out()) from v where name='a' ")) {
      assertEquals(res.stream().count(), 1);
    }
  }

  @Test
  public void mtTest() throws InterruptedException {

    ODatabasePool pool = new ODatabasePool(orientDB, CreateLightWeightEdgesSQLTest.class.getSimpleName(), "admin", "admin");

    ODatabaseSession session = pool.acquire();

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");
    session.command("create vertex v set id = 1 ");
    session.command("create vertex v set id = 2 ");

    session.close();

    CountDownLatch latch = new CountDownLatch(10);

    IntStream.range(0, 10).forEach((i) -> {
      new Thread(() -> {

        ODatabaseSession session1 = pool.acquire();

        try {
          for (int j = 0; j < 100; j++) {


            try {
              session1.command("create edge e from (select from v where id=1) to (select from v where id=2) ");
            } catch (OConcurrentModificationException e) {

            }
          }
        } finally {
          latch.countDown();
        }

      }).start();
    });

    latch.await();

    session = pool.acquire();
    try (OResultSet res = session.query("select sum(out().size()) as size from V where id = 1");
        OResultSet res1 = session.query("select sum(in().size()) as size from V where id = 2")) {

      Integer s1 = res.stream().findFirst().get().getProperty("size");
      Integer s2 = res1.stream().findFirst().get().getProperty("size");
      assertEquals(s1, s2);

    } finally {
      pool.close();
    }

  }

  @After
  public void after() {
    orientDB.close();
  }

}
