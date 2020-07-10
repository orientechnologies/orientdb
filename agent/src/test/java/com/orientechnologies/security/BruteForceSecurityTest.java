package com.orientechnologies.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.server.OServer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BruteForceSecurityTest {

  private static final int BRUTE_FORCE_ATTEMPT = 1000;
  private OServer server;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    server.getContext().create(BruteForceSecurityTest.class.getSimpleName(), ODatabaseType.MEMORY);
  }

  @Test
  public void testBruteForce() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(3);

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    // no user
    new Thread(
            () -> {
              try {

                for (int i = 0; i < BRUTE_FORCE_ATTEMPT; i++) {
                  try (ODatabaseSession session =
                      context.open(BruteForceSecurityTest.class.getSimpleName(), "fake", "fake")) {
                    Assert.fail();
                  } catch (OSecurityAccessException e) {

                  }
                }
              } finally {
                latch.countDown();
              }
            })
        .start();

    // brute password
    new Thread(
            () -> {
              try {

                for (int i = 0; i < BRUTE_FORCE_ATTEMPT / 100; i++) {
                  try (ODatabaseSession session =
                      context.open(
                          BruteForceSecurityTest.class.getSimpleName(), "admin", "fake" + i)) {
                    Assert.fail();
                  } catch (OSecurityAccessException e) {
                  }
                }
              } finally {
                latch.countDown();
              }
            })
        .start();
    AtomicLong total = new AtomicLong(0);
    AtomicLong count = new AtomicLong(0);
    new Thread(
            () -> {
              try {
                for (int i = 0; i < BRUTE_FORCE_ATTEMPT; i++) {
                  long start = System.currentTimeMillis();
                  long end;
                  try (ODatabaseSession open =
                      context.open(
                          BruteForceSecurityTest.class.getSimpleName(), "admin", "admin")) {
                    open.query("select from OUser").close();
                    end = System.currentTimeMillis() - start;
                    count.incrementAndGet();
                    total.addAndGet(end);
                  }
                }
              } finally {
                latch.countDown();
              }
            })
        .start();

    latch.await();

    long mean = total.get() / count.get();
    Assert.assertTrue(mean < 300 && mean > 0);
  }

  @After
  public void deInit() {
    server.getContext().drop(BruteForceSecurityTest.class.getSimpleName());
    server.shutdown();
  }
}
