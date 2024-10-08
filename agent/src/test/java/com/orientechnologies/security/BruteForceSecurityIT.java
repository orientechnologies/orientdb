package com.orientechnologies.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
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

public class BruteForceSecurityIT {

  private static final int BRUTE_FORCE_ATTEMPT = 100;
  private OServer server;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    server
        .getContext()
        .execute(
            "create database `"
                + BruteForceSecurityIT.class.getSimpleName()
                + "` memory users(admin identified by 'adminpwd' role admin)");
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
                      context.open(BruteForceSecurityIT.class.getSimpleName(), "fake", "fake")) {
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
                          BruteForceSecurityIT.class.getSimpleName(), "admin", "fake" + i)) {
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
                  long start = System.nanoTime();
                  long end;
                  try (ODatabaseSession open =
                      context.open(
                          BruteForceSecurityIT.class.getSimpleName(), "admin", "adminpwd")) {
                    open.query("select from OUser").close();
                    end = System.nanoTime() - start;
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
    Assert.assertTrue(
        String.format("failing 300000000 < %d > 0", mean), mean < 300000000 && mean > 0);
    context.close();
  }

  @After
  public void deInit() {
    server.getContext().drop(BruteForceSecurityIT.class.getSimpleName());
    server.shutdown();
  }
}
