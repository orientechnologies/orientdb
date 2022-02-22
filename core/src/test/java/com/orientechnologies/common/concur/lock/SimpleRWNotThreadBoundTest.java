package com.orientechnologies.common.concur.lock;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SimpleRWNotThreadBoundTest {

  @Test
  public void testWriteWaitRead() throws InterruptedException {

    OSimpleRWLockManager<String> manager = new ONotThreadRWLockManager<>();
    manager.acquireReadLock("aaa", 0);

    CountDownLatch error = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireWriteLock("aaa", 10);
              } catch (OLockException e) {
                error.countDown();
              }
            })
        .start();

    assertTrue(error.await(40, TimeUnit.MILLISECONDS));
    manager.releaseReadLock("aaa");
  }

  @Test
  public void testReadWaitWrite() throws InterruptedException {

    OSimpleRWLockManager<String> manager = new ONotThreadRWLockManager<>();
    manager.acquireWriteLock("aaa", 0);

    CountDownLatch error = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireReadLock("aaa", 10);
              } catch (OLockException e) {
                error.countDown();
              }
            })
        .start();

    assertTrue(error.await(40, TimeUnit.MILLISECONDS));

    manager.releaseWriteLock("aaa");
  }

  @Test
  public void testReadReleaseAcquireWrite() throws InterruptedException {

    OSimpleRWLockManager<String> manager = new ONotThreadRWLockManager<>();
    manager.acquireReadLock("aaa", 0);

    CountDownLatch error = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireWriteLock("aaa", 10);
              } catch (OLockException e) {
                error.countDown();
              }
            })
        .start();

    assertTrue(error.await(40, TimeUnit.MILLISECONDS));

    CountDownLatch ok = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireWriteLock("aaa", 10);
                ok.countDown();
              } catch (OLockException e) {
              }
            })
        .start();

    manager.releaseReadLock("aaa");

    assertTrue(ok.await(40, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReadReadWaitWrite() throws InterruptedException {

    OSimpleRWLockManager<String> manager = new ONotThreadRWLockManager<>();
    manager.acquireReadLock("aaa", 0);

    CountDownLatch ok = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireReadLock("aaa", 10);
                ok.countDown();
              } catch (OLockException e) {
              }
            })
        .start();

    CountDownLatch error = new CountDownLatch(1);
    new Thread(
            () -> {
              try {
                manager.acquireWriteLock("aaa", 10);
              } catch (OLockException e) {
                error.countDown();
              }
            })
        .start();

    assertTrue(ok.await(40, TimeUnit.MILLISECONDS));
    assertTrue(error.await(40, TimeUnit.MILLISECONDS));
    manager.releaseReadLock("aaa");
  }
}
