package com.orientechnologies.common.collection.closabledictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OClosableLinkedContainerTest {
  @Test
  public void testSingleItemAddRemove() throws Exception {
    final OClosableItem closableItem = new CItem(10);
    final OClosableLinkedContainer<Long, OClosableItem> dictionary =
        new OClosableLinkedContainer<Long, OClosableItem>(10);

    dictionary.add(1L, closableItem);

    OClosableEntry<Long, OClosableItem> entry = dictionary.acquire(0L);
    Assert.assertNull(entry);

    entry = dictionary.acquire(1L);
    Assert.assertNotNull(entry);
    dictionary.release(entry);

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
  }

  @Test
  public void testCloseHalfOfTheItems() throws Exception {
    final OClosableLinkedContainer<Long, OClosableItem> dictionary =
        new OClosableLinkedContainer<Long, OClosableItem>(10);

    for (int i = 0; i < 10; i++) {
      final OClosableItem closableItem = new CItem(i);
      dictionary.add((long) i, closableItem);
    }

    OClosableEntry<Long, OClosableItem> entry = dictionary.acquire(10L);
    Assert.assertNull(entry);

    for (int i = 0; i < 5; i++) {
      entry = dictionary.acquire((long) i);
      dictionary.release(entry);
    }

    dictionary.emptyBuffers();

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());

    for (int i = 0; i < 5; i++) {
      dictionary.add(10L + i, new CItem(10 + i));
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(dictionary.get((long) i).isOpen());
    }

    for (int i = 5; i < 10; i++) {
      Assert.assertTrue(!dictionary.get((long) i).isOpen());
    }

    for (int i = 10; i < 15; i++) {
      Assert.assertTrue(dictionary.get((long) i).isOpen());
    }

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
  }

  @Test
  @Ignore
  public void testMultipleThreadsConsistency() throws Exception {
    CItem.openFiles.set(0);
    CItem.maxDeltaLimit.set(0);

    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<Void>> futures = new ArrayList<Future<Void>>();
    CountDownLatch latch = new CountDownLatch(1);

    int limit = 60000;

    OClosableLinkedContainer<Long, CItem> dictionary =
        new OClosableLinkedContainer<Long, CItem>(16);
    futures.add(executor.submit(new Adder(dictionary, latch, 0, limit / 3)));
    futures.add(executor.submit(new Adder(dictionary, latch, limit / 3, 2 * limit / 3)));

    AtomicBoolean stop = new AtomicBoolean();

    for (int i = 0; i < 16; i++) {
      futures.add(executor.submit(new Acquier(dictionary, latch, limit, stop)));
    }

    latch.countDown();

    Thread.sleep(60000);

    futures.add(executor.submit(new Adder(dictionary, latch, 2 * limit / 3, limit)));

    Thread.sleep(15 * 60000);

    stop.set(true);
    for (Future<Void> future : futures) {
      future.get();
    }

    dictionary.emptyBuffers();

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
    Assert.assertTrue(dictionary.checkNoClosedItemsInLRUList());
    Assert.assertTrue(dictionary.checkLRUSize());
    Assert.assertTrue(dictionary.checkLRUSizeEqualsToCapacity());

    System.out.println("Open files " + CItem.openFiles.get());
    System.out.println("Max open files limit overhead " + CItem.maxDeltaLimit.get());
  }

  private class Adder implements Callable<Void> {
    private final OClosableLinkedContainer<Long, CItem> dictionary;
    private final CountDownLatch latch;
    private final int from;
    private final int to;

    public Adder(
        OClosableLinkedContainer<Long, CItem> dictionary, CountDownLatch latch, int from, int to) {
      this.dictionary = dictionary;
      this.latch = latch;
      this.from = from;
      this.to = to;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      try {
        for (int i = from; i < to; i++) {
          dictionary.add((long) i, new CItem(i));
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      System.out.println("Add from " + from + " to " + to + " completed");

      return null;
    }
  }

  private class Acquier implements Callable<Void> {
    private final OClosableLinkedContainer<Long, CItem> dictionary;
    private final CountDownLatch latch;
    private final int limit;
    private final AtomicBoolean stop;

    public Acquier(
        OClosableLinkedContainer<Long, CItem> dictionary,
        CountDownLatch latch,
        int limit,
        AtomicBoolean stop) {
      this.dictionary = dictionary;
      this.latch = latch;
      this.limit = limit;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      long counter = 0;
      long start = System.nanoTime();

      try {
        Random random = new Random();

        while (!stop.get()) {
          int index = random.nextInt(limit);
          final OClosableEntry<Long, CItem> entry = dictionary.acquire((long) index);
          if (entry != null) {
            Assert.assertTrue(entry.get().isOpen());
            counter++;
            dictionary.release(entry);
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      long end = System.nanoTime();

      System.out.println(
          "Files processed " + counter + " nanos per item " + (end - start) / counter);
      return null;
    }
  }

  private static class CItem implements OClosableItem {
    public static AtomicInteger openFiles = new AtomicInteger();
    public static AtomicInteger maxDeltaLimit = new AtomicInteger();

    private volatile boolean open = true;

    private final int openLimit;

    public CItem(int openLimit) {
      this.openLimit = openLimit;

      countOpenFiles();
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;

      int count = openFiles.decrementAndGet();

      if (count - openLimit > 0) {
        while (true) {
          int max = maxDeltaLimit.get();
          if (count - openLimit > max) {
            if (maxDeltaLimit.compareAndSet(max, count - openLimit)) break;
          } else {
            break;
          }
        }
      }
    }

    public void open() {
      open = true;

      countOpenFiles();
    }

    private void countOpenFiles() {
      int count = openFiles.incrementAndGet();
      if (count - openLimit > 0) {
        while (true) {
          int max = maxDeltaLimit.get();
          if (count - openLimit > max) {
            if (maxDeltaLimit.compareAndSet(max, count - openLimit)) break;
          } else {
            break;
          }
        }
      }
    }
  }
}
