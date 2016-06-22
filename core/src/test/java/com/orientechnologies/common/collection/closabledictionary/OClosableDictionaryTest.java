package com.orientechnologies.common.collection.closabledictionary;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Test
public class OClosableDictionaryTest {
  public void testSingleItemAddRemove() {
    final OClosableItem closableItem = new CItem(0);
    final OClosableDictionary<Long, OClosableItem> dictionary = new OClosableDictionary<Long, OClosableItem>(10);

    dictionary.add(1L, closableItem);

    OClosableHolder<OClosableItem> holder = dictionary.acquire(0L);
    Assert.assertNull(holder);

    holder = dictionary.acquire(1L);
    Assert.assertNotNull(holder);
    dictionary.release(holder);

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
  }

  public void testCloseHalfOfTheItems() {
    final OClosableDictionary<Long, OClosableItem> dictionary = new OClosableDictionary<Long, OClosableItem>(10);

    for (int i = 0; i < 10; i++) {
      final OClosableItem closableItem = new CItem(i);
      dictionary.add((long) i, closableItem);
    }

    OClosableHolder<OClosableItem> holder = dictionary.acquire(10L);
    Assert.assertNull(holder);

    for (int i = 0; i < 5; i++) {
      holder = dictionary.acquire((long) i);
      dictionary.release(holder);
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

  public void testAccessLastClosedItemsAndOpenThem() {
    final OClosableDictionary<Long, CItem> dictionary = new OClosableDictionary<Long, CItem>(10);

    for (int i = 0; i < 10; i++) {
      final CItem closableItem = new CItem(i);
      dictionary.add((long) i, closableItem);
    }

    for (int i = 0; i < 5; i++) {
      OClosableHolder<CItem> holder = dictionary.acquire((long) i);
      dictionary.release(holder);
    }

    dictionary.emptyBuffers();

    for (int i = 0; i < 5; i++) {
      dictionary.add(10L + i, new CItem(10 + i));
    }

    for (int i = 5; i < 10; i++) {
      Assert.assertTrue(!dictionary.get((long) i).isOpen());
    }

    for (int i = 5; i < 10; i++) {
      OClosableHolder<CItem> holder = dictionary.acquire((long) i);
      dictionary.release(holder);
    }

    dictionary.emptyBuffers();

    for (int i = 5; i < 10; i++) {
      Assert.assertTrue(!dictionary.get((long) i).isOpen());
    }

    for (int i = 5; i < 10; i++) {
      OClosableHolder<CItem> holder = dictionary.acquire((long) i);
      holder.get().open();
      dictionary.release(holder);
    }

    dictionary.emptyBuffers();

    for (int i = 5; i < 10; i++) {
      Assert.assertTrue(dictionary.get((long) i).isOpen());
    }

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
  }

  public void testMultipleThreadsConsistency() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<Void>> futures = new ArrayList<Future<Void>>();
    CountDownLatch latch = new CountDownLatch(1);

    int limit = 60000;

    OClosableDictionary<Long, CItem> dictionary = new OClosableDictionary<Long, CItem>(500);
    futures.add(executor.submit(new Adder(dictionary, latch, limit / 3)));
    // futures.add(executor.submit(new Adder(dictionary, latch, limit / 3)));

    AtomicBoolean stop = new AtomicBoolean();

    for (int i = 0; i < 1; i++) {
      futures.add(executor.submit(new Acquier(dictionary, latch, limit, stop)));
    }

    latch.countDown();

    Thread.sleep(10000);

    // futures.add(executor.submit(new Adder(dictionary, latch, limit / 3)));

    stop.set(true);
    for (Future<Void> future : futures) {
      future.get();
    }

    dictionary.emptyBuffers();

    Assert.assertTrue(dictionary.checkAllLRUListItemsInMap());
    Assert.assertTrue(dictionary.checkAllOpenItemsInLRUList());
  }

  private class Adder implements Callable<Void> {
    private final OClosableDictionary<Long, CItem> dictionary;
    private final CountDownLatch                   latch;
    private final int                              limit;

    public Adder(OClosableDictionary<Long, CItem> dictionary, CountDownLatch latch, int limit) {
      this.dictionary = dictionary;
      this.latch = latch;
      this.limit = limit;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      try {
        for (int i = 0; i < limit; i++) {
          dictionary.add((long) i, new CItem(i));
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private class Acquier implements Callable<Void> {
    private final OClosableDictionary<Long, CItem> dictionary;
    private final CountDownLatch                   latch;
    private final int                              limit;
    private final AtomicBoolean                    stop;

    public Acquier(OClosableDictionary<Long, CItem> dictionary, CountDownLatch latch, int limit, AtomicBoolean stop) {
      this.dictionary = dictionary;
      this.latch = latch;
      this.limit = limit;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      try {
        Random random = new Random();

        while (!stop.get()) {
          int index = random.nextInt(limit);

          final OClosableHolder<CItem> holder = dictionary.acquire((long) index);
          if (holder != null) {
            holder.get().open();
            dictionary.release(holder);
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private class CItem implements OClosableItem {
    private volatile boolean open = true;
    private final int index;

    public CItem(int index) {
      this.index = index;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }

    public void open() {
      open = true;
    }
  }
}
