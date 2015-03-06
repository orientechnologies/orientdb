package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.test.ConcurrentTestHelper;
import com.orientechnologies.orient.test.TestFactory;

/**
 * Concurrent test for {@link ConcurrentLRUList}.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ConcurrentLRUListConcurrentTest {
  private static final int AMOUNT_OF_OPERATIONS = 100000;
  private static final int THREAD_COUNT         = 8;

  private LRUList          list                 = new ConcurrentLRUList();
  private volatile long    c                    = 47;

  @BeforeMethod
  public void setUp() throws Exception {
    list = new ConcurrentLRUList();
  }

  @Test
  public void testConcurrentAdd() throws Exception {
    ConcurrentTestHelper.test(THREAD_COUNT, new AdderFactory());

    int expectedSize = AMOUNT_OF_OPERATIONS * THREAD_COUNT;
    assertListConsistency(expectedSize);
  }

  @Test
  public void testConcurrentAddAndRemove() throws Exception {
    Collection<Integer> res = ConcurrentTestHelper.<Integer> build().add(THREAD_COUNT, new AdderFactory())
        .add(THREAD_COUNT, new RemoveLRUFactory()).go();

    int expectedSize = 0;
    for (Integer r : res) {
      expectedSize += r;
    }

    assertListConsistency(expectedSize);
  }

  @Test
  public void testAddRemoveSameEntries() throws Exception {
    ConcurrentTestHelper.<Integer> build().add(THREAD_COUNT, new AddSameFactory()).add(THREAD_COUNT, new RemoveLRUFactory()).go();

    assertListConsistency();
  }

  @Test
  public void testAllOperationsRandomEntries() throws Exception {
    ConcurrentTestHelper.<Integer> build().add(THREAD_COUNT, new RandomAdderFactory()).add(THREAD_COUNT, new RandomRemoveFactory())
        .add(THREAD_COUNT, new RemoveLRUFactory()).go();

    assertListConsistency();
  }

  private void assertListConsistency(int expectedSize) {
    Assert.assertEquals(list.size(), expectedSize);
    int count = 0;
    List<OCacheEntry> items = new ArrayList<OCacheEntry>();
    for (OCacheEntry entry : list) {
      items.add(entry);
      count++;
    }
    Assert.assertEquals(count, expectedSize);

    Collections.reverse(items);
    for (OCacheEntry item : items) {
      OCacheEntry actual = list.removeLRU();
      Assert.assertEquals(actual, item);
    }
    Assert.assertNull(list.removeLRU());

  }

  private void assertListConsistency() {
    int expectedSize = list.size();
    int count = 0;
    List<OCacheEntry> items = new ArrayList<OCacheEntry>();
    for (OCacheEntry entry : list) {
      items.add(entry);
      count++;
    }
    Assert.assertEquals(count, expectedSize);

    Collections.reverse(items);
    for (OCacheEntry item : items) {
      OCacheEntry actual = list.removeLRU();
      Assert.assertEquals(actual, item);
    }

    Assert.assertNull(list.removeLRU());
  }

  private void consumeCPU(int cycles) {
    long c1 = c;
    for (int i = 0; i < cycles; i++) {
      c1 += c1 * 31 + i * 51;
    }
    c = c1;
  }

  private class AdderFactory implements TestFactory<Integer> {
    private int j = 0;

    @Override
    public Callable<Integer> createWorker() {
      return new Callable<Integer>() {
        private int threadNumber = ++j;

        @Override
        public Integer call() throws Exception {
          for (int i = 0; i < AMOUNT_OF_OPERATIONS; i++) {
            list.putToMRU(new OCacheEntry(threadNumber, i, null, false));
          }
          return AMOUNT_OF_OPERATIONS;
        }
      };
    }
  }

  private class RemoveLRUFactory implements TestFactory<Integer> {
    @Override
    public Callable<Integer> createWorker() {
      return new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          int actualRemoves = 0;
          consumeCPU(1000);
          for (int i = 0; i < AMOUNT_OF_OPERATIONS; i++) {
            OCacheEntry e = list.removeLRU();
            if (e != null) {
              actualRemoves++;
            }
            consumeCPU(1000);
          }
          return -actualRemoves;
        }
      };
    }
  }

  private class RandomAdderFactory implements TestFactory<Integer> {

    @Override
    public Callable<Integer> createWorker() {
      return new Callable<Integer>() {

        @Override
        public Integer call() throws Exception {
          Random r = new Random();
          for (int i = 0; i < AMOUNT_OF_OPERATIONS; i++) {
            list.putToMRU(new OCacheEntry(0, r.nextInt(200), null, false));
            consumeCPU(r.nextInt(500) + 1000);
          }
          return AMOUNT_OF_OPERATIONS;
        }
      };
    }
  }

  private class AddSameFactory implements TestFactory<Integer> {

    @Override
    public Callable<Integer> createWorker() {
      return new Callable<Integer>() {

        @Override
        public Integer call() throws Exception {
          Random r = new Random();
          for (int i = 0; i < AMOUNT_OF_OPERATIONS; i++) {
            list.putToMRU(new OCacheEntry(0, 0, null, false));
            consumeCPU(r.nextInt(500) + 1000);
          }
          return AMOUNT_OF_OPERATIONS;
        }
      };
    }
  }

  private class RandomRemoveFactory implements TestFactory<Integer> {
    @Override
    public Callable<Integer> createWorker() {
      return new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          Random r = new Random();
          int actualRemoves = 0;
          for (int i = 0; i < AMOUNT_OF_OPERATIONS; i++) {
            OCacheEntry e = list.remove(0, r.nextInt(100));
            if (e != null) {
              actualRemoves++;
            }
            consumeCPU(r.nextInt(1000) + 1000);
          }
          return -actualRemoves;
        }
      };
    }
  }
}
