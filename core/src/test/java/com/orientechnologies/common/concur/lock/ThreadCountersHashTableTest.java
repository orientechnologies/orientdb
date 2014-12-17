package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.MersenneTwister;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 8/26/14
 */
@Test
public class ThreadCountersHashTableTest {
  public void addSingleItem() {
    OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable(4, true);
    Thread thread = new RandomThread();

    threadCountersHashTable.insert(thread);
    Assert.assertSame(threadCountersHashTable.search(thread).getThread(), thread);
  }

  public void addTwoItems() {
    OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable(4, true);
    Thread threadOne = new RandomThread();
    Thread threadTwo = new RandomThread();

    threadCountersHashTable.insert(threadOne);
    threadCountersHashTable.insert(threadTwo);

    Assert.assertSame(threadCountersHashTable.search(threadOne).getThread(), threadOne);
    Assert.assertSame(threadCountersHashTable.search(threadTwo).getThread(), threadTwo);
  }

  public void addFourItems() {
    OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable(4, true);

    Thread threadOne = new RandomThread();
    Thread threadTwo = new RandomThread();
    Thread threadThree = new RandomThread();
    Thread threadFour = new RandomThread();

    threadCountersHashTable.insert(threadOne);
    threadCountersHashTable.insert(threadTwo);
    threadCountersHashTable.insert(threadThree);
    threadCountersHashTable.insert(threadFour);

    Assert.assertSame(threadCountersHashTable.search(threadOne).getThread(), threadOne);
    Assert.assertSame(threadCountersHashTable.search(threadTwo).getThread(), threadTwo);
    Assert.assertSame(threadCountersHashTable.search(threadThree).getThread(), threadThree);
    Assert.assertSame(threadCountersHashTable.search(threadFour).getThread(), threadFour);
  }

  public void add8Items() {
    OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable(4, true);

    Thread[] threads = new Thread[8];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new RandomThread();
      threadCountersHashTable.insert(threads[i]);
    }

    for (Thread thread : threads)
      Assert.assertSame(threadCountersHashTable.search(thread).getThread(), thread);
  }

  public void add4096Items() {
    OThreadCountersHashTable threadCountersHashTable = new OThreadCountersHashTable(4, true);

    Thread[] threads = new Thread[4096];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new RandomThread();
      threadCountersHashTable.insert(threads[i]);
    }

    for (Thread thread : threads)
      Assert.assertSame(threadCountersHashTable.search(thread).getThread(), thread);
  }

  private static final class RandomThread extends Thread {
    private static final Set<Long>       usedIds = new HashSet<Long>();

    private static final MersenneTwister rnd;
    static {
      long seed = OLongSerializer.INSTANCE.deserializeLiteral(SecureRandom.getSeed(8), 0);
      System.out.println("RandomThread seed : " + seed);
      rnd = new MersenneTwister(seed);
    }

    private final long                   tid;

    private static long nextId() {
      long nextId;
      do {
        nextId = rnd.nextLong();
      } while (!usedIds.add(nextId));

      return nextId;
    }

    private RandomThread() {
      tid = nextId();
    }

    @Override
    public long getId() {
      return tid;
    }
  }
}
