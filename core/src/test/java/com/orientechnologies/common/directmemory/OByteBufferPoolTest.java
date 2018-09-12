package com.orientechnologies.common.directmemory;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class OByteBufferPoolTest {
  @BeforeClass
  public static void beforeClass() {
    OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.setValue(true);
  }

  @AfterClass
  public static void afterClass() {
    OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.setValue(false);
  }

  @Test
  public void testByteBufferAllocationZeroPool() {
    final ODirectMemoryAllocator allocator = new ODirectMemoryAllocator();
    final OByteBufferPool byteBufferPool = new OByteBufferPool(42, allocator, 0);

    final ByteBuffer bufferOne = byteBufferPool.acquireDirect(false);
    Assert.assertEquals(42, bufferOne.capacity());
    Assert.assertEquals(42, allocator.getMemoryConsumption());

    Assert.assertEquals(0, byteBufferPool.getPoolSize());

    final ByteBuffer bufferTwo = byteBufferPool.acquireDirect(true);
    Assert.assertEquals(42, bufferTwo.capacity());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    assertBufferIsClear(bufferTwo);

    byteBufferPool.release(bufferOne);
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(42, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferTwo);
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(0, allocator.getMemoryConsumption());

    byteBufferPool.clear();
    byteBufferPool.checkMemoryLeaks();
  }

  @Test
  public void testByteBufferAllocationTwoPagesPool() {
    final ODirectMemoryAllocator allocator = new ODirectMemoryAllocator();
    final OByteBufferPool byteBufferPool = new OByteBufferPool(42, allocator, 2);

    ByteBuffer bufferOne = byteBufferPool.acquireDirect(false);

    Assert.assertEquals(42, bufferOne.capacity());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(42, allocator.getMemoryConsumption());

    ByteBuffer bufferTwo = byteBufferPool.acquireDirect(true);
    Assert.assertEquals(42, bufferTwo.capacity());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    assertBufferIsClear(bufferTwo);

    ByteBuffer bufferThree = byteBufferPool.acquireDirect(false);

    Assert.assertEquals(42, bufferThree.capacity());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferOne);

    Assert.assertEquals(1, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferTwo);

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferThree);

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    bufferOne = byteBufferPool.acquireDirect(true);

    Assert.assertEquals(42, bufferOne.capacity());
    Assert.assertEquals(0, bufferOne.position());
    Assert.assertEquals(1, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    assertBufferIsClear(bufferOne);

    bufferTwo = byteBufferPool.acquireDirect(true);

    Assert.assertEquals(42, bufferTwo.capacity());
    Assert.assertEquals(0, bufferTwo.position());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    assertBufferIsClear(bufferTwo);

    bufferThree = byteBufferPool.acquireDirect(false);

    Assert.assertEquals(42, bufferThree.capacity());
    Assert.assertEquals(0, bufferThree.position());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferThree);

    Assert.assertEquals(1, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    bufferThree = byteBufferPool.acquireDirect(true);

    Assert.assertEquals(42, bufferThree.capacity());
    Assert.assertEquals(0, bufferThree.position());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    assertBufferIsClear(bufferThree);

    byteBufferPool.release(bufferThree);

    Assert.assertEquals(1, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferOne);

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    try {
      byteBufferPool.release(bufferThree);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(126, allocator.getMemoryConsumption());

    byteBufferPool.release(bufferTwo);

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    try {
      byteBufferPool.release(bufferTwo);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    try {
      byteBufferPool.release(bufferOne);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    Assert.assertEquals(2, byteBufferPool.getPoolSize());
    Assert.assertEquals(84, allocator.getMemoryConsumption());

    byteBufferPool.clear();

    Assert.assertEquals(0, allocator.getMemoryConsumption());
    Assert.assertEquals(0, byteBufferPool.getPoolSize());

    byteBufferPool.checkMemoryLeaks();
  }

  @Test
  @Ignore
  public void mtTest() throws Exception {
    final ODirectMemoryAllocator allocator = new ODirectMemoryAllocator();
    final OByteBufferPool byteBufferPool = new OByteBufferPool(42, allocator, 600 * 8);
    final List<Future<Void>> futures = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean();

    final ExecutorService executorService = Executors.newCachedThreadPool();
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new Allocator(byteBufferPool, stop)));
    }

    Thread.sleep(5 * 60 * 1000);

    stop.set(true);

    for (Future<Void> future : futures) {
      future.get();
    }

    byteBufferPool.clear();

    byteBufferPool.checkMemoryLeaks();
    allocator.checkMemoryLeaks();
  }

  private void assertBufferIsClear(ByteBuffer bufferTwo) {
    while (bufferTwo.position() < bufferTwo.capacity()) {
      Assert.assertEquals(0, bufferTwo.get());
    }
  }

  private static final class Allocator implements Callable<Void> {
    private final OByteBufferPool pool;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicBoolean stop;
    private       long          allocatedSize;
    private List<ByteBuffer> allocatedBuffers = new ArrayList<>();

    private Allocator(OByteBufferPool pool, AtomicBoolean stop) {
      this.pool = pool;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      try {
        while (!stop.get()) {
          if (allocatedBuffers.size() < 500) {
            ByteBuffer buffer = pool.acquireDirect(false);
            allocatedBuffers.add(buffer);
          } else if (allocatedBuffers.size() < 1000) {
            if (random.nextDouble() <= 0.5) {
              ByteBuffer buffer = pool.acquireDirect(false);
              allocatedBuffers.add(buffer);
            } else {
              final int bufferToRemove = random.nextInt(allocatedBuffers.size());
              final ByteBuffer buffer = allocatedBuffers.remove(bufferToRemove);
              pool.release(buffer);
            }
          } else {
            if (random.nextDouble() <= 0.4) {
              ByteBuffer buffer = pool.acquireDirect(false);
              allocatedBuffers.add(buffer);
            } else {
              final int bufferToRemove = random.nextInt(allocatedBuffers.size());
              final ByteBuffer buffer = allocatedBuffers.remove(bufferToRemove);
              pool.release(buffer);
            }
          }
        }

        System.out.println("Allocated buffers " + allocatedBuffers.size());
        for (ByteBuffer buffer : allocatedBuffers) {
          pool.release(buffer);
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }
}
