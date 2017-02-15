package com.orientechnologies.common.directmemory;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Test
public class OByteBufferPoolTest {
  public void testAcquireReleaseSinglePage() {
    OByteBufferPool pool = new OByteBufferPool(12);
    Assert.assertEquals(pool.getSize(), 0);

    ByteBuffer buffer = pool.acquireDirect(true);
    Assert.assertEquals(pool.getSize(), 0);
    Assert.assertEquals(buffer.position(), 0);

    buffer.position(10);
    buffer.put((byte) 42);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), 1);

    buffer = pool.acquireDirect(false);
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(10), 42);

    Assert.assertEquals(pool.getSize(), 0);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), 1);

    buffer = pool.acquireDirect(true);
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(10), 0);

    Assert.assertEquals(pool.getSize(), 0);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), 1);
  }

  public void testAcquireReleasePageWithPreallocation() {
    OByteBufferPool pool = new OByteBufferPool(10, 300, 200);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);

    Assert.assertEquals(pool.getSize(), 0);

    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    for (int i = 0; i < 99; i++) {
      buffers.add(pool.acquireDirect(false));
      assertBufferOperations(pool, 0);
    }

    for (int i = 0; i < 99; i++) {
      final ByteBuffer buffer = buffers.get(i);

      buffer.position(8);
      buffer.put((byte) 42);

      pool.release(buffers.get(i));
      assertBufferOperations(pool, i + 1);
    }
  }

  @Test(enabled = false)
  public void testAcquireReleasePageWithPreallocationInMT() throws Exception {
    final OByteBufferPool pool = new OByteBufferPool(10, 300, 200);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);

    final List<Future<Long>> futures = new ArrayList<Future<Long>>();
    final CountDownLatch latch = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(8);

    for (int i = 0; i < 5; i++) {
      final int th = i;

      futures.add(executor.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          latch.await();

          final long startTs = System.nanoTime();

          try {
            for (int n = 0; n < 1000000; n++) {
              List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

              for (int i = 0; i < 2000; i++) {
                buffers.add(pool.acquireDirect(false));
              }

              for (int i = 0; i < 2000; i++) {
                final ByteBuffer buffer = buffers.get(i);
                pool.release(buffer);
              }

            }
          } catch (Exception e) {
            e.printStackTrace();
            throw e;
          }

          final long ensTs = System.nanoTime();

          return (ensTs - startTs);
        }
      }));
    }

    latch.countDown();

    long sum = 0;
    int counter = 0;
    for (Future<Long> future : futures) {
      sum += future.get();
      counter++;
    }

    System.out.printf("Avg. latency %d ", sum / (counter * 2000000000L));

  }

  private void assertBufferOperations(OByteBufferPool pool, int initialSize) {
    ByteBuffer buffer = pool.acquireDirect(true);
    Assert.assertEquals(pool.getSize(), initialSize);
    Assert.assertEquals(buffer.position(), 0);

    buffer.position(8);
    buffer.put((byte) 42);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), initialSize + 1);

    buffer = pool.acquireDirect(false);
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(8), 42);

    Assert.assertEquals(pool.getSize(), initialSize);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), initialSize + 1);

    buffer = pool.acquireDirect(true);
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(8), 0);

    Assert.assertEquals(pool.getSize(), initialSize);

    buffer.position(8);
    buffer.put((byte) 42);

    pool.release(buffer);
    Assert.assertEquals(pool.getSize(), initialSize + 1);
  }

}
