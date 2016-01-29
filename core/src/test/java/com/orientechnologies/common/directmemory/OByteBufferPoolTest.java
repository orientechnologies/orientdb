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
    OByteBufferPool pool = new OByteBufferPool(10, 100, 300);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);
    Assert.assertEquals(pool.getMaxAmountOfChunks(), 7);

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
    final OByteBufferPool pool = new OByteBufferPool(10, 10000, 300);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);
    Assert.assertEquals(pool.getMaxAmountOfChunks(), 625);

    final List<Future<Void>> futures = new ArrayList<Future<Void>>();
    final CountDownLatch latch = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(8);

    for (int i = 0; i < 5; i++) {
      futures.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          latch.await();

          try {
            for (int n = 0; n < 1000000; n++) {
              List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

              for (int i = 0; i < 2000; i++) {
                buffers.add(pool.acquireDirect(false));
                assertBufferOperations(pool);
              }

              for (int i = 0; i < 2000; i++) {
                final ByteBuffer buffer = buffers.get(i);

                buffer.position(8);
                buffer.put((byte) 42);

                pool.release(buffers.get(i));
                assertBufferOperations(pool);
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
            throw e;
          }

          return null;
        }

        private void assertBufferOperations(OByteBufferPool pool) {
          ByteBuffer buffer = pool.acquireDirect(true);
          Assert.assertEquals(buffer.position(), 0);

          buffer.position(8);
          buffer.put((byte) 42);

          pool.release(buffer);

          buffer = pool.acquireDirect(false);
          Assert.assertEquals(buffer.position(), 0);
          Assert.assertEquals(buffer.get(8), 42);

          pool.release(buffer);

          buffer = pool.acquireDirect(true);
          Assert.assertEquals(buffer.position(), 0);
          Assert.assertEquals(buffer.get(8), 0);

          buffer.position(8);
          buffer.put((byte) 42);

          pool.release(buffer);
        }
      }));

      latch.countDown();

      for (Future<Void> future : futures) {
        future.get();
      }
    }
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
