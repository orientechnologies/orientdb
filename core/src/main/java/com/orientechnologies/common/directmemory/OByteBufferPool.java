package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.sql.parser.OInteger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class OByteBufferPool {
  private static final OByteBufferPool INSTANCE;

  static {
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

    final BigDecimal cacheSize = new BigDecimal(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024);
    final BigDecimal allocatedPages = cacheSize.add(new BigDecimal(OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger() + 1)).
        divide(new BigDecimal(pageSize), RoundingMode.CEILING);

    final int memoryChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsInteger();
    INSTANCE = new OByteBufferPool(pageSize, allocatedPages.longValue(), memoryChunkSize);
  }

  private final int        pageSize;
  private final ByteBuffer zeroPage;

  private final AtomicReferenceArray<BufferHolder> preallocatedAreas;
  private final AtomicLong nextAllocationPosition = new AtomicLong();
  private final int  maxPagesPerSingleArea;
  private final long preallocatedPages;

  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<ByteBuffer>();

  public static OByteBufferPool instance() {
    return INSTANCE;
  }

  public OByteBufferPool(int pageSize) {
    this(pageSize, -1);
  }

  public OByteBufferPool(int pageSize, long preallocatePages) {
    this(pageSize, preallocatePages, -1);
  }

  public OByteBufferPool(int pageSize, long preallocatePages, int maxChunkSize) {
    this.pageSize = pageSize;
    this.zeroPage = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());

    if (preallocatePages > 0) {
      int pagesPerArea = (maxChunkSize / pageSize);
      pagesPerArea = closestPowerOfTwo(pagesPerArea);

      //we need not the biggest value, it may cause buffer overflow, but biggest after that.
      while ((long) pagesPerArea * pageSize > maxChunkSize) {
        pagesPerArea = pagesPerArea >>> 1;
      }

      maxPagesPerSingleArea = pagesPerArea;

      final int arraySize = (int) ((preallocatePages + maxPagesPerSingleArea - 1) / maxPagesPerSingleArea);
      preallocatedAreas = new AtomicReferenceArray<BufferHolder>(arraySize);

      this.preallocatedPages = preallocatePages;
    } else {
      preallocatedAreas = null;
      maxPagesPerSingleArea = -1;
      this.preallocatedPages = -1;
    }
  }

  public int getSize() {
    return pool.size();
  }

  public int getMaxPagesPerChunk() {
    return maxPagesPerSingleArea;
  }

  public int getMaxAmountOfChunks() {
    return preallocatedAreas.length();
  }

  /**
   * Finds closest power of two for given integer value.
   * Idea is simple duplicate the most significant bit to the lowest bits for the smallest number of iterations possible and then increment
   * result value by 1.
   *
   * @param value Integer the most significant power of 2 should be found.
   * @return The most significant power of 2.
   */
  private int closestPowerOfTwo(int value) {
    int n = value - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= (1 << 30)) ? 1 << 30 : n + 1;
  }

  public ByteBuffer acquireDirect(boolean clear) {
    final ByteBuffer buffer = pool.poll();

    if (buffer != null) {
      if (clear) {
        buffer.position(0);
        buffer.put(zeroPage.duplicate());
      }

      buffer.position(0);
      return buffer;
    }

    if (preallocatedPages > 0) {
      if (nextAllocationPosition.get() < preallocatedPages) {
        final long currentAllocationPosition = nextAllocationPosition.getAndIncrement();

        if (currentAllocationPosition < preallocatedPages) {
          final int arrayIndex = (int) (currentAllocationPosition / maxPagesPerSingleArea);
          final int position = (int) (currentAllocationPosition & (maxPagesPerSingleArea - 1));

          BufferHolder bfh = preallocatedAreas.get(arrayIndex);
          if (bfh == null) {
            bfh = new BufferHolder();

            if (preallocatedAreas.compareAndSet(arrayIndex, null, bfh)) {
              final int allocationSize;
              if (arrayIndex < preallocatedAreas.length() - 1) {
                allocationSize = maxPagesPerSingleArea * pageSize;
              } else {
                final long pagesLeft = preallocatedPages - arrayIndex * pageSize;
                allocationSize = (int) (pagesLeft * pageSize);
              }

              try {
                bfh.buffer = ByteBuffer.allocateDirect(allocationSize).order(ByteOrder.nativeOrder());
              } finally {
                bfh.latch.countDown();
              }
            } else {
              bfh = preallocatedAreas.get(arrayIndex);
              try {
                if (bfh.buffer == null) {
                  bfh.latch.await();
                }
              } catch (InterruptedException e) {
                throw OException
                    .wrapException(new OInterruptedException("Wait of new preallocated memory area was interrupted"), e);
              }
            }
          } else if (bfh.buffer == null) {
            try {
              bfh.latch.await();
            } catch (InterruptedException e) {
              throw OException.wrapException(new OInterruptedException("Wait of new preallocated memory area was interrupted"), e);
            }
          }

          final int rawPosition = position * pageSize;
          final ByteBuffer db = bfh.buffer.duplicate();

          db.position(rawPosition);
          db.limit(rawPosition + pageSize);

          ByteBuffer slice = db.slice();
          slice.order(ByteOrder.nativeOrder());

          if (clear) {
            slice.position(0);
            slice.put(zeroPage.duplicate());
          }

          slice.position(0);
          return slice;
        }
      }

      assert false;
      OLogManager.instance().warn(this, "Preallocated memory limit is reached !");
    }

    return ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  public void release(ByteBuffer buffer) {
    pool.offer(buffer);
  }

  private static final class BufferHolder {
    private volatile ByteBuffer buffer;
    private final CountDownLatch latch = new CountDownLatch(1);
  }
}
