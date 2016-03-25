/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.common.directmemory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Object of this class works at the same time as factory for <code>DirectByteBuffer</code> objects and pool for
 * <code>DirectByteBuffer</code> objects which were used and now are free to be reused by other parts of the code.
 * <p>
 * All <code>DirectByteBuffer</code> objects have the same size which is specified in objects constructor as "page size". Despite of
 * the fact that size of page is relatively small memory may be acquired from OS in relatively big chunks. It is done to optimize
 * memory usage inside of database.
 *
 * @see OGlobalConfiguration#MEMORY_CHUNK_SIZE
 */
public class OByteBufferPool {
  private static final OByteBufferPool INSTANCE;

  static {
    // page size in bytes
    final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

    // size of disk cache in bytes
    final BigDecimal cacheSize = new BigDecimal(OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024);

    // Maximum amount of pages which are going to be used by system.
    final BigDecimal allocatedPages = cacheSize
        .add(new BigDecimal((OGlobalConfiguration.WAL_CACHE_SIZE.getValueAsInteger() + 1) * pageSize))
        .divide(new BigDecimal(pageSize), RoundingMode.CEILING);

    // Maximum amount of chunk size which should be allocated at once by system
    final int memoryChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsInteger();

    // instance of byte buffer which should be used by all storage components
    INSTANCE = new OByteBufferPool(pageSize, allocatedPages.longValue(), memoryChunkSize);
  }

  /**
   * Size of single byte buffer instance in bytes.
   */
  private final int                                pageSize;

  /**
   * Page which is filled with zeros and used to speedup clear operation on page acquire operation {@link #acquireDirect(boolean)}.
   */
  private final ByteBuffer                         zeroPage;

  /**
   * Collections of chunks which are prealocated on demand when limit of currently allocated memory exceeds.
   */
  private final AtomicReferenceArray<BufferHolder> preallocatedAreas;

  /**
   * Index of next page which will be allocated if pool is empty.
   */
  private final AtomicLong                         nextAllocationPosition = new AtomicLong();

  /**
   * Maximum amount of pages which should be allocated in single preallocated memory chunk.
   */
  private final int                                maxPagesPerSingleArea;

  /**
   * Total amount of pages which may be preallocated in big memory chunks.
   */
  private final long                               preallocatedPages;

  /**
   * Pool of pages which are already allocated but not used any more.
   */
  private final ConcurrentLinkedQueue<ByteBuffer>  pool                   = new ConcurrentLinkedQueue<ByteBuffer>();

  /**
   * Pool returned by this method is used in all components of storage. Memory used by this pool is preallocated by chunks with size
   * not more than {@link OGlobalConfiguration#MEMORY_CHUNK_SIZE} Amount of maximum memory preallocated by this pool equals to sum
   * of {@link OGlobalConfiguration#DISK_CACHE_SIZE} and {@link OGlobalConfiguration#WAL_CACHE_SIZE} and one.
   * <p>
   * Size of single page equals to {@link OGlobalConfiguration#DISK_CACHE_PAGE_SIZE}.
   *
   * @return Global instance is used inside of storage.
   */
  public static OByteBufferPool instance() {
    return INSTANCE;
  }

  /**
   * @param pageSize
   *          Size of single page (instance of <code>DirectByteBuffer</code>) returned by pool.
   */
  public OByteBufferPool(int pageSize) {
    this(pageSize, -1);
  }

  /**
   * @param pageSize
   *          Size of single page (<code>DirectByteBuffer</code>) returned by pool.
   * @param preallocatePages
   *          Limit of pages memory for which have to be acquired in big chunks, after that memory will be allocated from OS in
   *          small chunks which are equals to page size.
   */
  public OByteBufferPool(int pageSize, long preallocatePages) {
    this(pageSize, preallocatePages, -1);
  }

  /**
   * @param pageSize
   *          Size of single page (<code>DirectByteBuffer</code>) returned by pool.
   * @param preallocatePages
   *          Limit of pages memory for which have to be acquired in big chunks, after that memory will be allocated from OS in
   *          small chunks which are equals to page size.
   * @param maxChunkSize
   *          Maximum size of big memory chunk which will be preallocated by pool in bytes.
   */
  public OByteBufferPool(int pageSize, long preallocatePages, int maxChunkSize) {
    this.pageSize = pageSize;
    this.zeroPage = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());

    if (preallocatePages > 0) {
      if (maxChunkSize < 0) {
        maxChunkSize = Integer.MAX_VALUE;
      }

      int pagesPerArea = (maxChunkSize / pageSize);
      pagesPerArea = closestPowerOfTwo(pagesPerArea);

      // we need not the biggest value, it may cause buffer overflow, but biggest after that.
      while ((long) pagesPerArea * pageSize > maxChunkSize) {
        pagesPerArea = pagesPerArea >>> 1;
      }

      maxPagesPerSingleArea = pagesPerArea;

      // rounding of division to biggest integer value
      final int arraySize = (int) ((preallocatePages + maxPagesPerSingleArea - 1) / maxPagesPerSingleArea);
      preallocatedAreas = new AtomicReferenceArray<BufferHolder>(arraySize);

      this.preallocatedPages = preallocatePages;
    } else {
      preallocatedAreas = null;
      maxPagesPerSingleArea = -1;
      this.preallocatedPages = -1;
    }
  }

  /**
   * @return Amount of pages which are available in pool. Pages which were allocated and now not used.
   */
  public int getSize() {
    return pool.size();
  }

  /**
   * @return Maximum amount of pages in single preallocate memory chunk.
   */
  public int getMaxPagesPerChunk() {
    return maxPagesPerSingleArea;
  }

  /**
   * @return Maximum amount of chunks which may be preallocate by given pool.
   */
  public int getMaxAmountOfChunks() {
    return preallocatedAreas.length();
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most significant bit to the lowest bits for
   * the smallest number of iterations possible and then increment result value by 1.
   *
   * @param value
   *          Integer the most significant power of 2 should be found.
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

  /**
   * Acquires direct memory buffer. If there is free (already released) direct memory buffer we reuse it, otherwise either new
   * memory chunk is allocated from direct memory or slice of already preallocated memory chunk is used as new byte buffer instance.
   * <p>
   * If we reached maximum amount of preallocated memory chunks then small portion of direct memory equals to page size is
   * allocated. Byte order of returned direct memory buffer equals to native byte order.
   * <p>
   * Position of returned buffer is always zero.
   *
   * @param clear
   *          Whether returned buffer should be filled with zeros before return.
   * @return Direct memory buffer instance.
   */
  public ByteBuffer acquireDirect(boolean clear) {
    // check the pool first.
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

        // verify if limit is reached
        if (currentAllocationPosition < preallocatedPages) {
          final int arrayIndex = (int) (currentAllocationPosition / maxPagesPerSingleArea);
          final int position = (int) (currentAllocationPosition & (maxPagesPerSingleArea - 1));

          // we cannot free chunk of allocated memory so we set place holder first
          // if operation successful we allocate part of direct memory.
          BufferHolder bfh = preallocatedAreas.get(arrayIndex);
          if (bfh == null) {
            bfh = new BufferHolder();

            if (preallocatedAreas.compareAndSet(arrayIndex, null, bfh)) {
              final int allocationSize;
              if (arrayIndex < preallocatedAreas.length() - 1) {
                allocationSize = maxPagesPerSingleArea * pageSize;
              } else {
                final long pagesLeft = preallocatedPages - arrayIndex * maxPagesPerSingleArea;
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
                // if place holder is not null it means that byte buffer is allocated but not set yet in other thread
                // so we wait till buffer instance will be shared by other thread
                if (bfh.buffer == null) {
                  bfh.latch.await();
                }
              } catch (InterruptedException e) {
                throw OException.wrapException(new OInterruptedException("Wait of new preallocated memory area was interrupted"),
                    e);
              }
            }
          } else if (bfh.buffer == null) {
            // if place holder is not null it means that byte buffer is allocated but not set yet in other thread
            // so we wait till buffer instance will be shared by other thread
            try {
              bfh.latch.await();
            } catch (InterruptedException e) {
              throw OException.wrapException(new OInterruptedException("Wait of new preallocated memory area was interrupted"), e);
            }
          }

          final int rawPosition = position * pageSize;
          // duplicate buffer to have thread local version of buffer position.
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

    // this should not happen if amount of pages is needed for storage is calculated correctly
    return ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  /**
   * Put buffer which is not used any more back to the pool.
   *
   * @param buffer
   *          Not used instance of buffer.
   */
  public void release(ByteBuffer buffer) {
    pool.offer(buffer);
  }

  private static final class BufferHolder {
    private volatile ByteBuffer  buffer;
    private final CountDownLatch latch = new CountDownLatch(1);
  }
}
