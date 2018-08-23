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

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.jna.ONative;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Object of this class works at the same time as factory for <code>DirectByteBuffer</code> objects and pool for
 * <code>DirectByteBuffer</code> objects which were used and now are free to be reused by other parts of the code.
 * All <code>DirectByteBuffer</code> objects have the same size which is specified in objects constructor as "page size".
 *
 * @see ODirectMemoryAllocator
 */
public class OByteBufferPool implements OByteBufferPoolMXBean {
  /**
   * Whether we should track memory leaks during application execution
   */
  private static final boolean TRACK = OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();

  /**
   * Holder for singleton instance. We use {@link AtomicReference} instead of static constructor to avoid throwing of exceptions
   * in static initializers.
   */
  private static final AtomicReference<OByteBufferPool> INSTANCE_HOLDER = new AtomicReference<>();

  /**
   * Limit of direct memory pointers are hold inside of the pool
   */
  private final int poolSize;

  private static final int LOCKED_CHUNK_SIZE = 256 * 1024 * 1024; //64M

  private final AtomicReferenceArray<LockedPointerHolder> lockedChuncks    = new AtomicReferenceArray<>(1024);
  private final AtomicInteger                             lockedChunkIndex = new AtomicInteger();

  private final boolean isLinux = Platform.isLinux();

  private final int osPageSize;

  /**
   * @return Singleton instance
   */
  public static OByteBufferPool instance() {
    final OByteBufferPool instance = INSTANCE_HOLDER.get();
    if (instance != null) {
      return instance;
    }

    final OByteBufferPool newInstance = new OByteBufferPool(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);
    if (INSTANCE_HOLDER.compareAndSet(null, newInstance)) {
      return newInstance;
    }

    return INSTANCE_HOLDER.get();
  }

  /**
   * Size of single page in bytes.
   */
  private final int pageSize;

  /**
   * {@link ByteBuffer}s can not be extended, so to keep mapping between pointers and buffers we use concurrent hash map.
   */
  private final ConcurrentHashMap<ByteBufferHolder, PointerHolder> bufferPointerMapping = new ConcurrentHashMap<>();

  /**
   * Pool of already allocated pages.
   */
  private final ConcurrentLinkedQueue<OPointer> pointersPool = new ConcurrentLinkedQueue<>();

  private final ConcurrentLinkedQueue<OPointer> lockedPointersPool = new ConcurrentLinkedQueue<>();

  /**
   * Size of the pool of pages is kept in separate counter because it is slow to ask pool itself and count all links in the pool.
   */
  private final AtomicInteger pointersPoolSize = new AtomicInteger();

  /**
   * Direct memory allocator.
   */
  private final ODirectMemoryAllocator allocator;

  /**
   * @param pageSize Size of single page (instance of <code>DirectByteBuffer</code>) returned by pool.
   */
  public OByteBufferPool(int pageSize) {
    this.pageSize = pageSize;
    this.allocator = ODirectMemoryAllocator.instance();
    this.poolSize = OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT.getValueAsInteger();

    if (isLinux) {
      osPageSize = ONative.instance().getpagesize();
    } else {
      osPageSize = -1;
    }
  }

  /**
   * @param allocator Direct memory allocator to use.
   * @param pageSize  Size of single page (instance of <code>DirectByteBuffer</code>) returned by pool.
   * @param poolSize  Size of the page pool
   */
  public OByteBufferPool(int pageSize, ODirectMemoryAllocator allocator, int poolSize) {
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.poolSize = poolSize;

    if (isLinux) {
      osPageSize = ONative.instance().getpagesize();
    } else {
      osPageSize = -1;
    }
  }

  /**
   * Acquires direct memory buffer with native byte order.
   * If there is free (already released) direct memory page we reuse it, otherwise new
   * memory chunk is allocated from direct memory.
   *
   * @param clear      Whether returned buffer should be filled with zeros before return.
   * @param lockMemory Prevents memory from swapping, valid only for Linux.
   *
   * @return Direct memory buffer instance.
   */
  public ByteBuffer acquireDirect(boolean clear, boolean lockMemory) {
    OPointer pointer;

    lockMemory = lockMemory && isLinux;

    if (lockMemory) {
      pointer = lockedPointersPool.poll();

      while (pointer == null) {
        int lockedIndex = lockedChunkIndex.get();
        LockedPointerHolder pointerHolder;

        if (lockedIndex >= lockedChuncks.length()) {
          return acquireDirect(clear, false);
        }

        pointerHolder = lockedChuncks.get(lockedIndex);

        if (pointerHolder == null) {
          pointerHolder = new LockedPointerHolder(LOCKED_CHUNK_SIZE);

          if (lockedChuncks.compareAndSet(lockedIndex, null, pointerHolder)) {
            final OPointer chunkPointer = allocator.allocate(LOCKED_CHUNK_SIZE, osPageSize);
            ONative.instance().mlock(chunkPointer.getNativePointer(), LOCKED_CHUNK_SIZE);
            pointerHolder.initPointer(chunkPointer);
          } else {
            continue;
          }

          try {
            pointer = pointerHolder.nextPage(pageSize);
          } catch (InterruptedException e) {
            throw OException.wrapException(new OInterruptedException("Waiting for direct memory allocation was interrupted"), e);
          }
        } else {
          try {
            pointer = pointerHolder.nextPage(pageSize);
          } catch (InterruptedException e) {
            throw OException.wrapException(new OInterruptedException("Waiting for direct memory allocation was interrupted"), e);
          }
        }

        //no space left in current locked pointer holder
        if (pointer == null) {
          lockedChunkIndex.compareAndSet(lockedIndex, lockedIndex + 1);
        }
      }
    } else {
      pointer = pointersPool.poll();

      if (pointer != null) {
        pointersPoolSize.decrementAndGet();
      } else {
        pointer = allocator.allocate(pageSize, -1);
      }
    }

    if (clear) {
      pointer.clear();
    }

    final ByteBuffer buffer = pointer.getNativeByteBuffer();
    buffer.position(0);

    bufferPointerMapping.put(wrapBuffer(buffer), wrapPointer(pointer, lockMemory));
    return buffer;
  }

  /**
   * Put buffer which is not used any more back to the pool or frees direct memory if pool is full.
   *
   * @param buffer Not used instance of buffer.
   *
   * @see OGlobalConfiguration#DIRECT_MEMORY_POOL_LIMIT
   */
  public void release(ByteBuffer buffer) {
    final PointerHolder holder = bufferPointerMapping.remove(wrapBuffer(buffer));

    if (holder == null) {
      throw new IllegalArgumentException(String.format("Buffer %X is not acquired", System.identityHashCode(buffer)));
    }

    if (holder.lockMemory) {
      lockedPointersPool.add(holder.pointer);
    } else {
      long poolSize = pointersPoolSize.incrementAndGet();
      if (poolSize > this.poolSize) {
        pointersPoolSize.decrementAndGet();
        allocator.deallocate(holder.pointer);
      } else {
        pointersPool.add(holder.pointer);
      }
    }
  }

  /**
   * @inheritDoc
   */
  @Override
  public int getPoolSize() {
    return pointersPoolSize.get();
  }

  /**
   * Writes passed in message into the log with provided {@link ByteBuffer} identity hash code and checks whether buffer was
   * released
   * to pool.
   *
   * @param prefix Prefix to add to the log message
   * @param buffer Buffer to check whether it is acquired or not
   */
  public void logTrackedBufferInfo(String prefix, ByteBuffer buffer) {
    if (TRACK) {
      final StringBuilder builder = new StringBuilder();
      builder.append("DIRECT-TRACK: ").append(prefix).append(String.format(" buffer `%X` ", System.identityHashCode(buffer)));

      PointerHolder holder = bufferPointerMapping.get(wrapBuffer(buffer));
      if (holder == null)
        builder.append("untracked");
      else
        builder.append("allocated from: ").append('\n').append(getStackTraceAsString(holder.allocation)).append('\n');

      OLogManager.instance().errorNoDb(this, builder.toString(), null);
    }
  }

  /**
   * Checks whether there are not released buffers in the pool
   */
  public void checkMemoryLeaks() {
    boolean detected = false;
    if (TRACK) {
      for (Map.Entry<ByteBufferHolder, PointerHolder> entry : bufferPointerMapping.entrySet()) {
        OLogManager.instance()
            .errorNoDb(this, "DIRECT-TRACK: unreleased direct memory buffer `%X` detected.", entry.getValue().allocation,
                System.identityHashCode(entry.getKey().byteBuffer));
        detected = true;
      }
    }

    assert !detected;
  }

  /**
   * Clears pool and dealocates memory.
   */
  public void clear() {
    for (OPointer pointer : pointersPool) {
      allocator.deallocate(pointer);
    }

    pointersPool.clear();
    pointersPoolSize.set(0);

    if (!TRACK && !bufferPointerMapping.isEmpty()) {
      final String message =
          "There are not released allocations in " + "ByteBufferPool which may indicate presence of memory leaks in database!!"
              + "Start JVM with system property" + OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getKey()
              + " = true for more details";

      OLogManager.instance().warnNoDb(this, message);
    }

    for (PointerHolder holder : bufferPointerMapping.values()) {
      allocator.deallocate(holder.pointer);
    }

    bufferPointerMapping.clear();

    final int lockedIndex = lockedChunkIndex.get();

    for (int i = 0; i <= lockedIndex; i++) {
      final LockedPointerHolder holder = lockedChuncks.get(i);

      if (holder != null) {
        ONative.instance().munlock(holder.pointer.getNativePointer(), holder.pointer.getSize());
        allocator.deallocate(holder.pointer);
        lockedChuncks.set(i, null);
      }
    }

    lockedChunkIndex.set(0);
    lockedPointersPool.clear();
  }

  /**
   * Holder which is used to compare byte buffers by object's identity not by content
   */
  private static final class ByteBufferHolder {
    private final ByteBuffer byteBuffer;

    ByteBufferHolder(ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(byteBuffer);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ByteBufferHolder))
        return false;

      return ((ByteBufferHolder) obj).byteBuffer == this.byteBuffer;
    }
  }

  /**
   * Holder which contains direct memory pointer and if memory tracking is enabled stack trace for the first allocation.
   */
  private static final class PointerHolder {
    private final OPointer  pointer;
    private final Exception allocation;
    private final boolean   lockMemory;

    PointerHolder(OPointer pointer, Exception allocation, boolean lockMemory) {
      this.pointer = pointer;
      this.allocation = allocation;
      this.lockMemory = lockMemory;
    }
  }

  private ByteBufferHolder wrapBuffer(ByteBuffer byteBuffer) {
    return new ByteBufferHolder(byteBuffer);
  }

  private PointerHolder wrapPointer(OPointer pointer, boolean lockMemory) {
    if (TRACK) {
      return new PointerHolder(pointer, new Exception(), lockMemory);
    } else {
      return new PointerHolder(pointer, null, lockMemory);
    }
  }

  /**
   * @return Wellformed stack trace of exception.
   */
  private static String getStackTraceAsString(Throwable throwable) {
    @SuppressWarnings("resource")
    final StringWriter writer = new StringWriter();
    throwable.printStackTrace(new PrintWriter(writer));
    return writer.toString();
  }

  private static final class LockedPointerHolder {
    private volatile OPointer pointer;
    private final    long     size;

    private final AtomicLong     position = new AtomicLong();
    private final CountDownLatch latch    = new CountDownLatch(1);

    LockedPointerHolder(long size) {
      this.size = size;
    }

    private OPointer nextPage(int pageSize) throws InterruptedException {
      final long ps = position.getAndAdd(pageSize);
      if (ps >= size) {
        return null;
      }

      if (pointer == null) {
        awaitInit();
      }

      return new OPointer(new Pointer(Pointer.nativeValue(pointer.getNativePointer()) + ps), pageSize);
    }

    private void awaitInit() throws InterruptedException {
      latch.await();
    }

    private void initPointer(OPointer pointer) {
      this.pointer = pointer;
      latch.countDown();
    }
  }
}
