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

import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Object of this class works at the same time as factory for <code>DirectByteBuffer</code> objects
 * and pool for <code>DirectByteBuffer</code> objects which were used and now are free to be reused
 * by other parts of the code. All <code>DirectByteBuffer</code> objects have the same size which is
 * specified in objects constructor as "page size".
 *
 * @see ODirectMemoryAllocator
 */
public final class OByteBufferPool implements OByteBufferPoolMXBean {
  /** Whether we should track memory leaks during application execution */
  private static final boolean TRACK =
      OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();

  /**
   * Holder for singleton instance. We use {@link AtomicReference} instead of static constructor to
   * avoid throwing of exceptions in static initializers.
   */
  private static final AtomicReference<OByteBufferPool> INSTANCE_HOLDER = new AtomicReference<>();

  /** Limit of direct memory pointers are hold inside of the pool */
  private final int poolSize;

  /** @return Singleton instance */
  public static OByteBufferPool instance(OContextConfiguration contextConfiguration) {
    final OByteBufferPool instance = INSTANCE_HOLDER.get();
    if (instance != null) {
      return instance;
    }

    int bufferSize;
    if (contextConfiguration != null) {
      bufferSize =
          contextConfiguration.getValueAsInteger(OGlobalConfiguration.DISK_CACHE_PAGE_SIZE);
    } else {
      bufferSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();
    }

    final OByteBufferPool newInstance = new OByteBufferPool(bufferSize * 1024);
    if (INSTANCE_HOLDER.compareAndSet(null, newInstance)) {
      return newInstance;
    }

    return INSTANCE_HOLDER.get();
  }

  /** Size of single page in bytes. */
  private final int pageSize;

  /**
   * {@link ByteBuffer}s can not be extended, so to keep mapping between pointers and buffers we use
   * concurrent hash map.
   */
  private final ConcurrentHashMap<OPointer, PointerTracker> pointerMapping =
      new ConcurrentHashMap<>();

  /** Pool of already allocated pages. */
  private final ConcurrentLinkedQueue<OPointer> pointersPool = new ConcurrentLinkedQueue<>();

  /**
   * Size of the pool of pages is kept in separate counter because it is slow to ask pool itself and
   * count all links in the pool.
   */
  private final AtomicInteger pointersPoolSize = new AtomicInteger();

  /** Direct memory allocator. */
  private final ODirectMemoryAllocator allocator;

  /**
   * @param pageSize Size of single page (instance of <code>DirectByteBuffer</code>) returned by
   *     pool.
   */
  public OByteBufferPool(int pageSize) {
    this.pageSize = pageSize;
    this.allocator = ODirectMemoryAllocator.instance();
    this.poolSize = OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT.getValueAsInteger();
  }

  /**
   * @param allocator Direct memory allocator to use.
   * @param pageSize Size of single page (instance of <code>DirectByteBuffer</code>) returned by
   *     pool.
   * @param poolSize Size of the page pool
   */
  public OByteBufferPool(int pageSize, ODirectMemoryAllocator allocator, int poolSize) {
    this.pageSize = pageSize;
    this.allocator = allocator;
    this.poolSize = poolSize;
  }

  /**
   * Acquires direct memory buffer with native byte order. If there is free (already released)
   * direct memory page we reuse it, otherwise new memory chunk is allocated from direct memory.
   *
   * @param clear Whether returned buffer should be filled with zeros before return.
   * @param intention Why this memory is allocated. This parameter is used for memory profiling.
   * @return Direct memory buffer instance.
   */
  public final OPointer acquireDirect(boolean clear, Intention intention) {
    OPointer pointer;

    pointer = pointersPool.poll();

    if (pointer != null) {
      pointersPoolSize.decrementAndGet();

      if (clear) {
        pointer.clear();
      }
    } else {
      pointer = allocator.allocate(pageSize, clear, intention);
    }

    pointer.getNativeByteBuffer().position(0);

    if (TRACK) {
      pointerMapping.put(pointer, generatePointer());
    }

    return pointer;
  }

  /**
   * Put buffer which is not used any more back to the pool or frees direct memory if pool is full.
   *
   * @param pointer Not used instance of buffer.
   * @see OGlobalConfiguration#DIRECT_MEMORY_POOL_LIMIT
   */
  public final void release(OPointer pointer) {
    if (TRACK) {
      pointerMapping.remove(pointer);
    }

    long poolSize = pointersPoolSize.incrementAndGet();
    if (poolSize > this.poolSize) {
      pointersPoolSize.decrementAndGet();
      allocator.deallocate(pointer);
    } else {
      pointersPool.add(pointer);
    }
  }

  /** @inheritDoc */
  @Override
  public final int getPoolSize() {
    return pointersPoolSize.get();
  }

  /** Checks whether there are not released buffers in the pool */
  public void checkMemoryLeaks() {
    boolean detected = false;
    if (TRACK) {
      for (Map.Entry<OPointer, PointerTracker> entry : pointerMapping.entrySet()) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "DIRECT-TRACK: unreleased direct memory pointer `%X` detected.",
                entry.getValue().allocation,
                System.identityHashCode(entry.getKey()));
        detected = true;
      }
    }

    assert !detected;
  }

  /** Clears pool and dealocates memory. */
  public void clear() {
    for (OPointer pointer : pointersPool) {
      allocator.deallocate(pointer);
    }

    pointersPool.clear();
    pointersPoolSize.set(0);

    for (OPointer pointer : pointerMapping.keySet()) {
      allocator.deallocate(pointer);
    }

    pointerMapping.clear();
  }

  /** Holder which contains if memory tracking is enabled stack trace for the first allocation. */
  private static final class PointerTracker {
    private final Exception allocation;

    PointerTracker(Exception allocation) {
      this.allocation = allocation;
    }
  }

  private PointerTracker generatePointer() {
    return new PointerTracker(new Exception());
  }
}
