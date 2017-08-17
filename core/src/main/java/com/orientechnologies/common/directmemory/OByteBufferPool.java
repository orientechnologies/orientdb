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
import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import javax.management.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;
import java.util.logging.Level;

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
public class OByteBufferPool implements OByteBufferPoolMXBean {
  /**
   * {@link OByteBufferPool}'s MBean name.
   */
  private static final String MBEAN_NAME = "com.orientechnologies.common.directmemory:type=OByteBufferPoolMXBean";

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
    return InstanceHolder.INSTANCE;
  }

  private static final boolean TRACK = OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();

  /**
   * Size of single byte buffer instance in bytes.
   */
  private final int pageSize;

  /**
   * Collections of chunks which are preallocated on demand when limit of currently allocated memory exceeds.
   */
  private final AtomicReference<BufferHolder> lastPreallocatedArea;

  /**
   * Index of next page which will be allocated if pool is empty.
   */
  private final AtomicLong nextAllocationPosition = new AtomicLong();

  /**
   * Maximum amount of pages which should be allocated in single preallocated memory chunk.
   */
  private final int maxPagesPerSingleArea;

  /**
   * Limit of memory which will be allocated by big chunks (in pages)
   */
  private final long preAllocationLimit;

  /**
   * Pool of pages which are already allocated but not used any more.
   */
  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();

  /**
   * Tracks the number of the overflow buffer allocations.
   */
  private final AtomicLong overflowBufferCount = new AtomicLong();

  /**
   * Size of page pool, we use separate counter because {@link ConcurrentLinkedQueue#size()} has linear complexity.
   */
  private final AtomicInteger poolSize = new AtomicInteger();

  /**
   * Amount of native memory in bytes consumed by current byte buffer pool
   */
  private final AtomicLong allocatedMemory = new AtomicLong();

  /**
   * Tracks the status of the MBean registration.
   */
  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  private final ReferenceQueue<ByteBuffer>                    trackedBuffersQueue;
  private final Set<TrackedBufferReference>                   trackedReferences;
  private final Map<TrackedBufferKey, TrackedBufferReference> trackedBuffers;
  private final Map<TrackedBufferKey, Exception>              trackedReleases;

  private volatile boolean          traceEnabled     = false;
  private volatile TraceAggregation traceAggregation = TraceAggregation.Medium;

  private final AtomicLongArray[] aggregatedTraceStats;
  private final BufferPoolMXBean  directBufferPoolMxBean;
  private final AtomicLong        expectedDirectMemorySize;
  private final AtomicLong        directMemoryEventCounter;

  /**
   * @param pageSize Size of single page (instance of <code>DirectByteBuffer</code>) returned by pool.
   */
  public OByteBufferPool(int pageSize) {
    this(pageSize, -1, -1);
  }

  /**
   * @param pageSize           Size of single page (<code>DirectByteBuffer</code>) returned by pool.
   * @param maxChunkSize       Maximum allocation chunk size
   * @param preAllocationLimit Limit of memory which will be allocated by big chunks
   */
  public OByteBufferPool(int pageSize, int maxChunkSize, long preAllocationLimit) {
    this.pageSize = pageSize;

    this.preAllocationLimit = (preAllocationLimit / pageSize) * pageSize;

    int pagesPerArea = (maxChunkSize / pageSize);
    if (pagesPerArea > 1) {
      pagesPerArea = closestPowerOfTwo(pagesPerArea);

      // we need not the biggest value, it may cause buffer overflow, but biggest after that.
      while ((long) pagesPerArea * pageSize > maxChunkSize) {
        pagesPerArea = pagesPerArea >>> 1;
      }

      maxPagesPerSingleArea = pagesPerArea;
      lastPreallocatedArea = new AtomicReference<>();
    } else {
      maxPagesPerSingleArea = 1;
      lastPreallocatedArea = null;
    }

    if (TRACK) {
      trackedBuffersQueue = new ReferenceQueue<>();
      trackedReferences = new HashSet<>();
      trackedBuffers = new HashMap<>();
      trackedReleases = new HashMap<>();
    } else {
      trackedBuffersQueue = null;
      trackedReferences = null;
      trackedBuffers = null;
      trackedReleases = null;
    }

    // Initialize tracing, we have to do it here once to avoid concurrency issues while turning the tracing on or off.

    final TraceEvent[] traceEvents = TraceEvent.values();
    aggregatedTraceStats = new AtomicLongArray[traceEvents.length];
    for (TraceEvent event : traceEvents)
      if (event.aggregate)
        // stores only deltas, so the size is the half of the full stats size plus one more element for the event counting
        aggregatedTraceStats[event.ordinal()] = new AtomicLongArray(event.statsSize / 2 + 1);

    // find the JVM direct memory buffer pool bean
    BufferPoolMXBean foundDirectBufferPoolMxBean = null;
    for (BufferPoolMXBean bean : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
      if ("direct".equals(bean.getName())) {
        foundDirectBufferPoolMxBean = bean;
        break;
      }
    directBufferPoolMxBean = foundDirectBufferPoolMxBean;
    if (directBufferPoolMxBean == null)
      OLogManager.instance()
          .warn(this, "DIRECT-TRACE: JVM direct memory buffer pool JMX bean is not found, tracing will be less detailed.");

    expectedDirectMemorySize = directBufferPoolMxBean == null ? null : new AtomicLong();
    directMemoryEventCounter = directBufferPoolMxBean == null ? null : new AtomicLong();

    setTraceAggregation(OGlobalConfiguration.DIRECT_MEMORY_TRACE_AGGREGATION.getValue());
    setTraceEnabled(OGlobalConfiguration.DIRECT_MEMORY_TRACE.getValueAsBoolean());
  }

  @Override
  public void setTraceAggregation(TraceAggregation traceAggregation) {
    if (this.traceAggregation == traceAggregation)
      return;

    this.traceAggregation = traceAggregation;
    resetTracing();
  }

  @Override
  public TraceAggregation getTraceAggregation() {
    return traceAggregation;
  }

  /**
   * Enables/disables the tracing.
   *
   * @param traceEnabled {@code true} to enable the tracing, {@code false} to disable it.
   */
  public void setTraceEnabled(boolean traceEnabled) {
    if (this.traceEnabled == traceEnabled)
      return;

    this.traceEnabled = traceEnabled;
    resetTracing();
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
  int getMaxPagesPerChunk() {
    return maxPagesPerSingleArea;
  }

  /**
   * Finds closest power of two for given integer value. Idea is simple duplicate the most significant bit to the lowest bits for
   * the smallest number of iterations possible and then increment result value by 1.
   *
   * @param value Integer the most significant power of 2 should be found.
   *
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
   * memory chunk is allocated from direct memory or slice of already preallocated memory chunk is used as new byte buffer
   * instance.
   * <p>
   * If we reached maximum amount of preallocated memory chunks then small portion of direct memory equals to page size is
   * allocated. Byte order of returned direct memory buffer equals to native byte order.
   * <p>
   * Position of returned buffer is always zero.
   *
   * @param clear Whether returned buffer should be filled with zeros before return.
   *
   * @return Direct memory buffer instance.
   */
  public ByteBuffer acquireDirect(boolean clear) {
    // check the pool first.
    final ByteBuffer buffer = pool.poll();

    if (buffer != null) {
      final long beforeSize = poolSize.getAndDecrement();

      if (clear) {
        buffer.position(0);
        buffer.put(new byte[pageSize]);
      }

      buffer.position(0);

      return trace(track(buffer), TraceEvent.AcquiredFromPool, beforeSize, -1);
    }

    if (maxPagesPerSingleArea > 1) {
      long currentAllocationPosition;

      do {
        currentAllocationPosition = nextAllocationPosition.get();

        //if we hit the end of preallocation buffer we allocate by small chunks
        if (currentAllocationPosition >= preAllocationLimit) {
          final long beforeCount = overflowBufferCount.getAndIncrement();
          final long beforeSize = allocatedMemory.getAndAdd(pageSize);

          return trace(track(ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())),
              TraceEvent.OverflowBufferAllocated, beforeCount, +1, beforeSize, +pageSize);
        }

      } while (!nextAllocationPosition.compareAndSet(currentAllocationPosition, currentAllocationPosition + 1));

      //all chucks consumes maxPagesPerSingleArea space with exception of last one
      int position = (int) (currentAllocationPosition & (maxPagesPerSingleArea - 1));
      int bufferIndex = (int) (currentAllocationPosition / maxPagesPerSingleArea);

      //allocation size should be the same for all buffers from chuck with the same index
      final int allocationSize = (int) Math
          .min(maxPagesPerSingleArea * pageSize, preAllocationLimit - (bufferIndex * maxPagesPerSingleArea * pageSize));

      //page is going to be allocated above the preallocation limit
      if (allocationSize <= position * pageSize) {
        final long beforeCount = overflowBufferCount.getAndIncrement();
        final long beforeSize = allocatedMemory.getAndAdd(pageSize);

        return trace(track(ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())), TraceEvent.OverflowBufferAllocated,
            beforeCount, +1, beforeSize, +pageSize);
      }

      BufferHolder bfh = null;
      try {
        while (true) {
          // we cannot free chunk of allocated memory so we set place holder first
          // if operation successful we allocate part of direct memory.
          bfh = lastPreallocatedArea.get();
          assert bfh == null || bfh.index <= bufferIndex;

          if (bfh == null) {
            bfh = new BufferHolder(bufferIndex);

            if (lastPreallocatedArea.compareAndSet(null, bfh)) {
              allocateBuffer(bfh, allocationSize);
            } else {
              continue;
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

          if (bfh.index < bufferIndex) {
            //we have to request page only from buffer with calculated index otherwise we may use memory already allocated
            //to other page

            final int requestedPages = bfh.requested.get();
            //wait till all pending memory requests which use this buffer will be fulfilled
            if (requestedPages < maxPagesPerSingleArea) {
              try {
                bfh.filled.await();
              } catch (InterruptedException e) {
                throw OException
                    .wrapException(new OInterruptedException("Wait of new preallocated memory area was interrupted"), e);
              }
            }

            final BufferHolder bufferHolder = new BufferHolder(bufferIndex);
            if (lastPreallocatedArea.compareAndSet(bfh, bufferHolder)) {
              bfh = bufferHolder;
              allocateBuffer(bfh, allocationSize);
            } else {
              assert bufferHolder.index == bufferIndex;
              continue;
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
            slice.put(new byte[pageSize]);
          }

          slice.position(0);
          return trace(track(slice), TraceEvent.SlicedFromPreallocatedArea, rawPosition, +pageSize);
        }
      } finally {
        //we put this in final block to be sure that indication about processed memory request was for sure reflected
        //in buffer holder counter which contains amount of memory blocks already processed by given buffer
        if (bfh != null) {
          final int completedRequests = bfh.requested.incrementAndGet();
          if (completedRequests == maxPagesPerSingleArea)
            bfh.filled.countDown();
        }
      }
    }

    // this should not happen if amount of pages is needed for storage is calculated correctly
    final long beforeCount = overflowBufferCount.getAndIncrement();
    final long beforeSize = allocatedMemory.getAndAdd(pageSize);
    return trace(track(ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())), TraceEvent.FallbackBufferAllocated,
        beforeCount, +1, beforeSize, +pageSize);
  }

  /**
   * Allocates direct byte buffer for buffer holder and notifies other threads that it can be used.
   *
   * @param bfh Buffer holder for direct memory buffer to be allocated.
   */
  private void allocateBuffer(BufferHolder bfh, int allocationSize) {
    try {
      bfh.buffer = ByteBuffer.allocateDirect(allocationSize).order(ByteOrder.nativeOrder());
      final long beforeSize = allocatedMemory.getAndAdd(allocationSize);

      trace(bfh.buffer, TraceEvent.AllocatedForPreallocatedArea, beforeSize, +allocationSize);
    } finally {
      bfh.latch.countDown();
    }
  }

  /**
   * Put buffer which is not used any more back to the pool.
   *
   * @param buffer Not used instance of buffer.
   */
  public void release(ByteBuffer buffer) {
    pool.offer(untrack(buffer));

    final long beforeSize = poolSize.getAndIncrement();
    trace(buffer, TraceEvent.ReturnedToPool, beforeSize, +1);
  }

  @Override
  public int getBufferSize() {
    return pageSize;
  }

  @Override
  public long getPreAllocatedBufferCount() {
    return nextAllocationPosition.get();
  }

  @Override
  public long getOverflowBufferCount() {
    return overflowBufferCount.get();
  }

  @Override
  public int getBuffersInThePool() {
    return getSize();
  }

  @Override
  public long getAllocatedMemory() {
    return allocatedMemory.get();
  }

  @Override
  public long getAllocatedMemoryInMB() {
    return getAllocatedMemory() / (1024 * 1024);
  }

  @Override
  public double getAllocatedMemoryInGB() {
    return Math.ceil((getAllocatedMemory() * 100) / (1024.0 * 1024 * 1024)) / 100;
  }

  @Override
  public long getPreAllocationLimit() {
    return preAllocationLimit;
  }

  @Override
  public int getMaxPagesPerSingleArea() {
    return maxPagesPerSingleArea;
  }

  @Override
  public int getPoolSize() {
    return poolSize.get();
  }

  @Override
  public void startTracing() {
    setTraceEnabled(true);
  }

  @Override
  public void stopTracing() {
    setTraceEnabled(false);
  }

  /**
   * Registers the MBean for this byte buffer pool.
   *
   * @see OByteBufferPoolMXBean
   */
  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(MBEAN_NAME);

        if (!server.isRegistered(mbeanName)) {
          server.registerMBean(this, mbeanName);
        } else {
          mbeanIsRegistered.set(false);
          OLogManager.instance().warn(this,
              "MBean with name %s has already registered. Probably your system was not shutdown correctly"
                  + " or you have several running applications which use OrientDB engine inside", mbeanName.getCanonicalName());
        }

      } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
        throw OException.wrapException(new OSystemException("Error during registration of byte buffer pool MBean"), e);
      }
    }
  }

  /**
   * Unregisters the MBean for this byte buffer pool.
   *
   * @see OByteBufferPoolMXBean
   */
  public void unregisterMBean() {
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException | InstanceNotFoundException | MBeanRegistrationException e) {
        throw OException.wrapException(new OSystemException("Error during unregistration of byte buffer pool MBean"), e);
      }
    }
  }

  /**
   * Verifies the state of this byte buffer pool for a consistency, leaks, etc. {@link OGlobalConfiguration#DIRECT_MEMORY_TRACK_MODE}
   * must be on, otherwise this method does nothing. Detected problems will be printed to the error log. If assertions are on and
   * erroneous state is detected, verification will fail with {@link AssertionError} exception.
   */
  public void verifyState() {
    if (TRACK) {
      synchronized (this) {
        for (TrackedBufferReference reference : trackedReferences)
          OLogManager.instance()
              .error(this, "DIRECT-TRACK: unreleased direct memory buffer `%X` detected.", reference.stackTrace, reference.id);

        checkTrackedBuffersLeaks();

        assert trackedReferences.size() == 0;
      }
    }
  }

  /**
   * Logs all known tracking information about the given buffer. {@link OGlobalConfiguration#DIRECT_MEMORY_TRACK_MODE} must be on,
   * otherwise this method does nothing.
   *
   * @param prefix the log message prefix
   * @param buffer the buffer to print information about
   */
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  public void logTrackedBufferInfo(String prefix, ByteBuffer buffer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedBufferKey trackedBufferKey = new TrackedBufferKey(buffer);
        final TrackedBufferReference reference = trackedBuffers.get(trackedBufferKey);

        final StringBuilder builder = new StringBuilder();
        builder.append("DIRECT-TRACK: ").append(prefix).append(String.format(" buffer `%X` ", id(buffer)));

        if (reference == null)
          builder.append("untracked");
        else
          builder.append("allocated from: ").append('\n').append(getStackTraceAsString(reference.stackTrace)).append('\n');

        final Exception release = trackedReleases.get(trackedBufferKey);
        if (release != null)
          builder.append("released from: ").append('\n').append(getStackTraceAsString(release)).append('\n');

        OLogManager.instance().error(this, builder.toString());
      }
    }
  }

  private static final class BufferHolder {
    private volatile ByteBuffer buffer;
    private final CountDownLatch latch = new CountDownLatch(1);

    /**
     * Latch which is triggered when there is no any unused space in this buffer.
     */
    private final CountDownLatch filled = new CountDownLatch(1);

    /**
     * Amount of pages which were requested from this buffer. But not buffer size or index of last page in buffer.
     * <p>
     * This parameter is used to be sure that if we are going to allocate new buffer this buffer is served all page requests
     */
    private final AtomicInteger requested = new AtomicInteger();

    /**
     * Logical index of buffer.
     * <p>
     * Each buffer may contain only limited number of pages . Amount of pages is specified in {@link #maxPagesPerSingleArea}
     * parameter.
     * <p>
     * Current field is allocation number of this buffer. Let say we requested 5-th page but buffer may contain only 4 pages. So
     * first buffer will be created with index 0 and buffer which is needed for 5-th page will be created with index 1.
     * <p>
     * In general index of buffer is result of dividing of page index on number of pages which may be contained by buffer without
     * reminder.
     * <p>
     * This index is used to check that buffer which is currently used to serve memory requests is one which should be used for page
     * with given index.
     */

    private final int index;

    BufferHolder(int index) {
      this.index = index;
    }

  }

  private ByteBuffer trace(ByteBuffer buffer, TraceEvent event, long... stats) {
    if (traceEnabled) {
      if (directBufferPoolMxBean != null) {
        final long eventAllocatedMemory = event.allocatedMemory(stats);
        final long allocatedMemory = eventAllocatedMemory == -1 ? getAllocatedMemory() : eventAllocatedMemory;
        final long allocatedMemoryDelta = event.allocatedMemoryDelta(stats);
        final long expectedDirectMemorySizeAfter = expectedDirectMemorySize.addAndGet(allocatedMemoryDelta);
        final long actualDirectMemorySize = directBufferPoolMxBean.getMemoryUsed();

        if (actualDirectMemorySize != expectedDirectMemorySizeAfter)
          if (traceAggregation == TraceAggregation.None)
            traceDirectBufferPoolSizeChanged(allocatedMemory, expectedDirectMemorySizeAfter, actualDirectMemorySize);
          else {
            final long aggregatedEvents = directMemoryEventCounter.getAndIncrement();

            if (aggregatedEvents == 0)
              traceDirectBufferPoolSizeChanged(allocatedMemory, expectedDirectMemorySizeAfter, actualDirectMemorySize);

            if (aggregatedEvents >= traceAggregation.threshold - 1)
              directMemoryEventCounter.set(0);
          }
      }

      if (traceAggregation == TraceAggregation.None || !event.aggregate)
        OLogManager.instance().log(this, event.level, "DIRECT-TRACE %s: %s, buffer = %X", null, event.tag(), event.describe(stats),
            System.identityHashCode(buffer));
      else {
        // select the event's aggregated stats
        final AtomicLongArray aggregatedStats = aggregatedTraceStats[event.ordinal()];

        // select the aggregated stats of the inverse event, if any
        final AtomicLongArray inverseAggregatedStats =
            event.inverse() == null ? null : aggregatedTraceStats[event.inverse().ordinal()];

        // aggregate the deltas
        for (int i = 0; i < stats.length / 2; ++i)
          aggregatedStats.getAndAdd(i, stats[i * 2 + 1]);

        // count the event
        final long eventsAggregated = aggregatedStats.incrementAndGet(event.statsSize / 2);

        // log the stats if the threshold is reached
        if (inverseAggregatedStats == null) {
          if (eventsAggregated >= traceAggregation.threshold) {
            OLogManager.instance().log(this, event.level, "DIRECT-TRACE %s x %d: %s", null, event.tag(), eventsAggregated,
                event.describe(aggregatedStats, null, stats));

            // reset the aggregated stats
            for (int i = 0; i < aggregatedStats.length(); ++i)
              aggregatedStats.set(i, 0);
          }
        } else {
          final long inverseEventsAggregated = inverseAggregatedStats.get(event.statsSize / 2);

          // log the stats if the threshold is reached
          if (eventsAggregated + inverseEventsAggregated >= traceAggregation.threshold) {
            OLogManager.instance().log(this, event.level, "DIRECT-TRACE %s/%s: %s", null, event.tag(), event.inverse().tag(),
                event.describe(aggregatedStats, inverseAggregatedStats, stats));

            // reset the aggregated stats
            assert aggregatedStats.length() == inverseAggregatedStats.length();
            for (int i = 0; i < aggregatedStats.length(); ++i) {
              aggregatedStats.set(i, 0);
              inverseAggregatedStats.set(i, 0);
            }
          }
        }
      }
    }

    return buffer;
  }

  private void traceDirectBufferPoolSizeChanged(long allocated, long expected, long actual) {
    final long delta = actual - expected;
    final long excess = actual - allocated;

    OLogManager.instance().warn(this, "DIRECT-TRACE DirectBufferPoolSizeChanged: JVM allocated memory ~ %s (~%s) + %s ~ %s (%c%s)",
        TraceEvent.memory(allocated), TraceEvent.percentage(allocated, actual), TraceEvent.memory(excess),
        TraceEvent.memory(actual), TraceEvent.sign(delta), TraceEvent.memory(Math.abs(delta)));

    expectedDirectMemorySize.set(actual);
  }

  private void resetTracing() {
    for (AtomicLongArray stats : aggregatedTraceStats)
      if (stats != null) // stats maybe absent if an event type doesn't support aggregation
        for (int i = 0; i < stats.length(); ++i)
          stats.set(i, 0);

    expectedDirectMemorySize.set(getAllocatedMemory());
    directMemoryEventCounter.set(0);
  }

  private ByteBuffer track(ByteBuffer buffer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedBufferReference reference = new TrackedBufferReference(buffer, trackedBuffersQueue);
        trackedReferences.add(reference);
        trackedBuffers.put(new TrackedBufferKey(buffer), reference);

        checkTrackedBuffersLeaks();
      }
    }

    return buffer;
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private ByteBuffer untrack(ByteBuffer buffer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedBufferKey trackedBufferKey = new TrackedBufferKey(buffer);

        final TrackedBufferReference reference = trackedBuffers.remove(trackedBufferKey);
        if (reference == null) {
          OLogManager.instance()
              .error(this, "DIRECT-TRACK: untracked direct byte buffer `%X` detected.", new Exception(), id(buffer));

          final Exception lastRelease = trackedReleases.get(trackedBufferKey);
          if (lastRelease != null)
            OLogManager.instance().error(this, "DIRECT-TRACK: last release.", lastRelease);

          assert false;
        } else
          trackedReferences.remove(reference);

        trackedReleases.put(trackedBufferKey, new Exception());

        checkTrackedBuffersLeaks();
      }
    }

    return buffer;
  }

  private void checkTrackedBuffersLeaks() {
    boolean leaked = false;

    TrackedBufferReference reference;
    while ((reference = (TrackedBufferReference) trackedBuffersQueue.poll()) != null) {
      if (trackedReferences.remove(reference)) {
        OLogManager.instance()
            .error(this, "DIRECT-TRACK: unreleased direct byte buffer `%X` detected.", reference.stackTrace, reference.id);
        leaked = true;
      }
    }

    assert !leaked;
  }

  private static int id(Object object) {
    return System.identityHashCode(object);
  }

  private static String getStackTraceAsString(Throwable throwable) {
    final StringWriter writer = new StringWriter();
    throwable.printStackTrace(new PrintWriter(writer));
    return writer.toString();
  }

  private static class TrackedBufferReference extends WeakReference<ByteBuffer> {

    public final int       id;
    final        Exception stackTrace;

    TrackedBufferReference(ByteBuffer referent, ReferenceQueue<? super ByteBuffer> q) {
      super(referent, q);

      this.id = id(referent);
      this.stackTrace = new Exception();
    }

  }

  private static class TrackedBufferKey extends WeakReference<ByteBuffer> {

    private final int hashCode;

    TrackedBufferKey(ByteBuffer referent) {
      super(referent);
      hashCode = System.identityHashCode(referent);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
      final ByteBuffer buffer = get();
      return buffer != null && buffer == ((TrackedBufferKey) obj).get();
    }

  }

  private static class InstanceHolder {
    private static final OByteBufferPool INSTANCE;

    static {
      // page size in bytes
      final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

      // Maximum amount of chunk size which should be allocated at once by system
      final int memoryChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsInteger();

      final long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsInteger() * 1024L * 1024L;

      // instance of byte buffer which should be used by all storage components
      INSTANCE = new OByteBufferPool(pageSize, memoryChunkSize, diskCacheSize);
    }
  }

  /**
   * Controls the aggregation level of the direct memory tracer.
   */
  public enum TraceAggregation {
    /**
     * No aggregation, events traced one-by-one.
     */
    None(1),

    /**
     * Low aggregation level.
     */
    Low(100),

    /**
     * Medium aggregation level.
     */
    Medium(1000),

    /**
     * High aggregation level.
     */
    High(10000);

    private final long threshold;

    TraceAggregation(long threshold) {
      this.threshold = threshold;
    }
  }

  private enum TraceEvent {
    AcquiredFromPool(Level.INFO, true, 2) {
      @Override
      public TraceEvent inverse() {
        return ReturnedToPool;
      }

      @Override
      public String describe(long... stats) {
        return formatCount("pool size", stats[0], stats[1]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        return formatCount("pool size", stats[0], stats[1], aggregatedStats.get(0), inverseAggregatedStats.get(0));
      }
    },

    OverflowBufferAllocated(Level.WARNING, true, 4) {
      @Override
      public long allocatedMemory(long... stats) {
        return stats[2];
      }

      @Override
      public long allocatedMemoryDelta(long... stats) {
        return stats[3];
      }

      @Override
      public String describe(long... stats) {
        return formatCount("overflow buffer count", stats[0], stats[1]) + ", " + formatMemory("allocated memory", stats[2],
            stats[3]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        return formatCount("overflow buffer count", stats[0], stats[1], aggregatedStats.get(0)) + ", " + formatMemory(
            "allocated memory", stats[2], stats[3], aggregatedStats.get(1));
      }
    },

    SlicedFromPreallocatedArea(Level.INFO, true, 2) {
      @Override
      public String describe(long... stats) {
        return formatMemory("sliced memory", stats[0], stats[1]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        return formatMemory("sliced memory", stats[0], stats[1], aggregatedStats.get(0));
      }
    },

    FallbackBufferAllocated(Level.WARNING, true, 4) {
      @Override
      public long allocatedMemory(long... stats) {
        return stats[2];
      }

      @Override
      public long allocatedMemoryDelta(long... stats) {
        return stats[3];
      }

      @Override
      public String describe(long... stats) {
        return formatCount("overflow buffer count", stats[0], stats[1]) + ", " + formatMemory("allocated memory", stats[2],
            stats[3]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        return formatCount("overflow buffer count", stats[0], stats[1], aggregatedStats.get(0)) + ", " + formatMemory(
            "allocated memory", stats[2], stats[3], aggregatedStats.get(1));
      }
    },

    AllocatedForPreallocatedArea(Level.INFO, false, 2) {
      @Override
      public long allocatedMemory(long... stats) {
        return stats[0];
      }

      @Override
      public long allocatedMemoryDelta(long... stats) {
        return stats[1];
      }

      @Override
      public String describe(long... stats) {
        return formatMemory("allocated memory", stats[0], stats[1]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        throw new UnsupportedOperationException("aggregation is not supported");
      }
    },

    ReturnedToPool(Level.INFO, true, 2) {
      @Override
      public TraceEvent inverse() {
        return AcquiredFromPool;
      }

      @Override
      public String describe(long... stats) {
        return formatCount("pool size", stats[0], stats[1]);
      }

      @Override
      public String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats) {
        return formatCount("pool size", stats[0], stats[1], aggregatedStats.get(0), inverseAggregatedStats.get(0));
      }
    };

    public final Level   level;
    public final boolean aggregate;
    public final int     statsSize;

    TraceEvent(Level level, boolean aggregate, int statsSize) {
      this.level = level;
      this.aggregate = aggregate;
      this.statsSize = statsSize;
    }

    public String tag() {
      return this.name();
    }

    public TraceEvent inverse() {
      return null;
    }

    public long allocatedMemory(long... stats) {
      return -1;
    }

    public long allocatedMemoryDelta(long... stats) {
      return 0;
    }

    public abstract String describe(long... stats);

    public abstract String describe(AtomicLongArray aggregatedStats, AtomicLongArray inverseAggregatedStats, long... stats);

    private static String formatCount(String label, long before, long delta) {
      return String.format("%s = %d %c %d = %d", label, before, sign(delta), Math.abs(delta), before + delta);
    }

    private static String formatCount(String label, long before, long delta, long aggregatedDelta) {
      return String.format("%s ~ %d (%c%d)", label, before + delta, sign(aggregatedDelta), Math.abs(aggregatedDelta));
    }

    private static String formatCount(String label, long before, long delta, long aggregatedDelta, long inverseAggregatedDelta) {
      return String.format("%s ~ %d (%c%d/%c%d)", label, before + delta, sign(aggregatedDelta), Math.abs(aggregatedDelta),
          sign(inverseAggregatedDelta), Math.abs(inverseAggregatedDelta));
    }

    private static String formatMemory(String label, long before, long delta) {
      return String
          .format("%s = %s %c %s = %s", label, memory(before), sign(delta), memory(Math.abs(delta)), memory(before + delta));
    }

    private static String formatMemory(String label, long before, long delta, long aggregatedDelta) {
      return String
          .format("%s ~ %s (%c%s)", label, memory(before + delta), sign(aggregatedDelta), memory(Math.abs(aggregatedDelta)));
    }

    private static char sign(long value) {
      return value < 0 ? '-' : '+';
    }

    private static String memory(long value) {
      if (value == 0)
        return "0B";

      if (value % (1024 * 1024 * 1024) == 0)
        return Long.toString(value / (1024 * 1024 * 1024)) + "GB";

      if (value % (1024 * 1024) == 0)
        return Long.toString(value / (1024 * 1024)) + "MB";

      if (value % 1024 == 0)
        return Long.toString(value / 1024) + "KB";

      return Long.toString(value) + "B";
    }

    private static String percentage(long part, long whole) {
      return Math.floor((double) part / whole * 100.0 * 100.0) / 100.0 + "%";
    }
  }

}
