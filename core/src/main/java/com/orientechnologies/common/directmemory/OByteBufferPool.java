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
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import javax.management.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogManager;

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
public class OByteBufferPool implements OOrientStartupListener, OOrientShutdownListener, OByteBufferPoolMXBean {
  /**
   * {@link OByteBufferPool}'s MBean name.
   */
  public static final String MBEAN_NAME = "com.orientechnologies.common.directmemory:type=OByteBufferPoolMXBean";

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
   * Page which is filled with zeros and used to speedup clear operation on page acquire operation {@link #acquireDirect(boolean)}.
   */
  private ByteBuffer zeroPage = null;

  /**
   * Map which contains collection of preallocated chunks and their indexes. Indexes are numbered in order of their allocation.
   */
  private final ConcurrentHashMap<Integer, BufferHolder> preallocatedAreas = new ConcurrentHashMap<Integer, BufferHolder>();

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
  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<ByteBuffer>();

  /**
   * Tracks the number of the overflow buffer allocations.
   */
  private final AtomicLong overflowBufferCount = new AtomicLong();

  /**
   * Tracks the status of the MBean registration.
   */
  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  private final ReferenceQueue<ByteBuffer>                    trackedBuffersQueue;
  private final Set<TrackedBufferReference>                   trackedReferences;
  private final Map<TrackedBufferKey, TrackedBufferReference> trackedBuffers;
  private final Map<TrackedBufferKey, Exception>              trackedReleases;

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
    this.zeroPage = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());

    this.preAllocationLimit = (preAllocationLimit / pageSize) * pageSize;

    int pagesPerArea = (maxChunkSize / pageSize);
    if (pagesPerArea > 1) {
      pagesPerArea = closestPowerOfTwo(pagesPerArea);

      // we need not the biggest value, it may cause buffer overflow, but biggest after that.
      while ((long) pagesPerArea * pageSize >= maxChunkSize) {
        pagesPerArea = pagesPerArea >>> 1;
      }

      maxPagesPerSingleArea = pagesPerArea;
    } else {
      maxPagesPerSingleArea = 1;
    }

    if (TRACK) {
      trackedBuffersQueue = new ReferenceQueue<ByteBuffer>();
      trackedReferences = new HashSet<TrackedBufferReference>();
      trackedBuffers = new HashMap<TrackedBufferKey, TrackedBufferReference>();
      trackedReleases = new HashMap<TrackedBufferKey, Exception>();
    } else {
      trackedBuffersQueue = null;
      trackedReferences = null;
      trackedBuffers = null;
      trackedReleases = null;
    }

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
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
   * memory chunk is allocated from direct memory or slice of already preallocated memory chunk is used as new byte buffer instance.
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
      if (clear) {
        buffer.position(0);
        buffer.put(zeroPage.duplicate());
      }

      buffer.position(0);

      return trackBuffer(buffer);
    }

    if (maxPagesPerSingleArea > 1) {
      final long currentAllocationPosition = nextAllocationPosition.getAndIncrement();

      //all chucks consumes maxPagesPerSingleArea space with exception of last one
      final int position = (int) (currentAllocationPosition & (maxPagesPerSingleArea - 1));
      final int bufferIndex = (int) (currentAllocationPosition / maxPagesPerSingleArea);

      //if we hit the end of preallocation buffer we allocate by small chunks
      if (currentAllocationPosition >= preAllocationLimit) {
        return trackBuffer(ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));
      }

      //allocation size should be the same for all buffers from chuck with the same index
      final int allocationSize = (int) Math
          .min(maxPagesPerSingleArea * pageSize, (preAllocationLimit - bufferIndex * maxPagesPerSingleArea) * pageSize);

      // we cannot free chunk of allocated memory so we set place holder first
      // if operation successful we allocate part of direct memory.
      BufferHolder bfh = preallocatedAreas.get(bufferIndex);

      if (bfh == null) {
        bfh = new BufferHolder();

        BufferHolder replacedBufferHolder = preallocatedAreas.putIfAbsent(bufferIndex, bfh);

        if (replacedBufferHolder == null) {
          allocateBuffer(bfh, allocationSize);
        } else {
          bfh = replacedBufferHolder;
        }
      }

      if (bfh.buffer == null) {
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
      return trackBuffer(slice);
    }

    // this should not happen if amount of pages is needed for storage is calculated correctly
    overflowBufferCount.incrementAndGet();
    return trackBuffer(ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));
  }

  /**
   * Allocates direct byte buffer for buffer holder and notifies other threads that it can be used.
   *
   * @param bfh Buffer holder for direct memory buffer to be allocated.
   */
  private void allocateBuffer(BufferHolder bfh, int allocationSize) {
    try {
      bfh.buffer = ByteBuffer.allocateDirect(allocationSize).order(ByteOrder.nativeOrder());
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
    pool.offer(untrackBuffer(buffer));
  }

  @Override
  public int getBufferSize() {
    return pageSize;
  }

  @Override
  public long getAllocatedBufferCount() {
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
    long memory = getOverflowBufferCount();

    final long allocatedAreas = (getAllocatedBufferCount() + maxPagesPerSingleArea - 1) / maxPagesPerSingleArea;
    memory += allocatedAreas * maxPagesPerSingleArea;

    return memory * pageSize;
  }

  @Override
  public long getAllocatedMemoryInMB() {
    return getAllocatedMemory() / (1024 * 1024);
  }

  @Override
  public double getAllocatedMemoryInGB() {
    return Math.ceil((getAllocatedMemory() * 100) / (1024.0 * 1024 * 1024)) / 100;
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

      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OSystemException("Error during registration of byte buffer pool MBean"), e);
      } catch (InstanceAlreadyExistsException e) {
        throw OException.wrapException(new OSystemException("Error during registration of byte buffer pool MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OSystemException("Error during registration of byte buffer pool MBean"), e);
      } catch (NotCompliantMBeanException e) {
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
      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OSystemException("Error during unregistration of byte buffer pool MBean"), e);
      } catch (InstanceNotFoundException e) {
        throw OException.wrapException(new OSystemException("Error during unregistration of byte buffer pool MBean"), e);
      } catch (MBeanRegistrationException e) {
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
        final boolean logsInAssertions = logInAssertion();
        final StringBuilder builder = logsInAssertions ? new StringBuilder() : null;

        for (TrackedBufferReference reference : trackedReferences)
          log(builder, this, "DIRECT-TRACK: unreleased direct memory buffer `%X` detected.", reference.stackTrace, reference.id);

        checkTrackedBuffersLeaks(builder);

        if (logsInAssertions) {
          if (builder.length() > 0)
            throw new AssertionError(builder.toString());
        } else
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

  @Override
  public void onStartup() {
    if (this.zeroPage != null) // already/still started?
      return;

    this.zeroPage = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  @Override
  public void onShutdown() {
    if (zeroPage == null) // already/still shutdown?
      return;

    final Set<ByteBuffer> cleaned = Collections.newSetFromMap(new IdentityHashMap<ByteBuffer, Boolean>());
    try {
      clean(zeroPage, cleaned);
      for (ByteBuffer byteBuffer : pool)
        clean(byteBuffer, cleaned);
    } catch (Throwable t) {
      return;
    }

    this.zeroPage = null;

    if (this.preallocatedAreas != null) {
      for (BufferHolder bufferHolder : preallocatedAreas.values()) {
        clean(bufferHolder.buffer, cleaned);
      }
      this.preallocatedAreas.clear();
    }

    nextAllocationPosition.set(0);
    pool.clear();
    overflowBufferCount.set(0);

    if (TRACK) {
      for (TrackedBufferReference reference : trackedReferences)
        reference.clear();
      trackedReferences.clear();
      trackedBuffers.clear();
      trackedReleases.clear();

      //noinspection StatementWithEmptyBody
      while (trackedBuffersQueue.poll() != null)
        ;
    }
  }

  private void clean(ByteBuffer buffer, Set<ByteBuffer> cleaned) {
    final ByteBuffer directByteBufferWithCleaner = findDirectByteBufferWithCleaner(buffer, 16);
    if (directByteBufferWithCleaner != null && !cleaned.contains(directByteBufferWithCleaner)) {
      cleaned.add(directByteBufferWithCleaner);
      ((DirectBuffer) directByteBufferWithCleaner).cleaner().clean();
      if (TRACK)
        OLogManager.instance().info(this, "DIRECT-TRACK: cleaned " + directByteBufferWithCleaner);
    }
  }

  private static ByteBuffer findDirectByteBufferWithCleaner(ByteBuffer buffer, int depthLimit) {
    if (depthLimit == 0)
      return null;

    if (!(buffer instanceof DirectBuffer))
      return null;
    final DirectBuffer directBuffer = (DirectBuffer) buffer;

    final Cleaner cleaner = directBuffer.cleaner();
    if (cleaner != null)
      return buffer;

    final Object attachment = directBuffer.attachment();
    if (!(attachment instanceof ByteBuffer))
      return null;

    return findDirectByteBufferWithCleaner((ByteBuffer) attachment, depthLimit - 1);
  }

  private static final class BufferHolder {
    private volatile ByteBuffer buffer;
    private final CountDownLatch latch = new CountDownLatch(1);
  }

  private ByteBuffer trackBuffer(ByteBuffer buffer) {
    if (TRACK) {
      synchronized (this) {
        final boolean logInAssertion = logInAssertion();
        final StringBuilder logBuilder = logInAssertion ? new StringBuilder() : null;

        final TrackedBufferReference reference = new TrackedBufferReference(buffer, trackedBuffersQueue);
        trackedReferences.add(reference);
        trackedBuffers.put(new TrackedBufferKey(buffer), reference);

        checkTrackedBuffersLeaks(logBuilder);

        if (logInAssertion && logBuilder.length() > 0)
          throw new AssertionError(logBuilder.toString());
      }
    }

    return buffer;
  }

  @SuppressWarnings({ "ThrowableResultOfMethodCallIgnored" })
  private ByteBuffer untrackBuffer(ByteBuffer buffer) {
    if (TRACK) {
      synchronized (this) {
        final boolean logInAssertion = logInAssertion();
        final StringBuilder logBuilder = logInAssertion ? new StringBuilder() : null;

        final TrackedBufferKey trackedBufferKey = new TrackedBufferKey(buffer);

        final TrackedBufferReference reference = trackedBuffers.remove(trackedBufferKey);
        if (reference == null) {
          log(logBuilder, this, "DIRECT-TRACK: untracked direct byte buffer `%X` detected.", new Exception(), id(buffer));

          final Exception lastRelease = trackedReleases.get(trackedBufferKey);
          if (lastRelease != null)
            log(logBuilder, this, "DIRECT-TRACK: last release.", lastRelease);

          if (logInAssertion) {
            if (logBuilder.length() > 0)
              throw new AssertionError(logBuilder.toString());
          } else
            assert false;
        } else
          trackedReferences.remove(reference);

        trackedReleases.put(trackedBufferKey, new Exception());

        checkTrackedBuffersLeaks(logBuilder);
        if (logInAssertion && logBuilder.length() > 0)
          throw new AssertionError(logBuilder.toString());
      }
    }

    return buffer;
  }

  private void checkTrackedBuffersLeaks(StringBuilder logBuilder) {
    boolean leaked = false;

    TrackedBufferReference reference;
    while ((reference = (TrackedBufferReference) trackedBuffersQueue.poll()) != null) {
      if (trackedReferences.remove(reference)) {
        log(logBuilder, this, "DIRECT-TRACK: unreleased direct byte buffer `%X` detected.", reference.stackTrace, reference.id);
        leaked = true;
      }
    }

    if (logBuilder == null)
      assert !leaked;
  }

  private static int id(Object object) {
    return System.identityHashCode(object);
  }

  @SuppressWarnings({ "AssertWithSideEffects", "ConstantConditions" })
  private static boolean logInAssertion() {
    boolean assertionsEnabled = false;
    assert assertionsEnabled = true;
    return assertionsEnabled && !(LogManager.getLogManager() instanceof OLogManager.DebugLogManager);
  }

  private static void log(StringBuilder builder, final Object from, final String message, final Throwable exception,
      final Object... args) {
    if (builder == null)
      OLogManager.instance().error(from, message, exception, args);
    else {
      final String newLine = String.format("%n");
      builder.append(String.format(message, args)).append(newLine);
      if (exception != null) {
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(stringWriter);
        exception.printStackTrace(printWriter);
        builder.append(stringWriter.toString());
      }
    }
  }

  private static String getStackTraceAsString(Throwable throwable) {
    final StringWriter writer = new StringWriter();
    throwable.printStackTrace(new PrintWriter(writer));
    return writer.toString();
  }

  private static class TrackedBufferReference extends WeakReference<ByteBuffer> {

    public final int       id;
    public final Exception stackTrace;

    public TrackedBufferReference(ByteBuffer referent, ReferenceQueue<? super ByteBuffer> q) {
      super(referent, q);

      this.id = id(referent);
      this.stackTrace = new Exception();
    }

  }

  private static class TrackedBufferKey extends WeakReference<ByteBuffer> {

    private final int hashCode;

    public TrackedBufferKey(ByteBuffer referent) {
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

}
