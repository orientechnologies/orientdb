/*
 *
 *  *  Copyright 2010-2018 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.directmemory;

import com.kenai.jffi.MemoryIO;
import com.orientechnologies.common.exception.ODirectMemoryAllocationFailedException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.types.OModifiableLong;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import sun.misc.Unsafe;

/**
 * Manages all allocations/deallocations from/to direct memory. Also tracks the presence of memory
 * leaks.
 *
 * @see OGlobalConfiguration#DIRECT_MEMORY_POOL_LIMIT
 */
public class ODirectMemoryAllocator implements ODirectMemoryAllocatorMXBean {

  private static final Unsafe unsafe;

  static {
    unsafe =
        (Unsafe)
            AccessController.doPrivileged(
                (PrivilegedAction<Object>)
                    () -> {
                      try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                      } catch (NoSuchFieldException | IllegalAccessException e) {
                        return null;
                      }
                    });
  }

  private static final boolean PROFILE_MEMORY =
      OGlobalConfiguration.MEMORY_PROFILING.getValueAsBoolean();

  private static final int MEMORY_STATISTICS_PRINTING_INTERVAL =
      OGlobalConfiguration.MEMORY_PROFILING_REPORT_INTERVAL.getValueAsInteger();

  /** Whether we should track memory leaks during application execution */
  private static final boolean TRACK =
      OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();

  /** Holder for singleton instance */
  private static final AtomicReference<ODirectMemoryAllocator> INSTANCE_HOLDER =
      new AtomicReference<>();

  /**
   * Reference queue for all created direct memory pointers. During check of memory leaks we access
   * this queue to check whether we have leaked direct memory pointers.
   */
  private final ReferenceQueue<OPointer> trackedPointersQueue;

  /**
   * WeakReference to the allocated pointer. We use those references to track stack traces where
   * those pointers were allocated. Even if reference to the pointer will be collected we still will
   * have information where it was allocated and also presence of this pointers into the queue
   * during OrientDB engine shutdown indicates that direct memory was not released back and there
   * are memory leaks in application.
   */
  private final Set<TrackedPointerReference> trackedReferences;

  /**
   * Map between pointers and soft references which are used for tracking of memory leaks. Key
   * itself is a weak reference but we can not use only single weak reference collection because
   * identity of key equals to identity of pointer and identity of reference is based on comparision
   * of instances of objects. The last one is used during memory leak detection when we find
   * references in the reference queue and try to check whether those references were tracked during
   * pointer allocation or not.
   */
  private final Map<TrackedPointerKey, TrackedPointerReference> trackedBuffers;

  /** Amount of direct memory consumed by using this allocator. */
  private final LongAdder memoryConsumption = new LongAdder();

  private final ThreadLocal<EnumMap<Intention, OModifiableLong>> memoryConsumptionByIntention =
      ThreadLocal.withInitial(() -> new EnumMap<>(Intention.class));

  private final Set<ConsumptionMapEvictionIndicator> consumptionMaps =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final ReferenceQueue<Thread> consumptionMapEvictionQueue = new ReferenceQueue<>();

  /** @return singleton instance. */
  public static ODirectMemoryAllocator instance() {
    final ODirectMemoryAllocator inst = INSTANCE_HOLDER.get();
    if (inst != null) {
      return inst;
    }

    final ODirectMemoryAllocator newAllocator = new ODirectMemoryAllocator();
    if (INSTANCE_HOLDER.compareAndSet(null, newAllocator)) {
      return newAllocator;
    }

    return INSTANCE_HOLDER.get();
  }

  public ODirectMemoryAllocator() {
    trackedPointersQueue = new ReferenceQueue<>();
    trackedReferences = new HashSet<>();
    trackedBuffers = new HashMap<>();

    if (PROFILE_MEMORY) {
      final long printInterval = (long) MEMORY_STATISTICS_PRINTING_INTERVAL * 60 * 1_000;
      Orient.instance()
          .scheduleTask(new MemoryStatPrinter(consumptionMaps), printInterval, printInterval);
    }
  }

  /**
   * Allocates chunk of direct memory of given size.
   *
   * @param size Amount of memory to allocate
   * @param clear clears memory if needed
   * @param intention Why this memory is allocated. This parameter is used for memory profiling.
   * @return Pointer to allocated memory
   * @throws ODirectMemoryAllocationFailedException if it is impossible to allocate amount of direct
   *     memory of given size
   */
  public OPointer allocate(int size, boolean clear, Intention intention) {
    if (size <= 0) {
      throw new IllegalArgumentException("Size of allocated memory can not be less or equal to 0");
    }

    final OPointer ptr;

    final long pointer;
    if (unsafe == null) {
      pointer = MemoryIO.getInstance().allocateMemory(size, clear);
    } else {
      pointer = unsafe.allocateMemory(size);
      if (clear) {
        unsafe.setMemory(pointer, size, (byte) 0);
      }
    }

    if (pointer <= 0) {
      throw new ODirectMemoryAllocationFailedException(
          "Can not allocate direct memory chunk of size " + size);
    }

    ptr = new OPointer(pointer, size, intention);

    memoryConsumption.add(size);
    if (PROFILE_MEMORY) {
      final EnumMap<Intention, OModifiableLong> consumptionMap = memoryConsumptionByIntention.get();

      if (consumptionMap.isEmpty()) {
        consumptionMaps.add(
            new ConsumptionMapEvictionIndicator(
                Thread.currentThread(), consumptionMapEvictionQueue, consumptionMap));
      }

      accumulateEvictedConsumptionMaps(consumptionMap);

      consumptionMap.compute(
          intention,
          (k, v) -> {
            if (v == null) {
              return new OModifiableLong(size);
            }

            v.value += size;
            return v;
          });
    }

    return track(ptr);
  }

  private void accumulateEvictedConsumptionMaps(
      EnumMap<Intention, OModifiableLong> consumptionMap) {
    ConsumptionMapEvictionIndicator evictionIndicator =
        (ConsumptionMapEvictionIndicator) consumptionMapEvictionQueue.poll();
    while (evictionIndicator != null) {
      consumptionMaps.remove(evictionIndicator);
      accumulateConsumptionStatistics(consumptionMap, evictionIndicator);

      evictionIndicator = (ConsumptionMapEvictionIndicator) consumptionMapEvictionQueue.poll();
    }
  }

  /** Returns allocated direct memory back to OS */
  public void deallocate(OPointer pointer) {
    if (pointer == null) {
      throw new IllegalArgumentException("Null value is passed");
    }

    final long ptr = pointer.getNativePointer();
    if (ptr > 0) {

      if (unsafe != null) {
        unsafe.freeMemory(ptr);
      } else {
        MemoryIO.getInstance().freeMemory(ptr);
      }

      memoryConsumption.add(-pointer.getSize());

      if (PROFILE_MEMORY) {
        final EnumMap<Intention, OModifiableLong> consumptionMap =
            memoryConsumptionByIntention.get();

        final boolean wasEmpty = consumptionMap.isEmpty();

        accumulateEvictedConsumptionMaps(consumptionMap);

        consumptionMap.compute(
            pointer.getIntention(),
            (k, v) -> {
              if (v == null) {
                return new OModifiableLong(-pointer.getSize());
              }

              v.value -= pointer.getSize();
              return v;
            });

        if (wasEmpty) {
          consumptionMaps.add(
              new ConsumptionMapEvictionIndicator(
                  Thread.currentThread(), consumptionMapEvictionQueue, consumptionMap));
        }
      }

      untrack(pointer);
    }
  }

  private static String printMemoryStatistics(
      final EnumMap<Intention, OModifiableLong> memoryConsumptionByIntention) {
    long total = 0;
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(
        "\r\n-----------------------------------------------------------------------------\r\n");
    stringBuilder.append("Memory profiling results for OrientDB direct memory allocation\r\n");
    stringBuilder.append("Amount of memory consumed by category in bytes/Kb/Mb/Gb\r\n");
    stringBuilder.append("\r\n");

    for (final Intention intention : Intention.values()) {
      final OModifiableLong consumedMemory = memoryConsumptionByIntention.get(intention);
      stringBuilder.append(intention.name()).append(" : ");
      if (consumedMemory != null) {
        total += consumedMemory.value;

        stringBuilder
            .append(consumedMemory.value)
            .append("/")
            .append(consumedMemory.value / 1024)
            .append("/")
            .append(consumedMemory.value / (1024 * 1024))
            .append("/")
            .append(consumedMemory.value / (1024 * 1024 * 1024));
      } else {
        stringBuilder.append("0/0/0/0");
      }
      stringBuilder.append("\r\n");
    }

    stringBuilder.append("\r\n");

    stringBuilder
        .append("Total : ")
        .append(total)
        .append("/")
        .append(total / 1024)
        .append("/")
        .append(total / (1024 * 1024))
        .append("/")
        .append(total / (1024 * 1024 * 1024));
    stringBuilder.append(
        "\r\n-----------------------------------------------------------------------------\r\n");
    return stringBuilder.toString();
  }

  /** @inheritDoc */
  @Override
  public long getMemoryConsumption() {
    return memoryConsumption.longValue();
  }

  /** Verifies that all pointers which were allocated by allocator are freed. */
  public void checkMemoryLeaks() {
    if (TRACK) {
      final long memCons = memoryConsumption.longValue();

      if (memCons > 0) {
        OLogManager.instance()
            .warnNoDb(
                this,
                "DIRECT-TRACK: memory consumption is not zero (%d bytes), it may indicate presence of memory leaks",
                memCons);

        assert false;
      }
      synchronized (this) {
        for (TrackedPointerReference reference : trackedReferences)
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "DIRECT-TRACK: unreleased direct memory pointer `%X` detected.",
                  reference.stackTrace,
                  reference.id);

        checkTrackedPointerLeaks();

        assert trackedReferences.size() == 0;
      }
    }
  }

  /**
   * Adds pointer to the containers of weak references so we will be able to find memory leaks
   * related to this pointer
   */
  private OPointer track(OPointer pointer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedPointerReference reference =
            new TrackedPointerReference(pointer, trackedPointersQueue);
        trackedReferences.add(reference);
        trackedBuffers.put(new TrackedPointerKey(pointer), reference);
      }
    }

    return pointer;
  }

  /** Checks reference queue to find direct memory leaks */
  public void checkTrackedPointerLeaks() {
    boolean leaked = false;

    TrackedPointerReference reference;
    while ((reference = (TrackedPointerReference) trackedPointersQueue.poll()) != null) {
      if (trackedReferences.remove(reference)) {
        OLogManager.instance()
            .errorNoDb(
                this,
                "DIRECT-TRACK: unreleased direct memory pointer `%X` detected.",
                reference.stackTrace,
                reference.id);
        leaked = true;
      }
    }

    assert !leaked;
  }

  /**
   * Removes direct memory pointer from container of weak references, it is done just after memory
   * which was referenced by this pointer will be deallocated. So no memory leaks can be caused by
   * this pointer.
   */
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private void untrack(OPointer pointer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedPointerKey trackedBufferKey = new TrackedPointerKey(pointer);

        final TrackedPointerReference reference = trackedBuffers.remove(trackedBufferKey);
        if (reference == null) {
          OLogManager.instance()
              .errorNoDb(
                  this,
                  "DIRECT-TRACK: untracked direct memory pointer `%X` detected.",
                  new Exception(),
                  id(pointer));

          assert false;
        } else {
          trackedReferences.remove(reference);
          reference.clear();
        }
      }
    }
  }

  public enum Intention {
    TEST,
    PAGE_PRE_ALLOCATION,
    ADD_NEW_PAGE_IN_DISK_CACHE,
    CHECK_FILE_STORAGE,
    LOAD_PAGE_FROM_DISK,
    COPY_PAGE_DURING_FLUSH,
    COPY_PAGE_DURING_EXCLUSIVE_PAGE_FLUSH,
    FILE_FLUSH,
    LOAD_WAL_PAGE,
    ADD_NEW_PAGE_IN_MEMORY_STORAGE,
    ALLOCATE_CHUNK_TO_WRITE_DATA_IN_BATCH,
    DWL_ALLOCATE_CHUNK,
    DWL_ALLOCATE_COMPRESSED_CHUNK,
    ALLOCATE_FIRST_WAL_BUFFER,
    ALLOCATE_SECOND_WAL_BUFFER,
  }

  /**
   * WeakReference to the direct memory pointer which tracks stack trace of allocation of direct
   * memory associated with this pointer.
   */
  private static class TrackedPointerReference extends WeakReference<OPointer> {

    public final int id;
    private final Exception stackTrace;

    TrackedPointerReference(OPointer referent, ReferenceQueue<? super OPointer> q) {
      super(referent, q);

      this.id = id(referent);
      this.stackTrace = new Exception();
    }
  }

  /**
   * WeakReference key which wraps direct memory pointer and can be used as key for the {@link Map}.
   */
  private static class TrackedPointerKey extends WeakReference<OPointer> {

    private final int hashCode;

    TrackedPointerKey(OPointer referent) {
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
      final OPointer pointer = get();
      return pointer != null && pointer == ((TrackedPointerKey) obj).get();
    }
  }

  private static int id(Object object) {
    return System.identityHashCode(object);
  }

  private static final class ConsumptionMapEvictionIndicator extends WeakReference<Thread> {
    private final EnumMap<Intention, OModifiableLong> consumptionMap;

    public ConsumptionMapEvictionIndicator(
        Thread referent,
        ReferenceQueue<? super Thread> q,
        EnumMap<Intention, OModifiableLong> consumptionMap) {
      super(referent, q);

      this.consumptionMap = consumptionMap;
    }
  }

  private static final class MemoryStatPrinter implements Runnable {
    private final Set<ConsumptionMapEvictionIndicator> consumptionMaps;

    private MemoryStatPrinter(Set<ConsumptionMapEvictionIndicator> consumptionMaps) {
      this.consumptionMaps = consumptionMaps;
    }

    @Override
    public void run() {
      final EnumMap<Intention, OModifiableLong> accumulator = new EnumMap<>(Intention.class);

      for (final ConsumptionMapEvictionIndicator consumptionMap : consumptionMaps) {
        accumulateConsumptionStatistics(accumulator, consumptionMap);
      }

      final String memoryStat = printMemoryStatistics(accumulator);
      OLogManager.instance().infoNoDb(this, memoryStat);
    }
  }

  private static void accumulateConsumptionStatistics(
      EnumMap<Intention, OModifiableLong> accumulator,
      ConsumptionMapEvictionIndicator consumptionMap) {
    for (final Map.Entry<Intention, OModifiableLong> entry :
        consumptionMap.consumptionMap.entrySet()) {
      accumulator.compute(
          entry.getKey(),
          (k, v) -> {
            if (v == null) {
              v = new OModifiableLong();
            }

            v.value += entry.getValue().value;
            return v;
          });
    }
  }
}
