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

import com.orientechnologies.common.exception.ODirectMemoryAllocationFailedException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Manages all allocations/deallocations from/to direct memory.
 * Also tracks the presence of memory leaks.
 *
 * @see OGlobalConfiguration#DIRECT_MEMORY_POOL_LIMIT
 */
public class ODirectMemoryAllocator implements ODirectMemoryAllocatorMXBean {
  /**
   * Name of JMX bean
   */
  private static final String MBEAN_NAME = "com.orientechnologies.common.directmemory:type=ODirectMemoryAllocatorMXBean";

  /**
   * Whether we should track memory leaks during application execution
   */
  private static final boolean TRACK = OGlobalConfiguration.DIRECT_MEMORY_TRACK_MODE.getValueAsBoolean();

  /**
   * Holder for singleton instance
   */
  private static final AtomicReference<ODirectMemoryAllocator> INSTANCE_HOLDER = new AtomicReference<>();

  /**
   * Reference queue for all created direct memory pointers. During check of memory leaks we access this queue to check
   * whether we have leaked direct memory pointers.
   */
  private final ReferenceQueue<OPointer> trackedPointersQueue;

  /**
   * WeakReference to the allocated pointer. We use those references to track stack traces where
   * those pointers were allocated. Even if reference to the pointer will be collected we still will have
   * information where it was allocated and also presence of this pointers into the queue during OrientDB engine shutdown
   * indicates that direct memory was not released back and there are memory leaks in application.
   */
  private final Set<TrackedPointerReference> trackedReferences;

  /**
   * Map between pointers and soft references which are used for tracking of memory leaks.
   * Key itself is a weak reference but we can not use only single weak reference collection because
   * identity of key equals to identity of pointer and identity of reference is based on comparision of
   * instances of objects. The last one is used during memory leak detection when we find references in the reference queue and
   * try to check whether those references were tracked during pointer allocation or not.
   */
  private final Map<TrackedPointerKey, TrackedPointerReference> trackedBuffers;

  /**
   * Amount of direct memory consumed by using this allocator.
   */
  private final LongAdder memoryConsumption = new LongAdder();

  /**
   * @return singleton instance.
   */
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

  ODirectMemoryAllocator() {
    trackedPointersQueue = new ReferenceQueue<>();
    trackedReferences = new HashSet<>();
    trackedBuffers = new HashMap<>();
  }

  /**
   * Allocates chunk of direct memory of given size.
   *
   * @param size Amount of memory to allocate
   *
   * @return Pointer to allocated memory
   *
   * @throws ODirectMemoryAllocationFailedException if it is impossible to allocate amount of direct memory of given size
   */
  public OPointer allocate(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Size of allocated memory can not be less or equal to 0");
    }

    final long pointer = Native.malloc(size);
    if (pointer == 0) {
      throw new ODirectMemoryAllocationFailedException("Can not allocate direct memory chunk of size " + size);
    }

    final OPointer ptr = new OPointer(new Pointer(pointer), size);
    memoryConsumption.add(size);

    return track(ptr);
  }

  /**
   * Returns allocated direct memory back to OS
   */
  public void deallocate(OPointer pointer) {
    if (pointer == null) {
      throw new IllegalArgumentException("Null value is passed");
    }

    final Pointer ptr = pointer.getNativePointer();
    Native.free(Pointer.nativeValue(ptr));
    memoryConsumption.add(-pointer.getSize());
    untrack(pointer);
  }

  /**
   * @inheritDoc
   */
  @Override
  public long getMemoryConsumption() {
    return memoryConsumption.longValue();
  }

  /**
   * Verifies that all pointers which were allocated by allocator are freed.
   */
  public void checkMemoryLeaks() {
    if (TRACK) {
      final long memCons = memoryConsumption.longValue();

      if (memCons > 0) {
        OLogManager.instance()
            .warnNoDb(this, "DIRECT-TRACK: memory consumption is not zero (%d bytes), it may indicate presence of memory leaks",
                memCons);

        assert false;
      }
      synchronized (this) {
        for (TrackedPointerReference reference : trackedReferences)
          OLogManager.instance()
              .errorNoDb(this, "DIRECT-TRACK: unreleased direct memory pointer `%X` detected.", reference.stackTrace, reference.id);

        checkTrackedPointerLeaks();

        assert trackedReferences.size() == 0;
      }
    }
  }

  /**
   * Adds pointer to the containers of weak references so we will be able to find memory leaks related to this pointer
   */
  private OPointer track(OPointer pointer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedPointerReference reference = new TrackedPointerReference(pointer, trackedPointersQueue);
        trackedReferences.add(reference);
        trackedBuffers.put(new TrackedPointerKey(pointer), reference);

        checkTrackedPointerLeaks();
      }
    }

    return pointer;
  }

  /**
   * Checks reference queue to find direct memory leaks
   */
  public void checkTrackedPointerLeaks() {
    boolean leaked = false;

    TrackedPointerReference reference;
    while ((reference = (TrackedPointerReference) trackedPointersQueue.poll()) != null) {
      if (trackedReferences.remove(reference)) {
        OLogManager.instance()
            .errorNoDb(this, "DIRECT-TRACK: unreleased direct memory pointer `%X` detected.", reference.stackTrace, reference.id);
        leaked = true;
      }
    }

    assert !leaked;
  }

  /**
   * Registers the MBean for this byte buffer pool.
   *
   * @see OByteBufferPoolMXBean
   */
  public void registerMBean() {
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mbeanName = new ObjectName(MBEAN_NAME);

      if (!server.isRegistered(mbeanName)) {
        server.registerMBean(this, mbeanName);
      } else {
        OLogManager.instance().warnNoDb(this,
            "MBean with name %s has already registered. Probably your system was not shutdown correctly"
                + " or you have several running applications which use OrientDB engine inside", mbeanName.getCanonicalName());
      }

    } catch (Exception e) {
      OLogManager.instance().errorNoDb(this, "Error during MBean registration", e);
    }
  }

  /**
   * Unregisters the MBean for this byte buffer pool.
   *
   * @see OByteBufferPoolMXBean
   */
  public void unregisterMBean() {
    try {
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final ObjectName mbeanName = new ObjectName(MBEAN_NAME);
      server.unregisterMBean(mbeanName);
    } catch (Exception e) {
      OLogManager.instance().errorNoDb(this, "Error during MBean de-registration", e);
    }
  }

  /**
   * Removes direct memory pointer from container of weak references, it is done just after memory which was referenced by this
   * pointer will be deallocated. So no memory leaks can be caused by this pointer.
   */
  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  private void untrack(OPointer pointer) {
    if (TRACK) {
      synchronized (this) {
        final TrackedPointerKey trackedBufferKey = new TrackedPointerKey(pointer);

        final TrackedPointerReference reference = trackedBuffers.remove(trackedBufferKey);
        if (reference == null) {
          OLogManager.instance()
              .errorNoDb(this, "DIRECT-TRACK: untracked direct memory pointer `%X` detected.", new Exception(), id(pointer));

          assert false;
        } else {
          trackedReferences.remove(reference);
          reference.clear();
        }

        checkTrackedPointerLeaks();
      }
    }
  }

  /**
   * WeakReference to the direct memory pointer which tracks stack trace of allocation of direct memory associated with this pointer.
   */
  private static class TrackedPointerReference extends WeakReference<OPointer> {

    public final int       id;
    final        Exception stackTrace;

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

}