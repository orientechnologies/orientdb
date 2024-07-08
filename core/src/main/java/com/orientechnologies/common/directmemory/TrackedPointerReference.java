package com.orientechnologies.common.directmemory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * WeakReference to the direct memory pointer which tracks stack trace of allocation of direct
 * memory associated with this pointer.
 */
public class TrackedPointerReference extends WeakReference<OPointer> {

  public final int id;
  public final Exception stackTrace;

  public TrackedPointerReference(OPointer referent, ReferenceQueue<? super OPointer> q) {
    super(referent, q);

    this.id = ODirectMemoryAllocator.id(referent);
    this.stackTrace = new Exception();
  }
}
