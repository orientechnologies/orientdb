package com.orientechnologies.common.directmemory;

import java.lang.ref.WeakReference;
import java.util.Map;

/**
 * WeakReference key which wraps direct memory pointer and can be used as key for the {@link Map}.
 */
public class TrackedPointerKey extends WeakReference<OPointer> {

  private final int hashCode;

  public TrackedPointerKey(OPointer referent) {
    super(referent);
    hashCode = System.identityHashCode(referent);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    final OPointer pointer = get();
    return pointer != null && pointer == ((TrackedPointerKey) obj).get();
  }
}
