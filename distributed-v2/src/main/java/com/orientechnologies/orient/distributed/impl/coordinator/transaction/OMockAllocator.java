package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.distributed.impl.coordinator.OClusterPositionAllocator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class OMockAllocator implements OClusterPositionAllocator {
  // Just Test not really need to be concurrent.
  private Map<Integer, AtomicLong> allocator = new HashMap<>();

  @Override
  public long allocate(int clusterId) {
    AtomicLong counter = allocator.get(clusterId);
    if (counter == null) {
      counter = new AtomicLong(0);
      allocator.put(clusterId, counter);
    }
    return counter.get();
  }
}
