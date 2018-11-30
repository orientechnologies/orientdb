package com.orientechnologies.orient.distributed.impl.coordinator;

public interface OClusterPositionAllocator {
  long allocate(int clusterId);
}
