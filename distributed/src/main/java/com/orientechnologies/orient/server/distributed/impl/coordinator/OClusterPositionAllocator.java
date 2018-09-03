package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OClusterPositionAllocator {
  long allocate(int clusterId);
}
