package com.orientechnologies.orient.server.distributed.impl.coordinator.lock;

import java.util.List;

public interface OnLocksAcquired {
  void execute(List<OLockGuard> guards);
}
