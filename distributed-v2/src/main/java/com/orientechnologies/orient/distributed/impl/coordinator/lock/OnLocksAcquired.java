package com.orientechnologies.orient.distributed.impl.coordinator.lock;

import java.util.List;

public interface OnLocksAcquired {
  void execute(List<OLockGuard> guards);
}
