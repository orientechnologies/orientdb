package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.OLockGuard;
import com.orientechnologies.orient.distributed.impl.coordinator.lock.OnLocksAcquired;
import java.util.List;
import java.util.SortedSet;

public interface ODistributedLockManager {

  void lock(
      SortedSet<ORID> rids, SortedSet<OPair<String, String>> indexKeys, OnLocksAcquired acquired);

  void unlock(List<OLockGuard> guards);

  void lockResource(String name, OnLocksAcquired acquired);
}
