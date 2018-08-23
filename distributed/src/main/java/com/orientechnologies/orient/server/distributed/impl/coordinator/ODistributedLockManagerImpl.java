package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.common.concur.lock.OSimpleLockManager;
import com.orientechnologies.common.concur.lock.OSimpleLockManagerImpl;
import com.orientechnologies.orient.core.id.ORID;

public class ODistributedLockManagerImpl implements ODistributedLockManager {

  private OSimpleLockManager<ORID>   ridLocks;
  private OSimpleLockManager<Object> keyLocks;

  public ODistributedLockManagerImpl(int timeout) {
    ridLocks = new OSimpleLockManagerImpl<>(timeout);
    keyLocks = new OSimpleLockManagerImpl<>(timeout);
  }

  @Override
  public OLockGuard lockRecord(ORID rid) {
    ridLocks.lock(rid);
    return new OLockGuard() {
      @Override
      public void release() {
        ridLocks.unlock(rid);
      }
    };
  }

  @Override
  public OLockGuard lockIndexKey(String index, Object key) {
    //TODO: Implement locking per index and key
    keyLocks.unlock(key);
    return new OLockGuard() {
      @Override
      public void release() {
        keyLocks.unlock(key);
      }
    };
  }
}
