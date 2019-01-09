package com.orientechnologies.orient.distributed.impl.coordinator.lock;

public class OLockGuard {
  private OLockKey key;

  public OLockGuard(OLockKey key) {
    this.key = key;
  }

  protected OLockKey getKey() {
    return key;
  }
}
