package com.orientechnologies.orient.distributed.impl.coordinator.lock;

import java.util.Objects;

public class OResourceLockKey implements OLockKey {
  private String resourceKey;

  public OResourceLockKey(String resourceKey) {
    this.resourceKey = resourceKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OResourceLockKey that = (OResourceLockKey) o;
    return Objects.equals(resourceKey, that.resourceKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceKey);
  }
}
