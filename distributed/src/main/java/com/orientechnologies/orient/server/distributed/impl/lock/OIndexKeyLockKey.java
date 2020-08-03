package com.orientechnologies.orient.server.distributed.impl.lock;

import java.util.Objects;

public class OIndexKeyLockKey implements OLockKey {
  private String index;
  private Object key;

  public OIndexKeyLockKey(String index, Object key) {
    this.index = index;
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OIndexKeyLockKey that = (OIndexKeyLockKey) o;
    return Objects.equals(index, that.index) && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, key);
  }

  @Override
  public String toString() {
    return "OIndexKeyLockKey{" + "index='" + index + '\'' + ", key='" + key + '\'' + '}';
  }
}
