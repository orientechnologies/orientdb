package com.orientechnologies.orient.distributed.impl.coordinator.lock;

import com.orientechnologies.common.util.OPair;
import java.util.Objects;

class OIndexKeyLockKey implements OLockKey {
  private String index;
  private String key;

  public OIndexKeyLockKey(OPair<String, String> indexKey) {
    this.index = indexKey.getKey();
    this.key = indexKey.getValue();
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
