package com.orientechnologies.orient.server.distributed.impl.lock;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Objects;

class ORIDLockKey implements OLockKey {
  private ORID rid;

  public ORIDLockKey(ORID rid) {
    this.rid = rid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ORIDLockKey that = (ORIDLockKey) o;
    return Objects.equals(rid, that.rid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid);
  }

  @Override
  public String toString() {
    return "ORIDLockKey{" + "rid=" + rid + '}';
  }
}
