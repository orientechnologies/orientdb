package com.orientechnologies.orient.server.distributed.impl.coordinator;

import com.orientechnologies.orient.core.id.ORID;

public interface ODistributedLockManager {

  OLockGuard lockRecord(ORID rid);

  OLockGuard lockIndexKey(String index, Object key);

}
