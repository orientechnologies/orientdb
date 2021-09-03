package com.orientechnologies.orient.server.distributed.impl.lock;

public interface OnFreezeAcquired {
  void acquired(OFreezeGuard guard);
}
