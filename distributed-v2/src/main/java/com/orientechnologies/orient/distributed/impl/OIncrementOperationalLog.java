package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;

import java.util.concurrent.atomic.AtomicLong;

public class OIncrementOperationalLog implements OOperationLog {
  private AtomicLong inc = new AtomicLong(0);

  @Override
  public OLogId log(OLogRequest request) {
    return new OLogId(inc.incrementAndGet());
  }

  @Override
  public void logReceived(OLogId logId, OLogRequest request) {

  }

  @Override
  public OLogId lastPersistentLog() {
    return new OLogId(inc.get());
  }
}
