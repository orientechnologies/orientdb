package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OOperationLog;

import java.util.concurrent.atomic.AtomicLong;

public class OIncrementOperationalLog implements OOperationLog {
  private AtomicLong inc = new AtomicLong(0);

  @Override
  public OLogId log(ONodeRequest request) {
    return new OLogId(inc.incrementAndGet());
  }

  @Override
  public void logReceived(OLogId logId, ONodeRequest request) {

  }
}
