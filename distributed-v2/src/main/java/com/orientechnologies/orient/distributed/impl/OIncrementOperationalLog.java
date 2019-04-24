package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLogEntry;

import java.util.Iterator;
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

  @Override
  public Iterator<OOperationLogEntry> iterate(OLogId from, OLogId to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {

  }

  @Override
  public void removeAfter(OLogId lastValid) {
    inc.set(lastValid.getId());
  }
}
