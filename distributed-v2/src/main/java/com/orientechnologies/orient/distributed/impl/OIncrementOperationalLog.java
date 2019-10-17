package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLogEntry;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class OIncrementOperationalLog implements OOperationLog {

  private long term = 0;
  private AtomicLong inc = new AtomicLong(term);

  @Override
  public OLogId log(OLogRequest request) {
    return new OLogId(inc.incrementAndGet(), term);
  }

  @Override
  public boolean logReceived(OLogId logId, OLogRequest request) {
    return true;
  }

  @Override
  public OLogId lastPersistentLog() {
    return new OLogId(inc.get(), 0);
  }

  @Override
  public Iterator<OOperationLogEntry> iterate(long from, long to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {

  }

  @Override
  public LogIdStatus removeAfter(OLogId lastValid) {
    inc.set(lastValid.getId());
    return LogIdStatus.PRESENT;
  }

  @Override
  public void setLeader(boolean master, long term) {
    this.term = term;
  }
}
