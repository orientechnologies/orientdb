package com.orientechnologies.orient.distributed.impl.coordinator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLog implements OOperationLog {
  private long term = 0;
  private AtomicLong sequence;

  public MockOperationLog() {
    this(0);
  }

  public MockOperationLog(long startFrom) {
    sequence = new AtomicLong(startFrom);
  }

  @Override
  public OLogId log(OLogRequest request) {
    return new OLogId(sequence.incrementAndGet(), term);
  }

  @Override
  public boolean logReceived(OLogId logId, OLogRequest request) {
    return true;
  }

  @Override
  public OLogId lastPersistentLog() {
    return new OLogId(sequence.get(), term);
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
    sequence.set(lastValid.getId());
    return LogIdStatus.PRESENT;
  }

  @Override
  public void setLeader(boolean master, long term) {
    this.term = term;
  }
}
