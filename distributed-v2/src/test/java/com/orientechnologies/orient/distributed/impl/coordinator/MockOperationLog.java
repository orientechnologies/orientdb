package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OLogRequest;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.impl.log.OOplogIterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLog implements OOperationLog {
  private long term = 0;
  private AtomicLong sequence;
  private OLogId lastLog;

  public MockOperationLog() {
    this(0);
  }

  public MockOperationLog(long startFrom) {
    sequence = new AtomicLong(startFrom);
  }

  @Override
  public OLogId log(OLogRequest request) {
    lastLog =
        new OLogId(sequence.incrementAndGet(), term, lastLog == null ? -1 : lastLog.getTerm());
    return lastLog;
  }

  @Override
  public boolean logReceived(OLogId logId, OLogRequest request) {
    lastLog = logId;
    return true;
  }

  @Override
  public OLogId lastPersistentLog() {
    return lastLog == null ? new OLogId(sequence.get(), -1, -1) : lastLog;
  }

  @Override
  public OOplogIterator iterate(long from, long to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<OOplogIterator> searchFrom(OLogId from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {}

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
