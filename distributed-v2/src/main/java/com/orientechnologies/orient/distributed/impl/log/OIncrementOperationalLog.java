package com.orientechnologies.orient.distributed.impl.log;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class OIncrementOperationalLog implements OOperationLog {

  private long term = 0;
  private AtomicLong inc = new AtomicLong(term);
  private OLogId lastLog;

  @Override
  public OLogId log(OLogRequest request) {
    lastLog = new OLogId(inc.incrementAndGet(), term, lastLog == null ? -1 : lastLog.getTerm());
    return lastLog;
  }

  @Override
  public boolean logReceived(OLogId logId, OLogRequest request) {
    lastLog = logId;
    return true;
  }

  @Override
  public OLogId lastPersistentLog() {
    return lastLog == null ? new OLogId(inc.get(), -1, -1) : lastLog;
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
    inc.set(lastValid.getId());
    return LogIdStatus.PRESENT;
  }

  @Override
  public void setLeader(boolean master, long term) {
    this.term = term;
  }
}
