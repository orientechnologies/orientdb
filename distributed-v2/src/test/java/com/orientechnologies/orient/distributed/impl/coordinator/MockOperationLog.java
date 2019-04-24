package com.orientechnologies.orient.distributed.impl.coordinator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class MockOperationLog implements OOperationLog {
  private AtomicLong sequence = new AtomicLong(0);

  @Override
  public OLogId log(OLogRequest request) {
    return new OLogId(sequence.incrementAndGet());
  }

  @Override
  public void logReceived(OLogId logId, OLogRequest request) {

  }

  @Override
  public OLogId lastPersistentLog() {
    return new OLogId(sequence.get());
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
    sequence.set(lastValid.getId());
  }
}
