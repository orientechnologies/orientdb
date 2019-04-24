package com.orientechnologies.orient.distributed.impl.coordinator;

import java.util.Iterator;

public interface OOperationLog extends AutoCloseable {
  OLogId log(OLogRequest request);

  void logReceived(OLogId logId, OLogRequest request);

  OLogId lastPersistentLog();

  /**
   * @param from first entry to get. Null to iterate since the beginning
   * @param to   last entry to get (included).
   *
   * @return
   */
  Iterator<OOperationLogEntry> iterate(OLogId from, OLogId to);

  @Override
  void close();

  void removeAfter(OLogId lastValid);
}
