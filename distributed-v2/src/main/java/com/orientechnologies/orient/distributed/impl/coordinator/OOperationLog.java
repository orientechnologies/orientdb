package com.orientechnologies.orient.distributed.impl.coordinator;

import java.util.Iterator;

public interface OOperationLog extends AutoCloseable {

  enum LogIdStatus {
    TOO_OLD, PRESENT, INVALID, FUTURE;
  }

  OLogId log(OLogRequest request);

  /**
   *
   * @param logId A log ID received
   * @param request a request corresponding to the log ID
   * @return true if the log was added, false otherwise (eg. if the oplog is ahead of the logId)
   */
  boolean logReceived(OLogId logId, OLogRequest request);

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

  LogIdStatus removeAfter(OLogId lastValid);
}
