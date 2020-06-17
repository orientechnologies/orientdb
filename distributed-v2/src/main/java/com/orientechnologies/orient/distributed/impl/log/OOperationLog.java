package com.orientechnologies.orient.distributed.impl.log;

import java.util.Optional;

public interface OOperationLog extends AutoCloseable {

  enum LogIdStatus {
    TOO_OLD,
    PRESENT,
    INVALID,
    FUTURE;
  }

  OLogId log(OLogRequest request);

  /**
   * @param logId A log ID received
   * @param request a request corresponding to the log ID
   * @return true if the log was added, false otherwise (eg. if the oplog is ahead of the logId)
   */
  boolean logReceived(OLogId logId, OLogRequest request);

  OLogId lastPersistentLog();

  /**
   * @param from first entry to get.
   * @param to last entry to get (included).
   * @return
   */
  OOplogIterator iterate(long from, long to);

  Optional<OOplogIterator> searchFrom(OLogId from);

  @Override
  void close();

  LogIdStatus removeAfter(OLogId lastValid);

  void setLeader(boolean master, long term);
}
