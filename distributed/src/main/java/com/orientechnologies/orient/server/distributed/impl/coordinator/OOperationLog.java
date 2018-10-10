package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.util.Iterator;

public interface OOperationLog extends AutoCloseable {
  OLogId log(ONodeRequest request);

  void logReceived(OLogId logId, ONodeRequest request);

  /**
   * @param from first entry to get. Null to iterate since the beginning
   * @param to   last entry to get (included).
   *
   * @return
   */
  default Iterator<OOperationLogEntry> iterate(OLogId from, OLogId to) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void close() {

  }
}
