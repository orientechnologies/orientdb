package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OOperationLog {
  OLogId log(ONodeRequest request);

  void logReceived(OLogId logId, ONodeRequest request);
}
