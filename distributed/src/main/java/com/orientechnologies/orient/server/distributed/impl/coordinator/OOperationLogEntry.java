package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OOperationLogEntry {
  OLogId getLogId();

  ONodeRequest getRequest();
}
