package com.orientechnologies.orient.server.distributed.impl.coordinator;

public interface OOperationLog {
  OLogId log(ONodeRequest request);
}
