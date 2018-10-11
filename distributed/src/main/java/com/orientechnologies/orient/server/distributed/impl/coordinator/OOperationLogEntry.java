package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class OOperationLogEntry {
  protected OLogId      logId;
  protected OLogRequest request;

  public OOperationLogEntry(OLogId logId, OLogRequest request) {
    this.logId = logId;
    this.request = request;
  }

  public OLogId getLogId() {
    return this.logId;
  }

  public OLogRequest getRequest() {
    return this.request;
  }
}
