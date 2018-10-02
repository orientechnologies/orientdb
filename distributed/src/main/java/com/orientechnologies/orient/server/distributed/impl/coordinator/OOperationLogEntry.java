package com.orientechnologies.orient.server.distributed.impl.coordinator;

public class OOperationLogEntry {
  protected OLogId       logId;
  protected ONodeRequest request;

  public OOperationLogEntry(OLogId logId, ONodeRequest request) {
    this.logId = logId;
    this.request = request;
  }

  public OLogId getLogId() {
    return this.logId;
  }

  public ONodeRequest getRequest() {
    return this.request;
  }
}
