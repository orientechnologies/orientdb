package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxException implements OTransactionResultPayload {
  public static final int ID = 5;

  private Throwable exception;

  public OTxException(Throwable exception) {
    this.exception = exception;
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable exception) {
    this.exception = exception;
  }

  @Override
  public int getResponseType() {
    return ID;
  }
}
