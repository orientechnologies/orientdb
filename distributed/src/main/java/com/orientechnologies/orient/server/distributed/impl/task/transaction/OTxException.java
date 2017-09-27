package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxException implements OTransactionResultPayload {
  public static final int ID = 5;

  private RuntimeException exception;

  public OTxException(RuntimeException exception) {
    this.exception = exception;
  }

  public RuntimeException getException() {
    return exception;
  }

  public void setException(RuntimeException exception) {
    this.exception = exception;
  }

  @Override
  public int getResponseType() {
    return ID;
  }
}
