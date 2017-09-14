package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.common.exception.OException;

public class OTxException implements OTransactionResultPayload {

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
}
