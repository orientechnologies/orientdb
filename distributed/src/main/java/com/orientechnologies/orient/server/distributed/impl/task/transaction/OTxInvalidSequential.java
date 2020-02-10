package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.server.distributed.OTransactionId;

public class OTxInvalidSequential implements OTransactionResultPayload {

  public static final int            ID = 9;
  private             OTransactionId current;

  public OTxInvalidSequential(OTransactionId current) {
    this.current = current;
  }

  @Override
  public int getResponseType() {
    return ID;
  }

  public OTransactionId getCurrent() {
    return current;
  }

  public void setCurrent(OTransactionId current) {
    this.current = current;
  }
}
