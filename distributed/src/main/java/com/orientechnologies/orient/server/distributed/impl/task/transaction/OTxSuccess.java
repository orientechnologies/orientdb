package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxSuccess implements OTransactionResultPayload {
  private static final int ID = 1;

  @Override
  public int getResponseType() {
    return ID;
  }
}
