package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxInvalidSequential implements OTransactionResultPayload {

  public static final int ID = 9;

  public OTxInvalidSequential() {}

  @Override
  public int getResponseType() {
    return ID;
  }
}
