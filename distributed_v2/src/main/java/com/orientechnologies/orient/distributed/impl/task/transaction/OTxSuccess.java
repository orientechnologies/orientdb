package com.orientechnologies.orient.distributed.impl.task.transaction;

public class OTxSuccess implements OTransactionResultPayload {
  public static final int ID = 1;

  @Override
  public int getResponseType() {
    return ID;
  }
}
