package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxLockTimeout implements OTransactionResultPayload{
  public static final int ID = 2;

  @Override
  public int getResponseType() {
    return ID;
  }
}
