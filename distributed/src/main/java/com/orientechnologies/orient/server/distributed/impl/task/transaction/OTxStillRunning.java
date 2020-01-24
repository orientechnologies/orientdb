package com.orientechnologies.orient.server.distributed.impl.task.transaction;

public class OTxStillRunning implements OTransactionResultPayload {
  public static final int ID = 8;

  @Override
  public int getResponseType() {
    return ID;
  }
}
