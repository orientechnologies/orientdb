package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORID;

public class OTxRecordLockTimeout implements OTransactionResultPayload {
  public static final int ID = 2;
  private String node;
  private ORID lockedId;

  public OTxRecordLockTimeout(String node, ORID lockedId) {
    this.node = node;
    this.lockedId = lockedId;
  }

  @Override
  public int getResponseType() {
    return ID;
  }

  public ORID getLockedId() {
    return lockedId;
  }

  public String getNode() {
    return node;
  }
}
