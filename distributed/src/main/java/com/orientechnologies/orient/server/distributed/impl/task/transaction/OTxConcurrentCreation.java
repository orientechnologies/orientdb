package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORID;

public class OTxConcurrentCreation implements OTransactionResultPayload {
  public static final int ID = 6;
  private ORID actualRid;
  private ORID expectedRid;

  public OTxConcurrentCreation(ORID actualRid, ORID expectedRid) {
    this.actualRid = actualRid;
    this.expectedRid = expectedRid;
  }

  @Override
  public int getResponseType() {
    return ID;
  }

  public ORID getActualRid() {
    return actualRid;
  }

  public ORID getExpectedRid() {
    return expectedRid;
  }

  public void setActualRid(ORID actualRid) {
    this.actualRid = actualRid;
  }

  public void setExpectedRid(ORID expectedRid) {
    this.expectedRid = expectedRid;
  }
}
