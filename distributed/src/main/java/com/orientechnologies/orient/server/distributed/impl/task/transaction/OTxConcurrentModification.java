package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORecordId;

public class OTxConcurrentModification implements OTransactionResultPayload {
  public static final int ID = 4;
  private ORecordId recordId;
  private int version;

  public OTxConcurrentModification(ORecordId recordId, int version) {
    this.recordId = recordId;
    this.version = version;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public ORecordId getRecordId() {
    return recordId;
  }

  public void setRecordId(ORecordId recordId) {
    this.recordId = recordId;
  }

  @Override
  public int getResponseType() {
    return ID;
  }
}
