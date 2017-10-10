package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORecordId;

public class OTxConcurrentCreate implements OTransactionResultPayload {
  public static final int ID = 6;
  private ORecordId recordId;

  public OTxConcurrentCreate(ORecordId recordId) {
    this.recordId = recordId;
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
