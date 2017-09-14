package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.id.ORecordId;

public class OTxUniqueIndex implements OTransactionResultPayload {
  private ORecordId recordId;
  private String    index;
  private Object    key;

  public OTxUniqueIndex(ORecordId recordId, String index, Object key) {
    this.recordId = recordId;
    this.index = index;
    this.key = key;
  }

  public ORecordId getRecordId() {
    return recordId;
  }

  public void setRecordId(ORecordId recordId) {
    this.recordId = recordId;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public Object getKey() {
    return key;
  }

  public void setKey(Object key) {
    this.key = key;
  }
}
