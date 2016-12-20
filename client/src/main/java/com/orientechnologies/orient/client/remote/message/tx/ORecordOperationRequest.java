package com.orientechnologies.orient.client.remote.message.tx;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

public class ORecordOperationRequest {
  private byte    type;
  private byte    recordType;
  private ORID    id;
  private ORecord record;
  private int     version;
  private boolean contentChanged;

  public ORID getId() {
    return id;
  }

  public void setId(ORID id) {
    this.id = id;
  }

  public ORecord getRecord() {
    return record;
  }

  public void setRecord(ORecord record) {
    this.record = record;
  }

  public byte getRecordType() {
    return recordType;
  }

  public void setRecordType(byte recordType) {
    this.recordType = recordType;
  }

  public byte getType() {
    return type;
  }

  public void setType(byte type) {
    this.type = type;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public void setContentChanged(boolean contentChanged) {
    this.contentChanged = contentChanged;
  }

  public boolean isContentChanged() {
    return contentChanged;
  }
}