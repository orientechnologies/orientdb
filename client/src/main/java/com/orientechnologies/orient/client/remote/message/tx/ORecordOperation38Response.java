package com.orientechnologies.orient.client.remote.message.tx;

import com.orientechnologies.orient.core.id.ORID;

public class ORecordOperation38Response {
  private byte type;
  private byte recordType;
  private ORID id;
  private ORID oldId;
  private byte[] record;
  private byte[] original;
  private int version;
  private boolean contentChanged;

  public ORecordOperation38Response() {}

  public ORecordOperation38Response(
      byte type,
      byte recordType,
      ORID id,
      ORID oldId,
      byte[] record,
      int version,
      boolean contentChanged) {
    this.type = type;
    this.recordType = recordType;
    this.id = id;
    this.oldId = oldId;
    this.record = record;
    this.version = version;
    this.contentChanged = contentChanged;
  }

  public ORID getId() {
    return id;
  }

  public void setId(ORID id) {
    this.id = id;
  }

  public ORID getOldId() {
    return oldId;
  }

  public void setOldId(ORID oldId) {
    this.oldId = oldId;
  }

  public byte[] getRecord() {
    return record;
  }

  public void setRecord(byte[] record) {
    this.record = record;
  }

  public byte[] getOriginal() {
    return original;
  }

  public void setOriginal(byte[] original) {
    this.original = original;
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
