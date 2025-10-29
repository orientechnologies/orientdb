package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

public interface OStorageRecordOperation {

  byte getType();

  void setType(byte created);

  ORID getRID();

  void setResultData(Integer result);

  ORecord getRecord();

  byte[] getRecordBytes();
}
