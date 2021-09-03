package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODeletedRecordResponse {
  private ORID rid;

  public ODeletedRecordResponse(ORID rid) {
    this.rid = rid;
  }

  public ODeletedRecordResponse() {}

  public ORID getRid() {
    return rid;
  }

  public void serialize(DataOutput output) throws IOException {
    ORecordId.serialize(rid, output);
  }

  public void deserialize(DataInput input) throws IOException {
    rid = ORecordId.deserialize(input);
  }
}
