package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OUpdatedRecordResponse {
  private ORID rid;
  private int version;

  public OUpdatedRecordResponse(ORID rid, int version) {
    this.rid = rid;
    this.version = version;
  }

  public OUpdatedRecordResponse() {}

  public ORID getRid() {
    return rid;
  }

  public int getVersion() {
    return version;
  }

  public void serialize(DataOutput output) throws IOException {
    ORecordId.serialize(rid, output);
    output.writeInt(version);
  }

  public void deserialize(DataInput input) throws IOException {
    rid = ORecordId.deserialize(input);
    version = input.readInt();
  }
}
