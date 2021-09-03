package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCreatedRecordResponse {
  private ORID currentRid;
  private ORID createdRid;
  private int version;

  public OCreatedRecordResponse(ORID currentRid, ORID createdRid, int version) {
    this.currentRid = currentRid;
    this.createdRid = createdRid;
    this.version = version;
  }

  public OCreatedRecordResponse() {}

  public ORID getCreatedRid() {
    return createdRid;
  }

  public ORID getCurrentRid() {
    return currentRid;
  }

  public int getVersion() {
    return version;
  }

  public void serialize(DataOutput output) throws IOException {
    ORecordId.serialize(currentRid, output);
    ORecordId.serialize(createdRid, output);
    output.writeInt(version);
  }

  public void deserialize(DataInput input) throws IOException {
    currentRid = ORecordId.deserialize(input);
    createdRid = ORecordId.deserialize(input);
    version = input.readInt();
  }
}
