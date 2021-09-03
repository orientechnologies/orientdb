package com.orientechnologies.orient.distributed.impl.coordinator.transaction.results;

import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OConcurrentModificationResult implements OTransactionResult {
  private ORecordId recordId;
  private int updateVersion;
  private int persistentVersion;

  public OConcurrentModificationResult(
      ORecordId recordId, int updateVersion, int persistentVersion) {
    this.recordId = recordId;
    this.updateVersion = updateVersion;
    this.persistentVersion = persistentVersion;
  }

  public OConcurrentModificationResult() {}

  public ORecordId getRecordId() {
    return recordId;
  }

  public int getUpdateVersion() {
    return updateVersion;
  }

  public int getPersistentVersion() {
    return persistentVersion;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    ORecordId.serialize(recordId, output);
    output.writeInt(updateVersion);
    output.writeInt(persistentVersion);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    recordId = ORecordId.deserialize(input);
    updateVersion = input.readInt();
    persistentVersion = input.readInt();
  }
}
