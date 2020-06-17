package com.orientechnologies.orient.distributed.impl.coordinator.transaction.results;

import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OUniqueKeyViolationResult implements OTransactionResult {
  private String keyStringified;
  private ORecordId recordRequesting;
  private ORecordId recordOwner;
  private String indexName;

  public OUniqueKeyViolationResult(
      String keyStringified, ORecordId recordRequesting, ORecordId recordOwner, String indexName) {
    this.keyStringified = keyStringified;
    this.recordRequesting = recordRequesting;
    this.recordOwner = recordOwner;
    this.indexName = indexName;
  }

  public OUniqueKeyViolationResult() {}

  public String getKeyStringified() {
    return keyStringified;
  }

  public ORecordId getRecordRequesting() {
    return recordRequesting;
  }

  public ORecordId getRecordOwner() {
    return recordOwner;
  }

  public String getIndexName() {
    return indexName;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(keyStringified);
    ORecordId.serialize(recordRequesting, output);
    ORecordId.serialize(recordOwner, output);
    output.writeUTF(indexName);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    keyStringified = input.readUTF();
    recordRequesting = ORecordId.deserialize(input);
    recordOwner = ORecordId.deserialize(input);
    indexName = input.readUTF();
  }
}
