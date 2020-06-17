package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OIndexOperationRequest {
  private String indexName;
  private boolean cleanIndexValues;
  private List<OIndexKeyChange> indexKeyChanges;

  public OIndexOperationRequest() {}

  public OIndexOperationRequest(
      String indexName, boolean cleanIndexValues, List<OIndexKeyChange> indexKeyChanges) {
    this.indexName = indexName;
    this.cleanIndexValues = cleanIndexValues;
    this.indexKeyChanges = indexKeyChanges;
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(indexName);
    output.writeBoolean(cleanIndexValues);
    output.writeInt(indexKeyChanges.size());
    for (OIndexKeyChange change : indexKeyChanges) {
      change.serialize(output);
    }
  }

  public void deserialize(DataInput input) throws IOException {
    indexName = input.readUTF();
    cleanIndexValues = input.readBoolean();
    int size = input.readInt();
    this.indexKeyChanges = new ArrayList<>(size);
    while (size-- > 0) {
      OIndexKeyChange change = new OIndexKeyChange();
      change.deserialize(input);
      this.indexKeyChanges.add(change);
    }
  }

  public List<OIndexKeyChange> getIndexKeyChanges() {
    return indexKeyChanges;
  }

  public String getIndexName() {
    return indexName;
  }

  public boolean isCleanIndexValues() {
    return cleanIndexValues;
  }
}
