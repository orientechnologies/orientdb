package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class OIndexOperationRequest {
  private String                indexName;
  //The clean of index values is alternative to the keys;
  private boolean               cleanIndexValues;
  private List<OIndexKeyChange> indexKeyChanges;

  public List<OIndexKeyChange> getIndexKeyChanges() {
    return indexKeyChanges;
  }

  public String getIndexName() {
    return indexName;
  }

  public boolean isCleanIndexValues() {
    return cleanIndexValues;
  }

  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(indexName);
    if (cleanIndexValues) {
      output.writeBoolean(true);
    } else {
      output.writeBoolean(false);
      output.writeInt(indexKeyChanges.size());
      for (OIndexKeyChange change : indexKeyChanges) {
        change.serialize(output);
      }
    }
  }

  public void deserialize(DataInput input) throws IOException {
    indexName = input.readUTF();
    cleanIndexValues = input.readBoolean();
    if (!cleanIndexValues) {
      int size = input.readInt();
      while (size-- > 0) {
        OIndexKeyChange change = new OIndexKeyChange();
        change.deserialize(input);
      }
    }
  }
}
