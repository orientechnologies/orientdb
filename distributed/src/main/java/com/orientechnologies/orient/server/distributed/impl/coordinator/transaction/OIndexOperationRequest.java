package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

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
}
