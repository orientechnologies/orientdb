package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.index.OCompositeKey;

public class OIndexKeyChange {
  // Key Change Operation
  private final static int PUT       = 1;
  private final static int REMOVE    = 2;
  // Key Type
  private final static int SIMPLE    = 1;
  private final static int COMPOSITE = 2;

  private int    operation;
  private int    keyType;
  private Object value;

  public OIndexKeyChange(int operation, Object value) {
    this.operation = operation;
    if (value instanceof OCompositeKey) {
      keyType = COMPOSITE;
    } else {
      keyType = SIMPLE;
    }
  }

  public int getOperation() {
    return operation;
  }

  public Object getValue() {
    return value;
  }
}
