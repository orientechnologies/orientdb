package com.orientechnologies.orient.core.storage.index.nkbtree.binarybtree;

import com.orientechnologies.orient.core.exception.ODurableComponentException;

public class BinaryBTreeException extends ODurableComponentException {
  public BinaryBTreeException(BinaryBTreeException exception) {
    super(exception);
  }

  public BinaryBTreeException(String message, BinaryBTree component) {
    super(message, component);
  }
}
