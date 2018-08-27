package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.orient.core.exception.ODurableComponentException;

public class OPrefixBTreeException extends ODurableComponentException {

  public OPrefixBTreeException(OSBTreeException exception) {
    super(exception);
  }

  public OPrefixBTreeException(String message, OPrefixBTree component) {
    super(message, component);
  }
}

