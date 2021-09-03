package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

/** This exception is thrown if access to the manager of tree-based RidBags will be prohibited. */
public class OAccessToSBtreeCollectionManagerIsProhibitedException extends OCoreException
    implements OHighLevelException {
  public OAccessToSBtreeCollectionManagerIsProhibitedException(
      OAccessToSBtreeCollectionManagerIsProhibitedException exception) {
    super(exception);
  }

  public OAccessToSBtreeCollectionManagerIsProhibitedException(String message) {
    super(message);
  }
}
