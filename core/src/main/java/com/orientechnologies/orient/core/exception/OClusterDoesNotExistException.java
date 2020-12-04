package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class OClusterDoesNotExistException extends OStorageException
    implements OHighLevelException {

  public OClusterDoesNotExistException(OClusterDoesNotExistException exception) {
    super(exception);
  }

  public OClusterDoesNotExistException(String string) {
    super(string);
  }
}
