package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class OStorageDoesNotExistException extends OStorageException
    implements OHighLevelException {

  public OStorageDoesNotExistException(OStorageDoesNotExistException exception) {
    super(exception);
  }

  public OStorageDoesNotExistException(String string) {
    super(string);
  }
}
