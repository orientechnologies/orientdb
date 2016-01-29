package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class OStorageExistsException extends OStorageException implements OHighLevelException {
  public OStorageExistsException(OStorageExistsException exception) {
    super(exception);
  }

  public OStorageExistsException(String string) {
    super(string);
  }
}
