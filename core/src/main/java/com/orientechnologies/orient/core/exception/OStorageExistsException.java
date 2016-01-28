package com.orientechnologies.orient.core.exception;

public class OStorageExistsException extends OStorageException {
  public OStorageExistsException(OStorageException exception) {
    super(exception);
  }

  public OStorageExistsException(String string) {
    super(string);
  }
}
