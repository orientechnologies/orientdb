package com.orientechnologies.orient.core.exception;

public class OStorageAlreadyClosedException extends OStorageException {
  public OStorageAlreadyClosedException(OStorageAlreadyClosedException exception) {
    super(exception);
  }

  public OStorageAlreadyClosedException(String string) {
    super(string);
  }
}
