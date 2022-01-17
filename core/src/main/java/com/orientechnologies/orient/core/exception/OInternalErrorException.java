package com.orientechnologies.orient.core.exception;

public class OInternalErrorException extends OCoreException {
  public OInternalErrorException(OInternalErrorException exception) {
    super(exception);
  }

  public OInternalErrorException(String string) {
    super(string);
  }
}
