package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

public class OAcquireTimeoutException extends OException {
  public OAcquireTimeoutException(String message) {
    super(message);
  }

  public OAcquireTimeoutException(OException exception) {
    super(exception);
  }
}
