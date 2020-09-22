package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OSystemException;

public class OPromiseException extends OSystemException {
  private static final long serialVersionUID = 1L;

  public OPromiseException(String message) {
    super(message);
  }
}
