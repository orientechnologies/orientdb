package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OSystemException;

public class OTxPromiseException extends OSystemException {
  private static final long serialVersionUID = 1L;

  public OTxPromiseException(String message) {
    super(message);
  }
}
