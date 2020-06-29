package com.orientechnologies.orient.server.distributed.exception;

import com.orientechnologies.common.exception.OException;

public class OTransactionAlreadyPresentException extends OException {

  public OTransactionAlreadyPresentException(String message) {
    super(message);
  }
}
