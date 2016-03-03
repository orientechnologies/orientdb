package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

public abstract class ORetryQueryException extends OException {
  public ORetryQueryException(String message) {
    super(message);
  }
}
