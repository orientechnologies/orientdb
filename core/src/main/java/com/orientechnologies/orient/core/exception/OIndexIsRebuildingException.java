package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

public class OIndexIsRebuildingException extends ORetryQueryException {
  public OIndexIsRebuildingException(String message) {
    super(message);
  }
}
