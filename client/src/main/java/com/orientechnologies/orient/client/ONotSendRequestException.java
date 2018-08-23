package com.orientechnologies.orient.client;

import com.orientechnologies.common.exception.OSystemException;

public class ONotSendRequestException extends OSystemException {
  public ONotSendRequestException(OSystemException exception) {
    super(exception);
  }

  public ONotSendRequestException(String message) {
    super(message);
  }
}
