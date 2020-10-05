package com.orientechnologies.orient.server.distributed.exception;

import com.orientechnologies.common.exception.OSystemException;

public class OTxPromiseException extends OSystemException {
  private static final long serialVersionUID = 1L;
  private final int requestedVersion;
  private final int existingVersion;

  public OTxPromiseException(String message, int requestedVersion, int existingVersion) {
    super(message);
    this.requestedVersion = requestedVersion;
    this.existingVersion = existingVersion;
  }

  public int getRequestedVersion() {
    return requestedVersion;
  }

  public int getExistingVersion() {
    return existingVersion;
  }
}
