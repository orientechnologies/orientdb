package com.orientechnologies.common.jnr;

public class LastErrorException extends RuntimeException {
  private long errorCode;

  public LastErrorException(long errorCode) {
    super("Error during execution of native call, error code " + errorCode);
    this.errorCode = errorCode;
  }

  public long getErrorCode() {
    return errorCode;
  }
}
