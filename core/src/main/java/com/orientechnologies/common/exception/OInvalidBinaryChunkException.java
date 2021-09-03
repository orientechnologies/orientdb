package com.orientechnologies.common.exception;

import java.io.IOException;

public class OInvalidBinaryChunkException extends IOException {
  public OInvalidBinaryChunkException() {}

  public OInvalidBinaryChunkException(String message) {
    super(message);
  }

  public OInvalidBinaryChunkException(String message, Throwable cause) {
    super(message, cause);
  }

  public OInvalidBinaryChunkException(Throwable cause) {
    super(cause);
  }
}
