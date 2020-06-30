package com.orientechnologies.orient.test.util;

public class OTestSetupException extends RuntimeException {
  public OTestSetupException(String msg) {
    super(msg);
  }

  public OTestSetupException(Exception e) {
    super(e);
  }

  public OTestSetupException(String msg, Exception e) {
    super(msg, e);
  }
}
