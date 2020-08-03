package com.orientechnologies.orient.setup;

public class TestSetupException extends RuntimeException {
  public TestSetupException(String msg) {
    super(msg);
  }

  public TestSetupException(Exception e) {
    super(e);
  }

  public TestSetupException(String msg, Exception e) {
    super(msg, e);
  }
}
