package com.orientechnologies.orient.core.processor;

import com.orientechnologies.common.exception.OException;

public class OProcessException extends OException {
  private static final long serialVersionUID = 1L;

  public OProcessException() {
  }

  public OProcessException(String arg0) {
    super(arg0);
  }

  public OProcessException(Throwable arg0) {
    super(arg0);
  }

  public OProcessException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }
}
