package com.orientechnologies.orient.core.exception;

/** Created by tglman on 19/06/17. */
public class OLiveQueryInterruptedException extends OCoreException {

  public OLiveQueryInterruptedException(OCoreException exception) {
    super(exception);
  }

  public OLiveQueryInterruptedException(String message) {
    super(message);
  }
}
