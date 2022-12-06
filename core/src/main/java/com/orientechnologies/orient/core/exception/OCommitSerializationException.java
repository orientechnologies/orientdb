package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class OCommitSerializationException extends OCoreException implements OHighLevelException {

  private static final long serialVersionUID = -1157631679527219263L;

  public OCommitSerializationException(OCommitSerializationException exception) {
    super(exception);
  }

  public OCommitSerializationException(String message) {
    super(message);
  }
}
