package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;

public class OInvalidDatabaseNameException extends OException implements OHighLevelException {

  public OInvalidDatabaseNameException(final String message) {
    super(message);
  }

  public OInvalidDatabaseNameException(final OInvalidDatabaseNameException exception) {
    super(exception);
  }
}
