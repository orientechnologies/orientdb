package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;

/** Exception is thrown to inform that non-empty component can not be removed. */
public class NotEmptyComponentCanNotBeRemovedException extends OException
    implements OHighLevelException {

  public NotEmptyComponentCanNotBeRemovedException(
      NotEmptyComponentCanNotBeRemovedException exception) {
    super(exception);
  }

  public NotEmptyComponentCanNotBeRemovedException(final String message) {
    super(message);
  }
}
