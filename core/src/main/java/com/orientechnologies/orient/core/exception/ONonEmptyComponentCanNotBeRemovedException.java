package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;

/**
 * Exception is thrown to inform that non-empty component can not be removed.
 */
public class ONonEmptyComponentCanNotBeRemovedException extends OException implements OHighLevelException {
  public ONonEmptyComponentCanNotBeRemovedException(final String message) {
    super(message);
  }
}
