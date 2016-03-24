package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;

/**
 * This exception is thrown if several processes try to access the same storage directory.
 * It is prohibited because may lead to data corruption.
 */
public class OFileLockedByAnotherProcessException extends OException implements OHighLevelException {
  public OFileLockedByAnotherProcessException(String message) {
    super(message);
  }

  public OFileLockedByAnotherProcessException(OFileLockedByAnotherProcessException exception) {
    super(exception);
  }
}
