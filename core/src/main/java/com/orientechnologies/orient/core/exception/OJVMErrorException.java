package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

/**
 * This exception is thrown if storage is switched to read-only mode because OrientDB observed
 * {@link Error} during data processing.
 */
public class OJVMErrorException extends OStorageException implements OHighLevelException {
  public OJVMErrorException(OJVMErrorException exception) {
    super(exception);
  }

  public OJVMErrorException(String string) {
    super(string);
  }
}
