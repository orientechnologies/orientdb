package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

/**
 * Exception which is thrown by storage when it detects that some pages of files are broken and it
 * switches to "read only" mode.
 */
public class OPageIsBrokenException extends OStorageException implements OHighLevelException {
  @SuppressWarnings("unused")
  public OPageIsBrokenException(OStorageException exception) {
    super(exception);
  }

  public OPageIsBrokenException(String string) {
    super(string);
  }
}
