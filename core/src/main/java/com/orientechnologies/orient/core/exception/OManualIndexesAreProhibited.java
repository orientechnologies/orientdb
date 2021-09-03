package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.exception.OHighLevelException;

/** Exception which is thrown to inform user that manual indexes are prohibited. */
public class OManualIndexesAreProhibited extends OException implements OHighLevelException {
  public OManualIndexesAreProhibited(OManualIndexesAreProhibited exception) {
    super(exception);
  }

  public OManualIndexesAreProhibited(String message) {
    super(message);
  }
}
