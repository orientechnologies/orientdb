package com.orientechnologies.common.concur;

import com.orientechnologies.common.exception.OSystemException;

/** Created by tglman on 29/12/15. */
public class OOfflineNodeException extends OSystemException {
  public OOfflineNodeException(OOfflineNodeException exception) {
    super(exception);
  }

  public OOfflineNodeException(String message) {
    super(message);
  }
}
