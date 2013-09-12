package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin
 * @since 8/30/13
 */
public class OSBTreeException extends OException {
  public OSBTreeException() {
  }

  public OSBTreeException(String message) {
    super(message);
  }

  public OSBTreeException(Throwable cause) {
    super(cause);
  }

  public OSBTreeException(String message, Throwable cause) {
    super(message, cause);
  }
}
