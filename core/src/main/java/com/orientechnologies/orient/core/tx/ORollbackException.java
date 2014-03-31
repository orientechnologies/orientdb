package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 3/11/14
 */
public class ORollbackException extends OException {
  public ORollbackException() {
  }

  public ORollbackException(String message) {
    super(message);
  }

  public ORollbackException(Throwable cause) {
    super(cause);
  }

  public ORollbackException(String message, Throwable cause) {
    super(message, cause);
  }
}