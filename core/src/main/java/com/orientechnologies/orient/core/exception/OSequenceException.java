package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 2/28/2015
 */
public class OSequenceException  extends OException {
  private static final long serialVersionUID = -2719447287841577672L;

  public OSequenceException(String message, Throwable cause) {
    super(message, cause);
  }

  public OSequenceException(Throwable cause) { super(cause); }

  public OSequenceException(String message) {
    super(message);
  }
}