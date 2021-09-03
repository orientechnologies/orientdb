package com.orientechnologies.orient.core.exception;

/**
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 2/28/2015
 */
public class OSequenceException extends OCoreException {
  private static final long serialVersionUID = -2719447287841577672L;

  public OSequenceException(OSequenceException exception) {
    super(exception);
  }

  public OSequenceException(String message) {
    super(message);
  }
}
