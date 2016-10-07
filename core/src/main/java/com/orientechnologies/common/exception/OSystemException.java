package com.orientechnologies.common.exception;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public class OSystemException extends OException {

  public OSystemException(OSystemException exception) {
    super(exception);
  }

  public OSystemException(String message) {
    super(message);
  }
}
