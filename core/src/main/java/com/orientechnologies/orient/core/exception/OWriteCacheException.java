package com.orientechnologies.orient.core.exception;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public class OWriteCacheException extends OCoreException {

  public OWriteCacheException(OWriteCacheException exception) {
    super(exception);
  }

  public OWriteCacheException(String message) {
    super(message);
  }
}
