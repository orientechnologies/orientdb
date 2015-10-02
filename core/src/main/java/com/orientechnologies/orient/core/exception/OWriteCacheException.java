package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public class OWriteCacheException extends OCoreException {
  public OWriteCacheException(String message) {
    super(message);
  }

  public OWriteCacheException(String message, ODurableComponent component) {
    super(message, component);
  }

  public OWriteCacheException(String message, ODurableComponent component, OErrorCode errorCode) {
    super(message, component, errorCode);
  }

  public OWriteCacheException(String message, Throwable cause, ODurableComponent component, OErrorCode errorCode) {
    super(message, component, errorCode);
  }
}
