package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public class OReadCacheException extends OCoreException {
  public OReadCacheException(String message) {
    super(message);
  }

  public OReadCacheException(String message, ODurableComponent component) {
    super(message, component);
  }

  public OReadCacheException(String message, ODurableComponent component, OErrorCode errorCode) {
    super(message, component, errorCode);
  }

  public OReadCacheException(String message, Throwable cause, ODurableComponent component, OErrorCode errorCode) {
    super(message, component, errorCode);
  }
}
