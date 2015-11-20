package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
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
