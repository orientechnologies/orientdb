package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/28/2015
 */
public class OReadCacheException extends OCoreException {

  public OReadCacheException(OReadCacheException exception) {
    super(exception);
  }

  public OReadCacheException(String message) {
    super(message);
  }
}
