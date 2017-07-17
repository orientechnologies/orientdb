package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

/**
 * This exception is thrown if it is impossible to read data from file which contains state of 2Q cache.
 *
 * @see com.orientechnologies.orient.core.storage.cache.local.twoq.O2QCache#loadCacheState(OWriteCache)
 */
public class OLoadCacheStateException extends OSystemException {
  public OLoadCacheStateException(OSystemException exception) {
    super(exception);
  }

  public OLoadCacheStateException(String message) {
    super(message);
  }
}
