package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.orient.core.index.OIndexException;

/**
 * @author Andrey Lomakin
 * @since 18.02.13
 */
public class OIndexMaximumLimitReachedException extends OIndexException {
  public OIndexMaximumLimitReachedException(String string) {
    super(string);
  }

  public OIndexMaximumLimitReachedException(String message, Throwable cause) {
    super(message, cause);
  }
}
