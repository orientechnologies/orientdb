package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.exception.OStorageException;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 4/24/13
 */
public class OBlockedPageException extends OStorageException {
  public OBlockedPageException(String string) {
    super(string);
  }
}
