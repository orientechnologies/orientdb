package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashTableDirectory;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OHashTableDirectoryException extends ODurableComponentException {
  public OHashTableDirectoryException(OHashTableDirectoryException exception) {
    super(exception);
  }

  public OHashTableDirectoryException(String message, HashTableDirectory component) {
    super(message, component);
  }
}
