package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OHashTableDirectoryV2;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OHashTableDirectoryException extends ODurableComponentException {
  public OHashTableDirectoryException(OHashTableDirectoryException exception) {
    super(exception);
  }

  public OHashTableDirectoryException(String message, OHashTableDirectoryV2 component) {
    super(message, component);
  }
}
