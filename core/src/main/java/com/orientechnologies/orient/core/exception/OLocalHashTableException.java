package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OLocalHashTableV2;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OLocalHashTableException extends ODurableComponentException {
  public OLocalHashTableException(OLocalHashTableException exception) {
    super(exception);
  }

  public OLocalHashTableException(String message, OLocalHashTableV2 component) {
    super(message, component);
  }
}
