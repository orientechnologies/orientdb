package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.index.hashindex.local.OLocalHashTable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OLocalHashTableException extends ODurableComponentException {
  public OLocalHashTableException(OLocalHashTableException exception) {
    super(exception);
  }

  public OLocalHashTableException(String message, OLocalHashTable component) {
    super(message, component);
  }
}
