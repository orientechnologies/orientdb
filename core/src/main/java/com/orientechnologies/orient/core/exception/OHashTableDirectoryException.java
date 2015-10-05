package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.index.hashindex.local.OHashTableDirectory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OHashTableDirectoryException extends ODurableComponentException {
  public OHashTableDirectoryException(OHashTableDirectoryException exception) {
    super(exception);
  }

  public OHashTableDirectoryException(String message, OHashTableDirectory component) {
    super(message, component);
  }
}
