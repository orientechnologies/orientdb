package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OClusterPositionMapException extends ODurableComponentException {
  public OClusterPositionMapException(OClusterPositionMapException exception) {
    super(exception);
  }

  public OClusterPositionMapException(String message, OClusterPositionMap component) {
    super(message, component);
  }
}
