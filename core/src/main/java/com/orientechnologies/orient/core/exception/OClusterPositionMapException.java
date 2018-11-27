package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OClusterPositionMapException extends ODurableComponentException {
  @SuppressWarnings("unused")
  public OClusterPositionMapException(OClusterPositionMapException exception) {
    super(exception);
  }

  public OClusterPositionMapException(String message, OClusterPositionMap component) {
    super(message, component);
  }
}
