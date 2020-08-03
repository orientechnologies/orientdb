package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.versionmap.OVersionPositionMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OVersionPositionMapException extends ODurableComponentException {
  @SuppressWarnings("unused")
  public OVersionPositionMapException(OVersionPositionMapException exception) {
    super(exception);
  }

  public OVersionPositionMapException(String message, OVersionPositionMap component) {
    super(message, component);
  }
}
