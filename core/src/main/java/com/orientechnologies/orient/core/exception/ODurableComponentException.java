package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public abstract class ODurableComponentException extends OCoreException {
  public ODurableComponentException(ODurableComponentException exception) {
    super(exception);
  }

  public ODurableComponentException(String message, ODurableComponent component) {
    super(message, component.getName());
  }
}
