package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OSBTreeBonsaiLocalException extends ODurableComponentException {
  public OSBTreeBonsaiLocalException(OSBTreeBonsaiLocalException exception) {
    super(exception);
  }

  public OSBTreeBonsaiLocalException(String message, OSBTreeBonsaiLocal component) {
    super(message, component);
  }
}
