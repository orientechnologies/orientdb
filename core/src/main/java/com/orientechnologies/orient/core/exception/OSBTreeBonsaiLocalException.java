package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.v1.OSBTreeBonsaiLocalV1;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OSBTreeBonsaiLocalException extends ODurableComponentException {
  public OSBTreeBonsaiLocalException(OSBTreeBonsaiLocalException exception) {
    super(exception);
  }

  public OSBTreeBonsaiLocalException(String message, OSBTreeBonsaiLocalV1 component) {
    super(message, component);
  }
}
