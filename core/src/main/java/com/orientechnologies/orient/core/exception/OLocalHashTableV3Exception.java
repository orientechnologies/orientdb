package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.hashindex.local.v3.OLocalHashTableV3;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OLocalHashTableV3Exception extends ODurableComponentException {
  public OLocalHashTableV3Exception(OLocalHashTableV3Exception exception) {
    super(exception);
  }

  public OLocalHashTableV3Exception(String message, OLocalHashTableV3 component) {
    super(message, component);
  }
}
