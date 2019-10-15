package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.LocalHashTableV2;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/2/2015
 */
public class OLocalHashTableV2Exception extends ODurableComponentException {
  public OLocalHashTableV2Exception(OLocalHashTableV2Exception exception) {
    super(exception);
  }

  public OLocalHashTableV2Exception(String message, LocalHashTableV2 component) {
    super(message, component);
  }
}
