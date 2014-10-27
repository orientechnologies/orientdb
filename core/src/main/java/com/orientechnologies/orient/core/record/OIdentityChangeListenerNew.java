package com.orientechnologies.orient.core.record;

public interface OIdentityChangeListenerNew {

  void onBeforeIdentityChange(ORecord record);

  void onAfterIdentityChange(ORecord record);

}
