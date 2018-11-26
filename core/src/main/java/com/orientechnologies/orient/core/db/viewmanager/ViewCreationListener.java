package com.orientechnologies.orient.core.db.viewmanager;

import com.orientechnologies.orient.core.db.ODatabaseSession;

public interface ViewCreationListener {
  void afterCreate(ODatabaseSession database, String viewName);

  void onError(String viewName, Exception exception);
}
