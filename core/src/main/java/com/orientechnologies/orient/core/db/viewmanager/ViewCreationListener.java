package com.orientechnologies.orient.core.db.viewmanager;

public interface ViewCreationListener {
  void afterCreate(String viewName);

  void onError(String viewName, Exception exception);
}
