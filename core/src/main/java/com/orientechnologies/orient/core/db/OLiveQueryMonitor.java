package com.orientechnologies.orient.core.db;

/** Created by tglman on 11/05/17. */
public interface OLiveQueryMonitor {

  void unSubscribe();

  int getMonitorId();
}
