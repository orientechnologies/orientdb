package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/**
 * Created by tglman on 16/05/17.
 */
public class OLiveQueryMonitorRemote implements OLiveQueryMonitor {

  private ODatabaseDocument database;
  private long              monitorId;

  public OLiveQueryMonitorRemote(ODatabaseDocument database, long monitorId) {
    this.database = database;
    this.monitorId = monitorId;
  }

  @Override
  public void unSubscribe() {
  }

  @Override
  public long getMonitorId() {
    return monitorId;
  }
}
