package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.OLiveQueryMonitor;

/**
 * Created by tglman on 16/05/17.
 */
public class OLiveQueryMonitorRemote implements OLiveQueryMonitor {

  private long monitorId;

  public OLiveQueryMonitorRemote(long monitorId) {
    this.monitorId = monitorId;
  }

  @Override
  public void unSubscribe() {
// TODO:
  }

  @Override
  public long getMonitorId() {
    return monitorId;
  }
}
