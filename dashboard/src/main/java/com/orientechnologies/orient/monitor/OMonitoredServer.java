package com.orientechnologies.orient.monitor;

import java.util.Date;
import java.util.Map;

import com.orientechnologies.orient.core.record.impl.ODocument;

public final class OMonitoredServer {
  private final OMonitorPlugin  handler;
  private final ODocument       server;
  private final ORealtimeMetric realtime;

  private Date                  lastConnection = new Date(0);
  private Map<String, Object>   lastSnapshot;

  OMonitoredServer(final OMonitorPlugin iHandler, final ODocument iServer) {
    this.handler = iHandler;
    this.server = iServer;
    realtime = new ORealtimeMetric(this, iServer);
  }

  public Date getLastConnection() {
    return lastConnection;
  }

  public void setLastConnection(Date lastConnection) {
    this.lastConnection = lastConnection;
  }

  public ODocument getConfiguration() {
    return server;
  }

  public Map<String, Object> getLastSnapshot() {
    return lastSnapshot;
  }

  public void setLastSnapshot(final Map<String, Object> iLastSnapshot) {
    lastSnapshot = iLastSnapshot;
  }

  public ORealtimeMetric getRealtime() {
    return realtime;
  }
}