package com.orientechnologies.workbench;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public final class OMonitoredServer {
  private final OWorkbenchPlugin handler;
  private final ODocument        server;
  private final ORealtimeMetric  realtime;
  private Date                   lastConnection = new Date(0);
  private Map<String, Object>    lastSnapshot;
  private Map<String, Object>    lastDatabaseInfo;

  OMonitoredServer(final OWorkbenchPlugin iHandler, final ODocument iServer) {
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

  public void attach() {

    try {
      final URL remoteUrl = new URL("http://" + getConfiguration().field("url") + "/profiler/start");
      OWorkbenchUtils.sendToRemoteServer(getConfiguration(), remoteUrl, "POST", "");
      getConfiguration().field("attached", true);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void detach() {
    try {
      final URL remoteUrl = new URL("http://" + getConfiguration().field("url") + "/profiler/stop");
      OWorkbenchUtils.sendToRemoteServer(getConfiguration(), remoteUrl, "POST", "");
      getConfiguration().field("attached", false);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Map<String, Object> getDatabasesInfo() {
    try {
      if (lastDatabaseInfo == null) {
        lastDatabaseInfo = getRealtime().getInformation("system.databases");
      }
      return lastDatabaseInfo;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}