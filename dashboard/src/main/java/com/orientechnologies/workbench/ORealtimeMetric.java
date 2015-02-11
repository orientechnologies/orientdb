package com.orientechnologies.workbench;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public final class ORealtimeMetric {
  private final OMonitoredServer owner;
  private ODocument              server;
  private long                   maxTTL       = 1000;
  private long                   lastReceived = 0;
  public Map<String, Object>    lastMetrics;

  ORealtimeMetric(final OMonitoredServer iOwner, final ODocument iServer) {
    this.owner = iOwner;
    this.server = iServer;
  }

  public synchronized Map<String, Object> getChrono(final String iName) throws MalformedURLException, IOException {
    fetch(iName);
    return (Map<String, Object>) lastMetrics.get("chronos");
  }

  public synchronized Map<String, Object> getStatistic(final String iName) throws MalformedURLException, IOException {
    fetch(iName);
    return (Map<String, Object>) lastMetrics.get("statistics");
  }

  public synchronized Map<String, Object> getInformation(final String iName) throws MalformedURLException, IOException {
    fetch(iName);
    return (Map<String, Object>) lastMetrics.get("hookValues");
  }

  public synchronized Map<String, Object> getCounter(final String iName) throws MalformedURLException, IOException {
    fetch(iName);
    return (Map<String, Object>) lastMetrics.get("counters");
  }

  public boolean fetch() throws MalformedURLException, IOException {
    return this.fetch(null);
  }

  protected boolean fetch(final String iName) throws MalformedURLException, IOException {
    final long now = System.currentTimeMillis();
//    if (now - lastReceived < maxTTL)
//      return false;

    final String url = server.field("url");
    final String serverName = server.field("name");

    OLogManager.instance().info(this, "MONITOR [%s (%s)]-> request for realtime metrics", serverName, url);

    final URL remoteUrl = new java.net.URL("http://" + url + "/profiler/realtime" + (iName != null ? ("/" + iName) : ""));
    final ODocument docMetrics = new ODocument().fromJSON(OWorkbenchUtils.fetchFromRemoteServer(server, remoteUrl));

    OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Received realtime metrics", serverName, url);

    lastMetrics = docMetrics.field("realtime");
    lastReceived = now;
    return true;
  }

  public synchronized void reset(final String iName) throws MalformedURLException, IOException {
    final String url = server.field("url");
    final String serverName = server.field("name");

    OLogManager.instance().info(this, "MONITOR [%s (%s)]-> resetting realtime metrics", serverName, url);

    final URL remoteUrl = new java.net.URL("http://" + url + "/profiler/reset/" + (iName != null ? ("/" + iName) : ""));
    OWorkbenchUtils.fetchFromRemoteServer(server, remoteUrl);
  }

  protected Map<String, Object> filterEntries(final String iName, final Map<String, Object> iMetricCollection) {
    Map<String, Object> result = new HashMap<String, Object>();
    if (iMetricCollection != null) {
      if (iName.contains("*")) {
        // WILDCARD
        final String toMatch = iName.replace('*', '%');
        for (Entry<String, Object> e : iMetricCollection.entrySet()) {
          if (OQueryHelper.like(e.getKey(), toMatch))
            result.put(e.getKey(), e.getValue());
        }
      } else
        result.put(iName, iMetricCollection.get(iName));
    }
    return result;
  }

}