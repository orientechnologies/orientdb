package com.orientechnologies.workbench;

import com.orientechnologies.common.io.OUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.workbench.OWorkbenchPlugin.STATUS;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.Map.Entry;

public final class OWorkbenchTask extends TimerTask {
  private final OWorkbenchPlugin handler;

  OWorkbenchTask(final OWorkbenchPlugin iHandler) {
    this.handler = iHandler;
  }

  @Override
  public void run() {
    try {
      OLogManager.instance().info(this, "MONITOR contacting configured servers...");

      ODatabaseRecordThreadLocal.INSTANCE.set(handler.getDb());
      handler.updateActiveServerList();

      for (Entry<String, OMonitoredServer> serverEntry : handler.getMonitoredServers()) {
        final String serverName = serverEntry.getKey();
        final ODocument server = serverEntry.getValue().getConfiguration();
        server.reload();
        final Date since = serverEntry.getValue().getLastConnection();
        try {

          updateDictionary(server);
          Map<String, Object> cfg = server.field("configuration");
          // if (cfg != null) {
          // String license = (String) cfg.get("license");
          // int idC = OL.getClientId(license);
          // int idS = OL.getServerId(license);
          //
          // // TO REMOVE
          // if (handler.getKeyMap().size() > 1 && handler.getKeyMap().get(idC).size() > 1) {
          // updateServerStatus(server, OWorkbenchPlugin.STATUS.LICENSE_INVALID);
          // log(server, OWorkbenchPlugin.STATUS.LICENSE_INVALID, "License " + license + " invalid");
          // continue;
          // }
          // }
          createSnapshot(serverEntry.getValue(), fetchSnapshots(server, since));

          // UPDATE SERVER STATUS TO ONLINE
          if (updateServerStatus(server, OWorkbenchPlugin.STATUS.ONLINE)) {
            OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Restored connection", serverName, server.field("url"));
            log(server, OWorkbenchPlugin.STATUS.ONLINE, "Restored connection");
          }
        } catch (Exception e) {
          final String msg = e.toString();
          if (msg.contains("401")) {
            // UPDATE SERVER STATUS TO UNAUTHORIZED
            if (updateServerStatus(server, OWorkbenchPlugin.STATUS.UNAUTHORIZED)) {
              OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Error on reading server metrics", serverName,
                  server.field("url"));
              // log(server, LOG_LEVEL.ERROR, e.toString());
              log(server, OWorkbenchPlugin.STATUS.UNAUTHORIZED, "Unauthorized");
            }
          } else if (msg.contains("405")) {
            if (updateServerStatus(server, STATUS.NO_AGENT)) {
              OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Error on reading server metrics", serverName,
                  server.field("url"));

              log(server, STATUS.NO_AGENT, "No Agent installed");
            }
          } else {
            // UPDATE SERVER STATUS TO OFFLINE
            if (updateServerStatus(server, OWorkbenchPlugin.STATUS.OFFLINE)) {
              OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Error on reading server metrics", serverName,
                  server.field("url"));
              // log(server, LOG_LEVEL.ERROR, e.toString());
              log(server, OWorkbenchPlugin.STATUS.OFFLINE, "Connection refused");

            }
          }
        }
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "MONITOR Error on reading server metrics");
    }
  }

  @SuppressWarnings("unchecked")
  protected void createSnapshot(final OMonitoredServer iMonitoredServer, final List<Map<String, Object>> archive) {

    final ODocument server = iMonitoredServer.getConfiguration();

    if (!archive.isEmpty()) {
      for (Map<String, Object> snapshot : archive) {
        // SNAPSHOT RECORD
        final long from = ((Number) snapshot.get("from")).longValue();
        final long to = ((Number) snapshot.get("to")).longValue();
        final ODocument snap = new ODocument(OWorkbenchPlugin.CLASS_SNAPSHOT).field("server", server).field("dateFrom", from)
            .field("dateTo", to).save();

        // HOOK VALUES
        final Map<String, Object> hookValues = (Map<String, Object>) snapshot.get("hookValues");
        for (Entry<String, Object> hookEntry : hookValues.entrySet()) {
          final String key = hookEntry.getKey();
          final Object value = hookEntry.getValue();

          if (key.startsWith(OWorkbenchPlugin.SYSTEM_CONFIG)) {
            updateServerConfiguration(server, key, value);
          } else {
            Map<String, Object> serverMetrics = iMonitoredServer.getLastSnapshot();
            if (serverMetrics == null) {
              serverMetrics = new HashMap<String, Object>();
              iMonitoredServer.setLastSnapshot(serverMetrics);
            }
            Object metric = serverMetrics.get(key);
            if (metric == null || !metric.equals(value)) {

              if (!Boolean.FALSE.equals(handler.getMetricsEnabled().get(key))) {
                new ODocument(OWorkbenchPlugin.CLASS_INFORMATION).field("snapshot", snap).field("name", key).field("value", value)
                    .save();
              }
              serverMetrics.put(key, value);
            }
          }
        }

        // STATS VALUES
        final Map<String, Object> statsValues = (Map<String, Object>) snapshot.get("statistics");
        for (Entry<String, Object> statEntry : statsValues.entrySet()) {

          Map<String, Object> serverMetrics = iMonitoredServer.getLastSnapshot();
          if (serverMetrics == null) {
            serverMetrics = new HashMap<String, Object>();
            iMonitoredServer.setLastSnapshot(serverMetrics);
          }
          if (!Boolean.FALSE.equals(handler.getMetricsEnabled().get(statEntry.getKey()))) {
            new ODocument(OWorkbenchPlugin.CLASS_STATISTIC).field("snapshot", snap).field("name", statEntry.getKey())
                .field("value", statEntry.getValue()).save();
          }
          serverMetrics.put(statEntry.getKey(), statEntry.getValue());
        }
        // COUNTERS
        final Map<String, Object> counters = (Map<String, Object>) snapshot.get("counters");
        for (Entry<String, Object> counterEntry : counters.entrySet()) {
          Map<String, Object> serverMetrics = iMonitoredServer.getLastSnapshot();
          if (serverMetrics == null) {
            serverMetrics = new HashMap<String, Object>();
            iMonitoredServer.setLastSnapshot(serverMetrics);
          }
          if (!Boolean.FALSE.equals(handler.getMetricsEnabled().get(counterEntry.getKey()))) {
            new ODocument(OWorkbenchPlugin.CLASS_COUNTER).field("snapshot", snap).field("name", counterEntry.getKey())
                .field("value", counterEntry.getValue()).save();
          }
          serverMetrics.put(counterEntry.getKey(), counterEntry.getValue());
        }
        // CHRONOS
        final Map<String, Object> chronos = (Map<String, Object>) snapshot.get("chronos");
        for (Entry<String, Object> chronoEntry : chronos.entrySet()) {
          final Map<String, Object> chrono = (Map<String, Object>) chronoEntry.getValue();

          Map<String, Object> serverMetrics = iMonitoredServer.getLastSnapshot();
          if (serverMetrics == null) {
            serverMetrics = new HashMap<String, Object>();
            iMonitoredServer.setLastSnapshot(serverMetrics);
          }
          if (!Boolean.FALSE.equals(handler.getMetricsEnabled().get(chronoEntry.getKey()))) {
            new ODocument(OWorkbenchPlugin.CLASS_CHRONO).field("snapshot", snap).field("name", chronoEntry.getKey()).fields(chrono)
                .save();
          }
          serverMetrics.put(chronoEntry.getKey(), chronoEntry.getValue());
        }

        iMonitoredServer.setLastConnection(new Date(to));
      }
    }

    if (server.isDirty()) {
      server.save();
    }
  }

  protected boolean updateServerConfiguration(ODocument server, final String key, final Object value) {
    Map<String, Object> cfg = server.field("configuration");
    if (cfg == null) {
      cfg = new HashMap<String, Object>();
      server.field("configuration", cfg);
    }

    final String cfgName = key.substring(OWorkbenchPlugin.SYSTEM_CONFIG.length() + 1).replace('.', '_');
    if (cfg.containsKey(cfgName)) {
      if (!OUtils.equals(cfg.get(cfgName), value)) {
        cfg.put(cfgName, value);
        for (OServerConfigurationListener l : handler.getListeners())
          l.onConfigurationChange(server, cfgName, value);
        return true;
      }
    } else {
      cfg.put(cfgName, value);
      for (OServerConfigurationListener l : handler.getListeners())
        l.onConfigurationChange(server, cfgName, value);
      return true;
    }
    return false;
  }

  /**
   * Changes the server's status only if different by the current one.
   * 
   * @throws IOException
   */
  protected boolean updateDictionary(final ODocument server) throws IOException {
    String url = server.field("url");
    final String serverName = server.field("name");

    OLogManager.instance().info(this, "MONITOR [%s (%s)]-> request for metadata", serverName, url);

    if (!url.startsWith("http://"))
      url = "http://" + url;

    URL remoteUrl = new java.net.URL(url + "/profiler/metadata");
    final ODocument docMetrics = fetchFromRemoteServer(server, remoteUrl);
    Map<String, Map<String, Object>> metadata = docMetrics.field("metadata");

    OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Received  metadata", serverName, url);

    handler.getMetricsEnabled().clear();
    for (Entry<String, Map<String, Object>> entry : metadata.entrySet()) {
      try {
        final String key = entry.getKey();
        Map<String, Object> value = entry.getValue();

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name", key);
        String query = "Select * from Dictionary where name = :name";
        List<ODocument> docs = handler.getDb().query(new OSQLSynchQuery<ORecordSchemaAware>(query), params);
        ODocument doc = null;
        if (docs.size() > 0) {
          doc = docs.iterator().next();
        } else {
          doc = new ODocument(OWorkbenchPlugin.CLASS_DICTIONARY);
          doc.field("name", key);
          doc.field("enabled", Boolean.TRUE);
          doc.field("description", value.get("description"));
          doc.field("type", value.get("type"));
          doc.save();
        }
        Boolean enabled = doc.field("enabled");
        handler.getMetricsEnabled().put(key, enabled);

      } catch (Exception e) {
        // Ignore duplicates
      }
    }
    return false;
  }

  /**
   * Changes the server's status only if different by the current one.
   */
  protected boolean updateServerStatus(final ODocument server, final STATUS iStatus) {
    final String currentStatus = iStatus.toString();

    if (OWorkbenchPlugin.STATUS.ONLINE.equals(iStatus))
      server.field("last_success", System.currentTimeMillis());

    final String status = server.field("status");
    if (!currentStatus.equals(status)) {
      server.field("status", currentStatus);
    }
    server.save();

    return !currentStatus.equals(status);
  }

  protected List<Map<String, Object>> fetchSnapshots(final ODocument server, final Date since) throws MalformedURLException,
      IOException {
    String url = server.field("url");
    final String serverName = server.field("name");

    OLogManager.instance().info(this, "MONITOR [%s (%s)]-> request for updates after %s (%d)", serverName, url, since,
        since.getTime());

    if (!url.startsWith("http://"))
      url = "http://" + url;

    URL remoteUrl = new java.net.URL(url + "/profiler/archive/" + since.getTime() + "/*");
    final ODocument docMetrics = fetchFromRemoteServer(server, remoteUrl);
    final List<Map<String, Object>> archive = docMetrics.field("archive");

    OLogManager.instance().info(this, "MONITOR <-[%s (%s)] Received %s snapshot(s) of metrics", serverName, url, archive.size());

    return archive;
  }

  protected static ODocument fetchFromRemoteServer(final ODocument server, final URL iRemoteUrl) throws IOException {
    URLConnection urlConnection = iRemoteUrl.openConnection();

    String enc = server.field("password");
    if (enc == null) {
      throw new IOException("401");
    }
    String pwd = null;
    try {
      pwd = OL.decrypt(enc);
    } catch (Exception e) {
      e.printStackTrace();
    }
    String authString = server.field("user") + ":" + pwd;
    String authStringEnc = OBase64Utils.encodeBytes(authString.getBytes());

    urlConnection.setRequestProperty("Authorization", "Basic " + authStringEnc);
    urlConnection.connect();
    InputStream is = urlConnection.getInputStream();
    InputStreamReader isr = new InputStreamReader(is);

    int numCharsRead;
    char[] charArray = new char[1024];
    StringBuffer sb = new StringBuffer();
    while ((numCharsRead = isr.read(charArray)) > 0) {
      sb.append(charArray, 0, numCharsRead);
    }
    String result = sb.toString();

    final ODocument docMetrics = new ODocument().fromJSON(result);
    return docMetrics;

  }

  public static void log(final OIdentifiable iServer, final OWorkbenchPlugin.STATUS iLevel, final String iDescription) {

    new ODocument(OWorkbenchPlugin.CLASS_LOG).field("date", new Date()).field("server", iServer).field("level", iLevel.ordinal())
        .field("levelDescription", iLevel.name()).field("description", iDescription).save();

  }
}