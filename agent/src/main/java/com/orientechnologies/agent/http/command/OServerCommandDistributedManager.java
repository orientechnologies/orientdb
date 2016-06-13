/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.util.*;

public class OServerCommandDistributedManager extends OServerCommandDistributedScope {

  private static final String[] NAMES = { "GET|distributed/*", "PUT|distributed/*" };

  public OServerCommandDistributedManager() {
    super("server.profiler");
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: distributed/<command>/[<id>]");

    iRequest.data.commandInfo = "Distributed information";

    try {

      if (iRequest.httpMethod.equals("PUT")) {

        doPut(iRequest, iResponse, parts);
      } else if (iRequest.httpMethod.equals("GET")) {
        doGet(iRequest, iResponse, parts);
      }

    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  private void doPut(final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] parts) throws IOException {

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    if (command.equalsIgnoreCase("database")) {

      final OHazelcastPlugin manager = (OHazelcastPlugin) server.getDistributedManager();

      manager.executeInDistributedDatabaseLock(id, new OCallable<Object, ODistributedConfiguration>() {
        @Override
        public Object call(final ODistributedConfiguration cfg) {
          final ODocument doc = cfg.getDocument().fromJSON(iRequest.content, "noMap");

          Integer version = doc.field("version");
          version++;
          doc.field("version", version);

          return null;
        }
      });

      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, OHttpUtils.STATUS_OK_DESCRIPTION, null);

    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {
    final ODistributedServerManager manager = server.getDistributedManager();

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    final ODocument doc;

    if (command.equalsIgnoreCase("node")) {

      if (manager != null) {
        doc = manager.getClusterConfiguration();
        enhanceStats(doc, (ODocument) manager.getConfigurationMap().get("clusterStats"));
      } else {
        doc = new ODocument();

        final ODocument member = new ODocument();

        member.field("name", "orientdb");
        member.field("status", "ONLINE");

        List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();

        member.field("listeners", listeners, OType.EMBEDDEDLIST);

        String realtime = Orient.instance().getProfiler().toJSON("realtime", "system.config.");
        ODocument cfg = new ODocument().fromJSON(realtime);

        Map<String, Object> eval = (Map) cfg.eval("realtime.hookValues");

        ODocument configuration = new ODocument();
        member.field("configuration", configuration);
        for (String key : eval.keySet()) {
          if (key.startsWith("system.config.")) {
            configuration.field(key.replace("system.config.", "").replace(".", "_"), eval.get(key));
          }
        }
        for (OServerNetworkListener listener : server.getNetworkListeners()) {
          final Map<String, Object> listenerCfg = new HashMap<String, Object>();
          listeners.add(listenerCfg);

          listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
          listenerCfg.put("listen", listener.getListeningAddress(true));
        }
        member.field("databases", server.getAvailableStorageNames().keySet());
        doc.field("members", new ArrayList<ODocument>() {
          {
            add(member);
          }
        });
      }

    } else if (command.equalsIgnoreCase("database")) {

      ODistributedConfiguration cfg = manager.getDatabaseConfiguration(id);
      doc = cfg.getDocument();

    } else if (command.equalsIgnoreCase("stats")) {

      if (id != null) {

        if (manager != null) {
          ODocument clusterStats = (ODocument) manager.getConfigurationMap().get("clusterStats");
          doc = new ODocument().fromMap(clusterStats.<Map<String, Object>> field(id));
        } else {
          doc = new ODocument().fromJSON(Orient.instance().getProfiler().toJSON("realtime", null));
        }
      } else {
        if (manager != null) {
          doc = manager.getClusterConfiguration();
          doc.field("clusterStats", manager.getConfigurationMap().get("clusterStats"));
        } else {
          throw new OConfigurationException("Seems that the server is not running in distributed mode");
        }

      }

    } else {
      throw new IllegalArgumentException("Command '" + command + "' not supported");
    }
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, doc.toJSON(""), null);
  }

  private void enhanceStats(ODocument doc, ODocument clusterStats) {

    Collection<ODocument> documents = doc.field("members");

    if (clusterStats != null) {
      for (ODocument document : documents) {
        String name = document.field("name");

        Map<String, Object> profilerStats = clusterStats.field(name);
        ODocument dStat = new ODocument().fromMap(profilerStats);

        Map<String, Object> eval = (Map) dStat.eval("realtime.hookValues");

        ODocument configuration = new ODocument();
        document.field("configuration", configuration);
        for (String key : eval.keySet()) {
          if (key.startsWith("system.config.")) {
            configuration.field(key.replace("system.config.", "").replace(".", "_"), eval.get(key));
          }
        }

      }
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
