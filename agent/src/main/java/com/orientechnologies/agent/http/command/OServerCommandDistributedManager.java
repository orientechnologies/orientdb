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

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedMessageService;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

  private void doPut(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    if (command.equalsIgnoreCase("database")) {

      final OHazelcastPlugin manager = (OHazelcastPlugin) server.getDistributedManager();
      ODocument doc = new ODocument().fromJSON(iRequest.content, "noMap");

      Integer version = doc.field("version");
      version++;
      doc.field("version", version);
      manager.updateCachedDatabaseConfiguration(id, doc, true, true);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, OHttpUtils.STATUS_OK_DESCRIPTION, null);

    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String[] parts) throws IOException {
    final ODistributedServerManager manager = server.getDistributedManager();

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    if (manager != null) {
      final ODocument doc;

      if (command.equalsIgnoreCase("node")) {

        doc = manager.getClusterConfiguration();
        enhanceStats(doc, (ODocument) manager.getConfigurationMap().get("clusterStats"));

      } else if (command.equalsIgnoreCase("queue")) {

        final ODistributedMessageService messageService = manager.getMessageService();
        if (id == null) {
          // RETURN QUEUE NAMES
          final List<String> queues = messageService.getManagedQueueNames();
          doc = new ODocument();
          doc.field("queues", queues);
        } else {
          doc = messageService.getQueueStats(id);
        }

      } else if (command.equalsIgnoreCase("database")) {

        ODistributedConfiguration cfg = manager.getDatabaseConfiguration(id);
        doc = cfg.serialize();

      } else if (command.equalsIgnoreCase("stats")) {

        if (id != null) {

          ODocument clusterStats = (ODocument) manager.getConfigurationMap().get("clusterStats");
          doc = new ODocument().fromMap(clusterStats.<Map<String, Object>> field(id));
        } else {
          doc = manager.getClusterConfiguration();
          doc.field("clusterStats", manager.getConfigurationMap().get("clusterStats"));
        }

      } else
        throw new IllegalArgumentException("Command '" + command + "' not supported");

      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, doc.toJSON(""), null);
    } else {
      throw new OConfigurationException("Seems that the server is not running in distributed mode");
    }
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
