/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */
package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.distributed.ONodeListenerConfig;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.distributed.impl.task.OEnterpriseStatsTask;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class OServerCommandDistributedManager extends OServerCommandDistributedScope {
  private static final OLogger logger =
      OLogManager.instance().logger(OServerCommandDistributedManager.class);
  private static final String[] NAMES = {
    "GET|distributed/*", "PUT|distributed/*", "POST|distributed/*"
  };

  public OServerCommandDistributedManager(OEnterpriseServer server) {
    super(EnterprisePermissions.SERVER_DISTRIBUTED.toString(), server);
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    iRequest.getData().setCommandInfo("Distributed information");
    return super.execute(iRequest, iResponse);
  }

  private void doPut(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] parts)
      throws IOException {

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    if (command.equalsIgnoreCase("database")) {

      iResponse.send(
          OHttpUtils.STATUS_NOTIMPL_CODE, null, null, OHttpUtils.STATUS_NOTIMPL_CODE, null);
    }
  }

  @Override
  protected void doPost(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws IOException {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: distributed/<command>/[<id>]");
    doPost(iRequest, iResponse, parts);
  }

  @Override
  protected void doPut(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws IOException {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: distributed/<command>/[<id>]");
    doPut(iRequest, iResponse, parts);
  }

  @Override
  protected void doGet(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws IOException {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: distributed/<command>/[<id>]");
    doGet(iRequest, iResponse, parts);
  }

  protected void doPost(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] parts)
      throws IOException {

    final String command = parts[1];

    if (command.equalsIgnoreCase("stop")) {
      if (parts.length < 2)
        throw new IllegalArgumentException("Cannot stop the server: missing server name to stop");

      if (!server.getDatabases().isDistributed())
        throw new OConfigurationException(
            "Cannot stop the server: local server is not distributed");

      OrientDBDistributed dManager = (OrientDBDistributed) server.getDatabases();
      dManager.stopNode(parts[2]);

      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, OHttpUtils.STATUS_OK_DESCRIPTION, null);

    } else if (command.equalsIgnoreCase("restart")) {
      if (parts.length < 2)
        throw new IllegalArgumentException(
            "Cannot restart the server: missing server name to restart");

      if (!server.getDatabases().isDistributed())
        throw new OConfigurationException(
            "Cannot restart the server: local server is not distributed");

      OrientDBDistributed dManager = (OrientDBDistributed) server.getDatabases();
      dManager.restartNode(parts[2]);

      iResponse.send(OHttpUtils.STATUS_OK_CODE, null, null, OHttpUtils.STATUS_OK_DESCRIPTION, null);
    } else if (command.equalsIgnoreCase("syncDatabase")) {
      syncDatabase(iResponse, parts);
    } else {
      throw new IllegalArgumentException(String.format("Command %s not supported", command));
    }
  }

  private void syncDatabase(final OHttpResponse iResponse, final String[] parts)
      throws IOException {
    if (parts.length < 3)
      throw new IllegalArgumentException("Cannot sync database: missing database name");

    OrientDBInternal context = server.getDatabases();

    if (context.isDistributed())
      throw new OConfigurationException("Cannot sync database: local server is not distributed");

    final String database = parts[2];

    boolean installDatabase;
    try {
      installDatabase =
          ((OrientDBDistributed) context).installDatabase(database, false, true).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      installDatabase = false;
    } catch (ExecutionException e) {
      installDatabase = false;
    }

    ODocument document = new ODocument().field("result", installDatabase);
    iResponse.send(
        OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, document.toJSON(""), null);
  }

  private void doGet(
      final OHttpRequest iRequest, final OHttpResponse iResponse, final String[] parts)
      throws IOException {

    final String command = parts[1];
    final String id = parts.length > 2 ? parts[2] : null;

    final ODocument doc;

    // NODE CONFIG
    if (command.equalsIgnoreCase("node")) {

      OClusterConfiguration info = doGetNodeConfig();
      if (info != null) {
        doc = info.getDocument();
      } else {
        doc = null;
      }

    } else if (command.equalsIgnoreCase("database")) {
      ODistributedConfiguration info = doGetDatabaseInfo(server, id);
      if (info != null) {
        doc = info.getDocument();
      } else {
        doc = null;
      }

    } else if (command.equalsIgnoreCase("stats")) {

      if (id != null) {

        doc = singleNodeStats(id);

      } else {
        OClusterConfiguration info = getClusterConfig();
        if (info != null) {
          doc = info.getDocument();
        } else {
          throw new OConfigurationException(
              "Seems that the server is not running in distributed mode");
        }
      }

    } else {
      throw new IllegalArgumentException("Command '" + command + "' not supported");
    }
    if (doc == null) {
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, "{}", null);
    } else {
      iResponse.send(
          OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, doc.toJSON(""), null);
    }
  }

  private ODocument singleNodeStats(final String id) {
    final ODocument doc;

    if (server.getDatabases() instanceof OrientDBDistributed dc) {
      final ODistributedResponse dResponse =
          dc.getPlugin()
              .sendRequest(
                  null,
                  OMultiValue.getSingletonList(id),
                  new OEnterpriseStatsTask(),
                  dc.nextRequestId(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE,
                  null);
      final Object payload = dResponse.getPayload();

      if (payload != null && payload instanceof Map) {
        doc = (ODocument) ((Map<String, Object>) payload).get(id);
        doc.field("member", getMemberConfig(dc.getClusterConfiguration(), id));
      } else doc = new ODocument();

    } else {
      doc = new ODocument().fromJSON(Orient.instance().getProfiler().toJSON("realtime", null));
    }

    return doc;
  }

  public OClusterConfiguration getClusterConfig() {
    if (server.getDatabases() instanceof OrientDBDistributed dc) {
      final OClusterConfiguration doc = dc.getClusterConfiguration();

      final Collection<ONodeConfig> members = doc.getMembers();
      List<String> servers = new ArrayList<>(members.size());
      for (ONodeConfig nodeConf : members) servers.add(nodeConf.getName());

      Set<String> databases = dc.listDatabases(null, null);
      if (databases.isEmpty()) {
        logger.warn("Cannot load stats, no databases on this server");
        return doc;
      }

      final ODistributedResponse dResponse =
          dc.getPlugin()
              .sendRequest(
                  databases.iterator().next(),
                  servers,
                  new OEnterpriseStatsTask(),
                  dc.nextRequestId(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE,
                  null);
      final Object payload = dResponse.getPayload();

      if (payload instanceof Map) {
        doc.setClusterStats((Map<String, ODocument>) payload);
      }

      doc.setDatabaseStatus(calculateDBStatus(doc));
      return doc;
    } else {
      return null;
    }
  }

  private ODocument calculateDBStatus(final OClusterConfiguration cfg) {

    final ODocument doc = new ODocument();
    final Collection<ONodeConfig> members = cfg.getMembers();

    Set<String> databases = new HashSet<String>();
    for (ONodeConfig m : members) {
      final Collection<String> dbs = m.getDatabases();
      for (String db : dbs) {
        databases.add(db);
      }
    }
    for (String database : databases) {
      doc.setProperty(database, singleDBStatus(database));
    }
    return doc;
  }

  private ODocument singleDBStatus(String database) {
    OrientDBDistributed ctx = ((OrientDBDistributed) server.getDatabases());
    ODatabasesTopology dbTopology = ctx.getNodeState().getDatabaseTopology();
    var dbId = dbTopology.getDatabaseId(database).get();
    var members = dbTopology.getMembers(dbId);
    final ODocument entries = new ODocument();
    for (var member : members) {
      var status = dbTopology.getState(dbId, member);
      entries.setProperty(member.toString(), status.toString());
    }
    return entries;
  }

  public ODistributedConfiguration doGetDatabaseInfo(final OServer server, final String id) {
    if (server.getDatabases() instanceof OrientDBDistributed dc) {
      return dc.getDistributedConfiguration(id);
    } else {
      return null;
    }
  }

  public OClusterConfiguration doGetNodeConfig() {
    OClusterConfiguration doc;
    if (server.getDatabases() instanceof OrientDBDistributed dc) {
      doc = dc.getClusterConfiguration();

      final Collection<ONodeConfig> documents = doc.getMembers();
      List<String> servers = new ArrayList<>(documents.size());
      for (ONodeConfig document : documents) servers.add(document.getName());

      final ODistributedResponse dResponse =
          dc.getPlugin()
              .sendRequest(
                  null,
                  servers,
                  new OEnterpriseStatsTask(),
                  dc.nextRequestId(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE,
                  null);
      final Object payload = dResponse.getPayload();

      if (payload != null && payload instanceof Map) {
        for (ONodeConfig document : documents) {
          final String serverName = document.getName();
          Object stats = ((Map<String, Object>) payload).get(serverName);
          if (stats instanceof ODocument) {
            final ODocument dStat = (ODocument) stats;
            addConfiguration("realtime.sizes", document, dStat);
            addConfiguration("realtime.texts", document, dStat);
          }
        }
      }

    } else {
      doc = new OClusterConfiguration();

      final ONodeConfig member = new ONodeConfig();

      member.setName("orientdb");
      member.setStatus("ONLINE");

      final String realtime = Orient.instance().getProfiler().toJSON("realtime", "system.config.");
      ODocument cfg = new ODocument().fromJSON(realtime);

      addConfiguration("realtime.sizes", member, cfg);
      addConfiguration("realtime.texts", member, cfg);

      final List<ONodeListenerConfig> listeners = new ArrayList<>();
      for (OServerNetworkListener listener : server.getNetworkListeners()) {
        listeners.add(
            new ONodeListenerConfig(
                listener.getProtocolType().getSimpleName(), listener.getListeningAddress(true)));
      }
      member.setListeners(listeners);
      member.setDatabases(server.getAvailableStorageNames().keySet());
      doc.addMember(member);
    }
    return doc;
  }

  private void addConfiguration(final String path, final ONodeConfig member, final ODocument cfg) {

    if (member != null) {
      ODocument configuration = member.getConfiguration();

      if (configuration == null) {
        configuration = new ODocument();
      }

      if (cfg != null) {
        final Map<String, Object> eval = (Map) cfg.eval(path);
        if (eval != null) {
          for (String key : eval.keySet()) {
            if (key.startsWith("system.config.")) {
              configuration.field(
                  key.replace("system.config.", "").replace(".", "_"), eval.get(key));
            }
          }
        }
      }
      member.setConfiguration(configuration);
    }
  }

  private ONodeConfig getMemberConfig(final OClusterConfiguration doc, final String node) {

    final Collection<ONodeConfig> documents = doc.getMembers();

    ONodeConfig member = null;
    for (ONodeConfig document : documents) {
      final String name = document.getName();
      if (name.equalsIgnoreCase(node)) {
        member = document;
        break;
      }
    }
    return member;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
