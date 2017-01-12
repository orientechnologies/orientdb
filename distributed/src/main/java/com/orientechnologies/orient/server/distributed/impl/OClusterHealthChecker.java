/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.OHeartbeatTask;
import com.orientechnologies.orient.server.distributed.impl.task.ORequestDatabaseConfigurationTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.IOException;
import java.util.*;

/**
 * Timer task that checks periodically the cluster health status.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OClusterHealthChecker extends TimerTask {
  private final ODistributedServerManager manager;

  public OClusterHealthChecker(final ODistributedServerManager manager) {
    this.manager = manager;
  }

  public synchronized void run() {
    // CHECK CURRENT STATUS OF DBS
    OLogManager.instance().debug(this, "Checking cluster health...");
    try {

      checkServerConfig();
      checkServerStatus();
      checkServerInStall();
      checkServerList();

    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
    } catch (Throwable t) {
      if (manager.getServerInstance().isActive())
        OLogManager.instance().error(this, "Error on checking cluster health", t);
      else
        // SHUTDOWN IN PROGRESS
        OLogManager.instance().debug(this, "Error on checking cluster health", t);
    } finally {
      OLogManager.instance().debug(this, "Cluster health checking completed");
    }
  }

  private void checkServerConfig() {
    // NO NODES CONFIGURED: CHECK IF THERE IS ANY MISCONFIGURATION BY CHECKING THE DATABASE STATUSES
    for (String databaseName : manager.getMessageService().getDatabases()) {
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      final Set<String> confServers = cfg.getServers(null);

      for (String s : manager.getActiveServers()) {
        if (manager.isNodeAvailable(s, databaseName) && !confServers.contains(s)) {
          final List<String> nodes = new ArrayList<String>();
          for (String n : manager.getActiveServers()) {
            if (manager.isNodeAvailable(n, databaseName))
              nodes.add(n);
          }

          // THE SERVERS HAS THE DATABASE ONLINE BUT IT IS NOT IN THE CFG. DETERMINE THE MOST UPD CFG
          try {
            final ODistributedResponse response = manager
                .sendRequest(databaseName, null, nodes, new ORequestDatabaseConfigurationTask(databaseName),
                    manager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

            final Object payload = response != null ? response.getPayload() : null;
            if (payload instanceof Map) {
              String mostUpdatedServer = null;
              int mostUpdatedServerVersion = -1;

              final Map<String, Object> responses = (Map<String, Object>) payload;
              for (Map.Entry<String, Object> r : responses.entrySet()) {
                if (r.getValue() instanceof ODocument) {
                  final ODocument doc = (ODocument) r.getValue();
                  int v = doc.field("version");
                  if (v > mostUpdatedServerVersion) {
                    mostUpdatedServerVersion = v;
                    mostUpdatedServer = r.getKey();
                  }
                }
              }

              if (cfg.getVersion() < mostUpdatedServerVersion) {
                // OVERWRITE DB VERSION
                ((ODistributedStorage) manager.getStorage(databaseName)).setDistributedConfiguration(
                    new OModifiableDistributedConfiguration((ODocument) responses.get(mostUpdatedServer)));
              }

            }

          } catch (ODistributedOperationException e) {
            // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
          }

        }
      }
    }
  }

  private void checkServerList() {
    final Set<String> activeServers = manager.getActiveServers();
    for (String server : activeServers) {
      int id = manager.getNodeIdByName(server);
      if (id == -1) {
        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Server '%s' was not found in the list of registered servers. Reloading configuration from cluster...", server);

        ((OHazelcastPlugin) manager).reloadRegisteredNodes(null);
        id = manager.getNodeIdByName(server);
        if (id == -1) {
          if (server.equals(manager.getLocalNodeName())) {
            // LOCAL NODE
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Local server was not found in the list of registered servers after the update", server);

          } else {
            // REMOTE NODE
            ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Server '%s' was not found in the list of registered servers after the update, restarting the server...", server);

            try {
              ((OHazelcastPlugin) manager).restartNode(server);
            } catch (IOException e) {
              ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                  "Error on restarting server '%s' (error=%s)", server, e);
            }
          }
        }
        break;
      }
    }
  }

  private void checkServerStatus() {
    if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // ONLY ONLINE NODE CAN TRY TO RECOVER FOR SINGLE DB STATUS
      return;

    for (String dbName : manager.getMessageService().getDatabases()) {
      final ODistributedServerManager.DB_STATUS localNodeStatus = manager.getDatabaseStatus(manager.getLocalNodeName(), dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.NOT_AVAILABLE)
        // ONLY NOT_AVAILABLE NODE/DB CAN BE RECOVERED
        continue;

      final List<String> servers = manager.getOnlineNodes(dbName);
      servers.remove(manager.getLocalNodeName());

      if (servers.isEmpty()) {
        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "No server are ONLINE for database '%s'. Considering local copy of database as the good one. Setting status=ONLINE...",
            dbName);

        manager.setDatabaseStatus(manager.getLocalNodeName(), dbName, ODistributedServerManager.DB_STATUS.ONLINE);

      } else {
        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Trying to recover current server for database '%s'...", dbName);

        final ODistributedConfiguration dCfg = ((ODistributedStorage) manager.getStorage(dbName)).getDistributedConfiguration();
        if (dCfg != null) {
          final boolean result = manager.installDatabase(true, dbName, false,
              OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());

          if (result)
            ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Recover complete for database '%s'...", dbName);
          else
            ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
                "Recover cannot be completed for database '%s'...", dbName);
        }
      }
    }
  }

  private void checkServerInStall() {
    if (manager.getNodeStatus() != ODistributedServerManager.NODE_STATUS.ONLINE)
      // ONLY ONLINE NODE CAN CHECK FOR OTHERS
      return;

    for (String dbName : manager.getMessageService().getDatabases()) {
      final ODistributedServerManager.DB_STATUS localNodeStatus = manager.getDatabaseStatus(manager.getLocalNodeName(), dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.ONLINE)
        // ONLY ONLINE NODE/DB CAN CHECK FOR OTHERS
        continue;

      final List<String> servers = manager.getOnlineNodes(dbName);
      servers.remove(manager.getLocalNodeName());

      if (servers.isEmpty())
        continue;

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, manager.getLocalNodeName(), servers.toString(), ODistributedServerLog.DIRECTION.OUT,
            "Sending heartbeat message to servers (db=%s)", dbName);

      try {
        final ODistributedResponse response = manager
            .sendRequest(dbName, null, servers, new OHeartbeatTask(), manager.getNextMessageIdCounter(),
                ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

        final Object payload = response != null ? response.getPayload() : null;
        if (payload instanceof Map) {
          final Map<String, Object> responses = (Map<String, Object>) payload;
          servers.removeAll(responses.keySet());
        }

      } catch (ODistributedOperationException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
      }

      for (String server : servers) {
        setDatabaseOffline(dbName, server);
      }
    }
  }

  private void setDatabaseOffline(final String dbName, final String server) {
    if (manager.getDatabaseStatus(server, dbName) != ODistributedServerManager.DB_STATUS.ONLINE)
      return;

    if (OGlobalConfiguration.DISTRIBUTED_CHECK_HEALTH_CAN_OFFLINE_SERVER.getValueAsBoolean()) {
      ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
          "Server '%s' did not respond to the heartbeat message (db=%s, timeout=%dms). Setting the database as NOT_AVAILABLE",
          server, dbName, OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong());

      manager.setDatabaseStatus(server, dbName, ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    } else {

      ODistributedServerLog.warn(this, manager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
          "Server '%s' did not respond to the heartbeat message (db=%s, timeout=%dms), but cannot be set OFFLINE by configuration",
          server, dbName, OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong());
    }
  }
}
