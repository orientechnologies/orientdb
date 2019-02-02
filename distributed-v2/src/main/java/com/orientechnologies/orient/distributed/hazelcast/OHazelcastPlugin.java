/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.distributed.hazelcast;

import com.orientechnologies.common.io.OUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.impl.ODistributedAbstractPlugin;
import com.orientechnologies.orient.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.core.db.config.ONodeConfiguration;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.network.protocol.OBeforeDatabaseOpenNetworkEventListener;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Hazelcast implementation for clustering.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements OBeforeDatabaseOpenNetworkEventListener {

  public static final String CONFIG_DATABASE_PREFIX = "database.";

  // THIS MAP IS BACKED BY HAZELCAST EVENTS. IN THIS WAY WE AVOID TO USE HZ MAP DIRECTLY

  public OHazelcastPlugin() {
  }

  // Must be set before config() is called.
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    super.config(iServer, iParams);
  }

  @Override
  protected ONodeConfiguration getNodeConfiguration() {
    return null;
  }

  @Override
  public void startup() {
  }

  @Override
  public boolean isWriteQuorumPresent(final String databaseName) {
    final ODistributedConfiguration cfg = getDatabaseConfiguration(databaseName);
    if (cfg != null) {
      final int availableServers = getAvailableNodes(databaseName);
      if (availableServers == 0)
        return false;

      final int quorum = cfg.getWriteQuorum(null, cfg.getMasterServers().size(), getLocalNodeName());
      return availableServers >= quorum;
    }
    return false;
  }

  @Override
  public int getNodeIdByName(final String name) {
    return -1;
  }

  @Override
  public String getNodeNameById(final int id) {
    return null;
  }

  @Override
  public Throwable convertException(final Throwable original) {
    return original;
  }

  @Override
  public long getClusterTime() {
    return -1;

  }

  @Override
  public void shutdown() {
  }

  public ORemoteServerController getRemoteServer(final String rNodeName) throws IOException {
    if (rNodeName == null)
      throw new IllegalArgumentException("Server name is NULL");

    return null;

  }

  @Override
  public String getPublicAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> getConfigurationMap() {
    return null;
  }

  public boolean updateCachedDatabaseConfiguration(final String databaseName, final OModifiableDistributedConfiguration cfg,
      final boolean iDeployToCluster) {
    return false;
  }

  public void notifyClients(String databaseName) {
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
  }

  public ODocument getNodeConfigurationByUuid(final String iNodeId, final boolean useCache) {
    return null;
  }

  @Override
  public DB_STATUS getDatabaseStatus(final String iNode, final String iDatabaseName) {
    return DB_STATUS.NOT_AVAILABLE;
  }

  @Override
  public void setDatabaseStatus(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
  }

  private void invokeOnDatabaseStatusChange(final String iNode, final String iDatabaseName, final DB_STATUS iStatus) {
    // NOTIFY DB/NODE IS CHANGING STATUS
    for (ODistributedLifecycleListener l : listeners) {
      try {
        l.onDatabaseChangeStatus(iNode, iDatabaseName, iStatus);
      } catch (Exception e) {
        // IGNORE IT
      }

    }
  }

  public boolean removeNodeFromConfiguration(final String nodeLeftName, final String databaseName,
      final boolean removeOnlyDynamicServers, final boolean statusOffline) {
    ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
        "Removing server '%s' from database configuration '%s' (removeOnlyDynamicServers=%s)...", nodeLeftName, databaseName,
        removeOnlyDynamicServers);

    final OModifiableDistributedConfiguration cfg = getDatabaseConfiguration(databaseName).modify();

    if (removeOnlyDynamicServers) {
      // CHECK THE SERVER IS NOT REGISTERED STATICALLY
      final String dc = cfg.getDataCenterOfServer(nodeLeftName);
      if (dc != null) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in data center '%s' configuration for database '%s'", nodeLeftName,
            dc, databaseName);
        return false;
      }

      // CHECK THE SERVER IS NOT REGISTERED IN SERVERS
      final Set<String> registeredServers = cfg.getRegisteredServers();
      if (registeredServers.contains(nodeLeftName)) {
        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Cannot remove server '%s' because it is enlisted in 'servers' of the distributed configuration for database '%s'",
            nodeLeftName, databaseName);
        return false;
      }
    }

    final boolean found = executeInDistributedDatabaseLock(databaseName, 20000, cfg,
        new OCallable<Boolean, OModifiableDistributedConfiguration>() {
          @Override
          public Boolean call(OModifiableDistributedConfiguration cfg) {
            return cfg.removeServer(nodeLeftName) != null;
          }
        });

    final DB_STATUS nodeLeftStatus = getDatabaseStatus(nodeLeftName, databaseName);
    if (statusOffline && nodeLeftStatus != DB_STATUS.OFFLINE)
      setDatabaseStatus(nodeLeftName, databaseName, DB_STATUS.OFFLINE);
    else if (!statusOffline && nodeLeftStatus != DB_STATUS.NOT_AVAILABLE)
      setDatabaseStatus(nodeLeftName, databaseName, DB_STATUS.NOT_AVAILABLE);

    return found;
  }

  @Override
  public void removeServer(final String nodeLeftName, final boolean removeOnlyDynamicServers) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  /**
   * Elects a new server as coordinator. The election browse the ordered server list.
   */
  @Override
  public String electNewLockManager() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public Set<String> getActiveServers() {
    return new HashSet<>();
  }

  @Override
  public void onBeforeDatabaseOpen(final String url) {
    final ODistributedDatabaseImpl dDatabase = getMessageService().getDatabase(OUtils.getDatabaseNameFromURL(url));
    if (dDatabase != null)
      dDatabase.waitForOnline();
  }

}
