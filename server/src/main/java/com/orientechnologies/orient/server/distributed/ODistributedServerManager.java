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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.distributed.ONodeConfig;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.config.OClusterConfiguration;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Server cluster interface to abstract cluster behavior.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface ODistributedServerManager {
  String FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";

  /** Database status. */
  enum DB_STATUS {
    /** The database is not installed. In this status the server does not receive any request. */
    NOT_AVAILABLE,

    /**
     * The database has been put in OFFLINE status. In this status the server does not receive any
     * request.
     */
    OFFLINE,

    /**
     * The database is in synchronization status. This status is set when a synchronization (full or
     * delta) is requested. The node tha accepts the synchronization, is in SYNCHRONIZING mode too.
     * During this status the server receive requests that will be enqueue until the database is
     * ready. Server in SYNCHRONIZING status do not concur in the quorum.
     */
    SYNCHRONIZING,

    /**
     * The database is ONLINE as fully operative. During this status the server is considered in the
     * quorum (if the server's role is MASTER)
     */
    ONLINE,

    /** The database is ONLINE, but is not involved in the quorum. */
    BACKUP
  }

  OServer getServerInstance();

  boolean isEnabled();

  ODistributedServerManager registerLifecycleListener(ODistributedLifecycleListener iListener);

  ODistributedServerManager unregisterLifecycleListener(ODistributedLifecycleListener iListener);

  ORemoteServerController getRemoteServer(String nodeName) throws IOException;

  long getLastClusterChangeOn();

  NODE_STATUS getNodeStatus();

  void setNodeStatus(NODE_STATUS iStatus);

  DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName);

  ODistributedMessageService getMessageService();

  ODistributedDatabase getDatabase(String name);

  ODistributedStrategy getDistributedStrategy();

  void setDistributedStrategy(ODistributedStrategy streatgy);

  long getNextMessageIdCounter();

  void updateLastClusterChange();

  void reassignClustersOwnership(String iNode, String databaseName, boolean canCreateNewClusters);

  int getLocalNodeId();

  String getLocalNodeName();

  OClusterConfiguration getClusterConfiguration();

  String getNodeNameById(int id);

  int getNodeIdByName(String node);

  ONodeConfig getNodeConfigurationByUuid(String iNode, boolean useCache);

  ONodeConfig getLocalNodeConfiguration();

  ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName);

  /**
   * Sends a distributed request against multiple servers.
   *
   * @param iDatabaseName
   * @param iTargetNodeNames
   * @param iTask
   * @return
   */
  ODistributedResponse sendRequest(
      String iDatabaseName, Collection<String> iTargetNodeNames, ORemoteTask iTask);

  /**
   * Sends a distributed request against multiple servers.
   *
   * @param iDatabaseName
   * @param node
   * @param iTask
   * @return
   */
  ODistributedResponse sendSingleRequest(String iDatabaseName, String node, ORemoteTask iTask);

  ODistributedResponse sendRequest(
      String iDatabaseName,
      Collection<String> iTargetNodeNames,
      ORemoteTask iTask,
      ODistributedRequestId messageId,
      Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory);

  /**
   * Returns the task factory manager. During first connect the minor version of the protocol is
   * used.
   */
  ORemoteTaskFactoryManager getTaskFactoryManager();

  Set<String> getActiveServers();

  Set<String> getActiveServerNotLocal();

  File getDefaultDatabaseConfigFile();

  void notifyClients(String databaseName);

  boolean isSyncronizing(String databaseName);

  ODistributedRequestId nextRequestId();
}
