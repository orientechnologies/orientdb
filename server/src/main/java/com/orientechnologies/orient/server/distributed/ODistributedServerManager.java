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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Server cluster interface to abstract cluster behavior.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public interface ODistributedServerManager {
  String FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";

  /** Server status. */
  enum NODE_STATUS {
    /** The server was never started or the shutdown is complete. */
    OFFLINE,

    /** The server is STARTING. */
    STARTING,

    /** The server is ONLINE. */
    ONLINE,

    /** The server starts to merge to another cluster. */
    MERGING,

    /** The server is shutting down. */
    SHUTTINGDOWN
  };

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

  /**
   * Checks the node status if it's one of the statuses received as argument.
   *
   * @param iNodeName Node name
   * @param iDatabaseName Database name
   * @param statuses vararg of statuses
   * @return true if the node's status is equals to one of the passed statuses, otherwise false
   */
  boolean isNodeStatusEqualsTo(String iNodeName, String iDatabaseName, DB_STATUS... statuses);

  boolean isNodeAvailable(String iNodeName);

  Set<String> getAvailableNodeNames(String databaseName);

  @Deprecated
  String getCoordinatorServer();

  void waitUntilNodeOnline() throws InterruptedException;

  void waitUntilNodeOnline(String nodeName, String databaseName) throws InterruptedException;

  OServer getServerInstance();

  boolean isEnabled();

  ODistributedServerManager registerLifecycleListener(ODistributedLifecycleListener iListener);

  ODistributedServerManager unregisterLifecycleListener(ODistributedLifecycleListener iListener);

  Object executeOnLocalNode(
      ODistributedRequestId reqId, ORemoteTask task, ODatabaseDocumentInternal database);

  void executeOnLocalNodeFromRemote(ODistributedRequest request);

  ORemoteServerController getRemoteServer(String nodeName) throws IOException;

  long getLastClusterChangeOn();

  NODE_STATUS getNodeStatus();

  void setNodeStatus(NODE_STATUS iStatus);

  boolean checkNodeStatus(NODE_STATUS status);

  void removeServer(String nodeLeftName, boolean removeOnlyDynamicServers);

  DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName);

  void setDatabaseStatus(String iNode, String iDatabaseName, DB_STATUS iStatus);

  int getNodesWithStatus(Collection<String> iNodes, String databaseName, DB_STATUS... statuses);

  ODistributedMessageService getMessageService();

  ODistributedStrategy getDistributedStrategy();

  void setDistributedStrategy(ODistributedStrategy streatgy);

  // This is always used with deployToCluster=true!
  boolean updateCachedDatabaseConfiguration(
      String iDatabaseName, OModifiableDistributedConfiguration cfg);

  long getNextMessageIdCounter();

  String getNodeUuidByName(String name);

  void updateLastClusterChange();

  void reassignClustersOwnership(
      String iNode,
      String databaseName,
      OModifiableDistributedConfiguration cfg,
      boolean canCreateNewClusters);

  /** Available means not OFFLINE, so ONLINE or SYNCHRONIZING. */
  boolean isNodeAvailable(String iNodeName, String databaseName);

  /** Returns true if the node status is ONLINE. */
  boolean isNodeOnline(String iNodeName, String databaseName);

  int getTotalNodes(String iDatabaseName);

  int getAvailableNodes(String iDatabaseName);

  int getAvailableNodes(Collection<String> iNodes, String databaseName);

  boolean isOffline();

  int getLocalNodeId();

  String getLocalNodeName();

  ODocument getClusterConfiguration();

  String getNodeNameById(int id);

  int getNodeIdByName(String node);

  ODocument getNodeConfigurationByUuid(String iNode, boolean useCache);

  ODocument getLocalNodeConfiguration();

  ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName);

  ODistributedConfiguration getDatabaseConfiguration(
      String iDatabaseName, boolean createIfNotPresent);

  /**
   * Sends a distributed request against multiple servers.
   *
   * @param iDatabaseName
   * @param iClusterNames
   * @param iTargetNodeNames
   * @param iTask
   * @param messageId Message Id as long
   * @param iExecutionMode
   * @param localResult It's the result of the request executed locally
   * @return
   */
  ODistributedResponse sendRequest(
      String iDatabaseName,
      Collection<String> iClusterNames,
      Collection<String> iTargetNodeNames,
      ORemoteTask iTask,
      long messageId,
      EXECUTION_MODE iExecutionMode,
      Object localResult);

  ODistributedResponse sendRequest(
      String iDatabaseName,
      Collection<String> iClusterNames,
      Collection<String> iTargetNodeNames,
      ORemoteTask iTask,
      long messageId,
      EXECUTION_MODE iExecutionMode,
      Object localResult,
      ODistributedResponseManagerFactory responseManagerFactory);

  ODocument getStats();

  Throwable convertException(Throwable original);

  List<String> getOnlineNodes(String iDatabaseName);

  boolean installDatabase(
      boolean iStartup, String databaseName, boolean forceDeployment, boolean tryWithDeltaFirst);

  /**
   * Returns the task factory manager. During first connect the minor version of the protocol is
   * used.
   */
  ORemoteTaskFactoryManager getTaskFactoryManager();

  Set<String> getActiveServers();

  /**
   * Returns the cluster-wide time in milliseconds.
   *
   * <p>Cluster tries to keep a cluster-wide time which might be different than the member's own
   * system time. Cluster-wide time is -almost- the same on all members of the cluster.
   */
  long getClusterTime();

  File getDefaultDatabaseConfigFile();

  ODistributedLockManager getLockManagerRequester();

  ODistributedLockManager getLockManagerExecutor();

  /**
   * Executes an operation protected by a distributed lock (one per database).
   *
   * @param <T> Return type
   * @param databaseName Database name
   * @param timeoutLocking
   * @param iCallback Operation @return The operation's result of type T
   */
  <T> T executeInDistributedDatabaseLock(
      String databaseName,
      long timeoutLocking,
      OModifiableDistributedConfiguration lastCfg,
      OCallable<T, OModifiableDistributedConfiguration> iCallback);

  /**
   * Returns true if the quorum is present in terms of number of available nodes for full
   * replication only. With sharding, instead, the quorum may depend on the involved clusters.
   *
   * @return
   */
  boolean isWriteQuorumPresent(String databaseName);

  void notifyClients(String databaseName);

  default void messageReceived(ODistributedRequest request) {}

  default void messagePartitionCalculate(
      ODistributedRequest request, Set<Integer> involvedWorkerQueues) {}

  default void messageBeforeOp(String op, ODistributedRequestId requestId) {}

  default void messageAfterOp(String op, ODistributedRequestId requestId) {}

  default void messageCurrentPayload(ODistributedRequestId requestId, Object responsePayload) {}

  default void messageProcessStart(ODistributedRequest message) {}

  default void messageProcessEnd(ODistributedRequest iRequest, Object responsePayload) {}

  ODocument getOnlineDatabaseConfiguration(String databaseName);
}
