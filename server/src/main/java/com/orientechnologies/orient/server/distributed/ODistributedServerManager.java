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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Server cluster interface to abstract cluster behavior.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public interface ODistributedServerManager {
  String FILE_DISTRIBUTED_DB_CONFIG = "distributed-config.json";

  enum NODE_STATUS {
    OFFLINE, STARTING, ONLINE, SHUTTINGDOWN
  };

  enum DB_STATUS {
    OFFLINE, SYNCHRONIZING, ONLINE, BACKUP
  };

  boolean isNodeAvailable(final String iNodeName);

  Set<String> getAvailableNodeNames(String databaseName);

  void waitUntilNodeOnline() throws InterruptedException;

  void waitUntilNodeOnline(final String nodeName, final String databaseName) throws InterruptedException;

  OServer getServerInstance();

  boolean isEnabled();

  ODistributedServerManager registerLifecycleListener(ODistributedLifecycleListener iListener);

  ODistributedServerManager unregisterLifecycleListener(ODistributedLifecycleListener iListener);

  Serializable executeOnLocalNode(ODistributedRequestId reqId, ORemoteTask task, ODatabaseDocumentInternal database);

  ORemoteServerController getRemoteServer(final String nodeName) throws IOException;

  Map<String, Object> getConfigurationMap();

  long getLastClusterChangeOn();

  NODE_STATUS getNodeStatus();

  void setNodeStatus(NODE_STATUS iStatus);

  boolean checkNodeStatus(NODE_STATUS string);

  DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName);

  void setDatabaseStatus(String iNode, String iDatabaseName, DB_STATUS iStatus);

  ODistributedMessageService getMessageService();

  void updateCachedDatabaseConfiguration(String iDatabaseName, ODocument cfg, boolean iSaveToDisk, boolean iDeployToCluster);

  long getNextMessageIdCounter();

  void updateLastClusterChange();

  /**
   * Available means not OFFLINE, so ONLINE or SYNCHRONIZING.
   */
  boolean isNodeAvailable(String iNodeName, String databaseName);

  /**
   * Returns true if the node status is ONLINE.
   */
  boolean isNodeOnline(String iNodeName, String databaseName);

  int getAvailableNodes(final String iDatabaseName);

  int getAvailableNodes(final Collection<String> iNodes, final String databaseName);

  boolean isOffline();

  int getLocalNodeId();

  String getLocalNodeName();

  ODocument getClusterConfiguration();

  String getNodeNameById(int id);

  int getNodeIdByName(String node);

  ODocument getNodeConfigurationByUuid(String iNode);

  ODocument getLocalNodeConfiguration();

  void propagateSchemaChanges(ODatabaseInternal iStorage);

  /**
   * Gets a distributed lock
   *
   * @param iLockName
   *          name of the lock
   * @return
   */
  Lock getLock(String iLockName);

  ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName);

  /**
   * Sends a distributed request against multiple servers.
   * 
   * @param iDatabaseName
   * @param iClusterNames
   * @param iTargetNodeNames
   * @param iTask
   * @param messageId
   *          Message Id as long
   * @param iExecutionMode
   * @param localResult
   *          It's the result of the request executed locally
   *
   * @param iAfterSentCallback
   * @return
   */
  ODistributedResponse sendRequest(String iDatabaseName, Collection<String> iClusterNames, Collection<String> iTargetNodeNames,
      ORemoteTask iTask, long messageId, EXECUTION_MODE iExecutionMode, Object localResult,
      OCallable<Void, ODistributedRequestId> iAfterSentCallback);

  ODocument getStats();

  Throwable convertException(Throwable original);

  ORemoteTaskFactory getTaskFactory();
}
