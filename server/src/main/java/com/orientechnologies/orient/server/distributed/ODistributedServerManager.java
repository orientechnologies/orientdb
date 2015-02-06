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

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Server cluster interface to abstract cluster behavior.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public interface ODistributedServerManager {

  public enum NODE_STATUS {
    OFFLINE, STARTING, ONLINE, SHUTTINGDOWN
  };

  public enum DB_STATUS {
    OFFLINE, SYNCHRONIZING, ONLINE
  };

  public boolean isEnabled();

  public Map<String, Object> getConfigurationMap();

  public long getLastClusterChangeOn();

  public NODE_STATUS getNodeStatus();

  public void setNodeStatus(NODE_STATUS iStatus);

  public boolean checkNodeStatus(NODE_STATUS string);

  public DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName);

  public void setDatabaseStatus(String iNode, String iDatabaseName, DB_STATUS iStatus);

  public ODistributedMessageService getMessageService();

  public void updateLastClusterChange();

  public boolean isNodeAvailable(String iNodeName, String databaseName);

  public boolean isOffline();

  public String getLocalNodeId();

  public String getLocalNodeName();

  public ODocument getClusterConfiguration();

  public ODocument getNodeConfigurationById(String iNode);

  public ODocument getLocalNodeConfiguration();

  public void propagateSchemaChanges(ODatabaseInternal iStorage);

  /**
   * Returns a time taking care about the offset with the cluster time. This allows to have a quite precise idea about information
   * on date times, such as logs to determine the youngest in case of conflict.
   *
   * @return
   */
  public long getDistributedTime(long iTme);

  /**
   * Gets a distributed lock
   *
   * @param iLockName
   *          name of the lock
   * @return
   */
  public Lock getLock(String iLockName);

  public ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName);

  public Object sendRequest(String iDatabaseName, Collection<String> iClusterNames, Collection<String> iTargetNodeNames,
      OAbstractRemoteTask iTask, EXECUTION_MODE iExecutionMode);

  public ODocument getStats();
}
