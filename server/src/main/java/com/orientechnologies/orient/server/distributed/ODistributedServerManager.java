/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.util.List;
import java.util.concurrent.locks.Lock;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Server cluster interface to abstract cluster behavior.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODistributedServerManager {

  public enum STATUS {
    OFFLINE, ONLINE, ALIGNING, SHUTDOWNING
  };

  public boolean isEnabled();

  public STATUS getStatus();

  public boolean checkStatus(STATUS string);

  public void setStatus(STATUS iStatus);

  public boolean isNodeAvailable(final String iNodeName);

  public boolean isOfflineNodeById(String iNodeName);

  public String getLocalNodeId();

  public String getLocalNodeName();

  public ODocument getClusterConfiguration();

  public ODocument getNodeConfigurationById(String iNode);

  public ODocument getLocalNodeConfiguration();

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

  public Class<? extends OReplicationConflictResolver> getConfictResolverClass();

  public ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName);

  public ODistributedPartition newPartition(List<String> partition);

  public Object sendRequest(String iDatabaseName, String iClusterName, OAbstractRemoteTask iTask, EXECUTION_MODE iExecutionMode);

  public void sendRequest2Node(String iDatabaseName, String iTargetNodeName, OAbstractRemoteTask iTask);

  public ODistributedPartitioningStrategy getPartitioningStrategy(String partitionStrategy);

  public ODocument getStats();

}
