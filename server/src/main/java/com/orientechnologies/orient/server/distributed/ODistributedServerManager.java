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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * Server cluster interface to abstract cluster behavior.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODistributedServerManager {

  public enum EXECUTION_MODE {
    SYNCHRONOUS, ASYNCHRONOUS, FIRE_AND_FORGET
  }

  public String getStatus();

  public boolean checkStatus(String string);

  public void setStatus(String iStatus);

  public boolean isOfflineNode(String iNodeId);

  public boolean isLocalNodeMaster(Object iKey);

  public OReplicationConfig getReplicationData(String iDatabaseName, String iClusterName, Object iKey, String iSourceNodeId,
      String iDestinationNodeId);

  public Collection<String> getSynchronousReplicaNodes(String iDatabaseName, String iClusterName, Object iKey);

  public Collection<String> getAsynchronousReplicaNodes(String iDatabaseName, String iClusterName, Object iKey);

  public Object execute(String iClusterName, Object iKey, OAbstractRemoteTask<?> iTask, OReplicationConfig iReplicationConfig)
      throws ExecutionException;

  public Object sendOperation2Node(String iNodeId, OAbstractRemoteTask<?> iTask) throws ODistributedException;

  public Map<String, Object> propagate(Set<String> iNodeIds, OAbstractRemoteTask<?> iTask) throws ODistributedException;

  public String getLocalNodeId();

  public Set<String> getRemoteNodeIds();

  public Set<String> getRemoteNodeIdsBut(String... iNodeId);

  public ODocument getDatabaseStatus(String iDatabaseName);

  public ODocument getDatabaseConfiguration(String iDatabaseName);

  public ODocument getClusterConfiguration();

  public ODocument getNodeConfiguration(String iNode);

  public ODocument getLocalNodeConfiguration();

  /**
   * Returns the offset in milliseconds as difference between the current date time and the central cluster time. This allows to
   * have a quite precise idea about information on date times, such as logs to determine the youngest in case of conflict.
   * 
   * @return
   */
  public long getTimeOffset();

  public long getRunId();

  public long incrementDistributedSerial(String iDatabaseName);

  public OStorageSynchronizer getDatabaseSynchronizer(String iDatabaseName);

  /**
   * Communicates the alignment has been postponed. Current server will send an updated request of alignment against the postponed
   * node.
   */
  public void postponeAlignment(String iNode, String iDatabaseName);

  public void endAlignment(String nodeSource, String databaseName);

  /**
   * Gets a distributed lock
   * 
   * @param iLockName
   *          name of the lock
   * @return
   */
  public Lock getLock(String iLockName);

  public Class<? extends OReplicationConflictResolver> getConfictResolverClass();

  public Object enqueueLocalExecution(OAbstractReplicatedTask<?> iTask) throws Exception;

  public void resetOperationQueue(long iCurrentRunId, long iOperationSerial);
}
