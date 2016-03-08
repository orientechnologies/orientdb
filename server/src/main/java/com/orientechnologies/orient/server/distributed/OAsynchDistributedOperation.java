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
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.List;
import java.util.Set;

/**
 * Asynchronous distributed operation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAsynchDistributedOperation {
  private final String       databaseName;
  private final Set<String>  clusterNames;
  private final List<String> nodes;
  private final ORemoteTask  task;
  private final OCallable    callback;
  private final int          quorumOffset;

  public OAsynchDistributedOperation(final String iDatabaseName, final Set<String> iClusterNames, final List<String> iNodes,
      final ORemoteTask iTask, final int iQuorumOffset) {
    this(iDatabaseName, iClusterNames, iNodes, iTask, null, iQuorumOffset);
  }

  public OAsynchDistributedOperation(final String iDatabaseName, final Set<String> iClusterNames, final List<String> iNodes,
      final ORemoteTask iTask, final OCallable iCallback, final int iQuorumOffset) {
    databaseName = iDatabaseName;
    clusterNames = iClusterNames;
    nodes = iNodes;
    task = iTask;
    callback = iCallback;
    quorumOffset = iQuorumOffset;
  }

  public Set<String> getClusterNames() {
    return clusterNames;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public ORemoteTask getTask() {
    return task;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public OCallable getCallback() {
    return callback;
  }

  public int getQuorumOffset() {
    return quorumOffset;
  }

}
