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

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Asynchronous sistributed operation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OAsynchDistributedOperation {
  private final String              databaseName;
  private final Set<String>         clusterNames;
  private final Collection<String>  nodes;
  private final OAbstractRemoteTask task;

  public OAsynchDistributedOperation(final String iDatabaseName, final Set<String> iClusterNames, final Collection<String> iNodes,
      final OAbstractRemoteTask iTask) {
    databaseName = iDatabaseName;
    clusterNames = iClusterNames;
    nodes = iNodes;
    task = iTask;
  }

  public Set<String> getClusterNames() {
    return clusterNames;
  }

  public Collection<String> getNodes() {
    return nodes;
  }

  public OAbstractRemoteTask getTask() {
    return task;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
