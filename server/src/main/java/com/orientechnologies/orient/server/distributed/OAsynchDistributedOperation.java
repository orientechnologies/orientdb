/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.Collection;
import java.util.Set;

/**
 * Asynchronous distributed operation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OAsynchDistributedOperation {
  private final String databaseName;
  private final Set<String> clusterNames;
  private final Collection<String> nodes;
  private final ORemoteTask task;
  private final long messageId;
  private final OCallable<Object, OPair<ODistributedRequestId, Object>> callback;
  private final Object localResult;
  private final OCallable<Void, ODistributedRequestId> afterRequestCallback;

  public OAsynchDistributedOperation(
      final String iDatabaseName,
      final Set<String> iClusterNames,
      final Collection<String> iNodes,
      final ORemoteTask iTask,
      final long iMessageId,
      final Object iLocalResult,
      final OCallable<Void, ODistributedRequestId> iAfterRequestCallback,
      final OCallable<Object, OPair<ODistributedRequestId, Object>> iCallback) {
    databaseName = iDatabaseName;
    clusterNames = iClusterNames;
    nodes = iNodes;
    task = iTask;
    messageId = iMessageId;
    callback = iCallback;
    localResult = iLocalResult;
    afterRequestCallback = iAfterRequestCallback;
  }

  public Set<String> getClusterNames() {
    return clusterNames;
  }

  public Collection<String> getNodes() {
    return nodes;
  }

  public ORemoteTask getTask() {
    return task;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public OCallable<Object, OPair<ODistributedRequestId, Object>> getCallback() {
    return callback;
  }

  public Object getLocalResult() {
    return localResult;
  }

  public OCallable<Void, ODistributedRequestId> getAfterSendCallback() {
    return afterRequestCallback;
  }

  public long getMessageId() {
    return messageId;
  }
}
