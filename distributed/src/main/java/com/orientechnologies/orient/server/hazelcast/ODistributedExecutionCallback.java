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
package com.orientechnologies.orient.server.hazelcast;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.hazelcast.core.ExecutionCallback;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

public class ODistributedExecutionCallback implements ExecutionCallback<Object> {
  private final OAbstractRemoteTask<? extends Object> task;
  private final EXECUTION_MODE                        mode;
  private final String                                nodeId;
  private final Map<String, Object>                   results;

  public ODistributedExecutionCallback(OAbstractRemoteTask<? extends Object> iTask, final EXECUTION_MODE iMode, String iNodeId,
      Map<String, Object> iResults) {
    task = iTask;
    mode = iMode;
    nodeId = iNodeId;
    results = iResults;

    results.put(nodeId, null);
  }

  @Override
  public void onResponse(final Object result) {
    results.put(nodeId, result);
  }

  @Override
  public void onFailure(final Throwable e) {
    handleTaskException(this, task, mode, nodeId, results, e);
  }

  public static void handleTaskException(final Object iContext, OAbstractRemoteTask<? extends Object> task,
      final EXECUTION_MODE mode, String nodeId, Map<String, Object> results, final Throwable e) {
    final Throwable t;
    if (e instanceof ExecutionException) {
      if (e.getCause() instanceof ONeedRetryException) {
        // PROPAGATE IT
        ODistributedServerLog.debug(iContext, task.getDistributedServerManager().getLocalNodeId(), nodeId, DIRECTION.OUT,
            "error on execution %d.%d of operation in %s mode raising a ONeedRetryException", task.getRunId(),
            task.getOperationSerial(), mode);
        t = e.getCause();

      } else {
        ODistributedServerLog.error(iContext, task.getDistributedServerManager().getLocalNodeId(), nodeId, DIRECTION.OUT,
            "error on execution of operation %d.%d in %s mode", e, task.getRunId(), task.getOperationSerial(), mode);
        t = new ODistributedException("Error on executing remote operation " + task.getRunId() + "." + task.getOperationSerial()
            + " in " + mode + " mode against node: " + nodeId, e);
      }
    } else if (e instanceof ONeedRetryException) {
      // PROPAGATE IT
      ODistributedServerLog.debug(iContext, task.getDistributedServerManager().getLocalNodeId(), nodeId, DIRECTION.OUT,
          "error on execution %d.%d of operation in %s mode raising a ONeedRetryException", task.getRunId(),
          task.getOperationSerial(), mode);
      t = e;
    } else
      t = e;

    results.put(nodeId, t);
  }
}