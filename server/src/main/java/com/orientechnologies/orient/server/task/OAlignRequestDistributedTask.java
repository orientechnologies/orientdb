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
package com.orientechnologies.orient.server.task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;

/**
 * Distributed align request task used for synchronization.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAlignRequestDistributedTask extends OAbstractDistributedTask<Integer> {
  private static final long  serialVersionUID = 1L;

  protected long             lastRunId;
  protected long             lastOperationId;
  protected static final int OP_BUFFER        = 50;

  public OAlignRequestDistributedTask() {
  }

  public OAlignRequestDistributedTask(final String nodeSource, final String iDbName, final EXECUTION_MODE iMode,
      final long iLastRunId, final long iLastOperationId) {
    super(nodeSource, iDbName, iMode);
    lastRunId = iLastRunId;
    lastOperationId = iLastOperationId;
  }

  @Override
  public Integer call() throws Exception {
    OLogManager.instance().warn(this, "DISTRIBUTED <-[%s/%s] align request starting from %d.%d", nodeSource, databaseName,
        lastRunId, lastOperationId);

    int aligned = 0;

    final ODistributedServerManager dManager = getDistributedServerManager();

    final String localNode = dManager.getLocalNodeId();

    final OStorageSynchronizer synchronizer = getDatabaseSynchronizer();
    final ODatabaseJournal log = synchronizer.getLog();

    final OMultipleDistributedTasks tasks = new OMultipleDistributedTasks(localNode, databaseName, EXECUTION_MODE.SYNCHRONOUS);
    final List<Long> positions = new ArrayList<Long>();

    for (Iterator<Long> it = log.browse(new long[] { lastRunId, lastOperationId }); it.hasNext();) {
      final long pos = it.next();

      final OAbstractDistributedTask<?> operation = log.getOperation(pos);

      OLogManager.instance().warn(this, "DISTRIBUTED ->[%s/%s] operation %s", nodeSource, databaseName, operation);

      operation.setNodeSource(localNode);
      operation.setDatabaseName(databaseName);
      operation.setMode(EXECUTION_MODE.SYNCHRONOUS);

      tasks.addTask(operation);
      positions.add(pos);

      if (tasks.getTasks() >= OP_BUFFER)
        aligned += flushBufferedTasks(dManager, synchronizer, tasks, positions);
    }

    if (tasks.getTasks() > 0)
      aligned += flushBufferedTasks(dManager, synchronizer, tasks, positions);

    OLogManager.instance().warn(this, "DISTRIBUTED ->[%s/%s] aligned %d operations", nodeSource, databaseName, aligned);

    // SEND TO THE REQUESTER NODE THE TASK TO EXECUTE
    dManager.sendOperation2Node(nodeSource, new OAlignResponseDistributedTask(localNode, databaseName,
        EXECUTION_MODE.FIRE_AND_FORGET, aligned));

    return aligned;
  }

  protected int flushBufferedTasks(final ODistributedServerManager dManager, final OStorageSynchronizer synchronizer,
      final OMultipleDistributedTasks tasks, final List<Long> positions) throws IOException {

    // SEND TO THE REQUESTER NODE THE TASK TO EXECUTE
    @SuppressWarnings("unused")
    final Object[] result = (Object[]) dManager.sendOperation2Node(nodeSource, tasks);

    for (int i = 0; i < positions.size(); ++i)
      tasks.getTask(i).setAsCompleted(synchronizer, positions.get(i));

    final int aligned = tasks.getTasks();

    // REUSE THE MULTIPLE TASK
    tasks.clearTasks();
    positions.clear();

    return aligned;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeLong(lastRunId);
    out.writeLong(lastOperationId);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    lastRunId = in.readLong();
    lastOperationId = in.readLong();
  }

  @Override
  public String getName() {
    return "align_request";
  }
}
