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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed two phase commit task. Operations can keep locks on records on the distributed servers. A second message of
 * {@link OCompleted2pcTask} is requested to unlock the records. Note that locks are freed after a (configurable) while anyway.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public abstract class OAbstract2pcTask extends OAbstractReplicatedTask {
  protected static final long   serialVersionUID  = 1L;
  public static final    String NON_LOCAL_CLUSTER = "_non_local_cluster";

  protected List<OAbstractRecordReplicatedTask> tasks = new ArrayList<OAbstractRecordReplicatedTask>();

  protected transient List<OAbstractRemoteTask> localUndoTasks = new ArrayList<OAbstractRemoteTask>();
  protected transient OTxTaskResult result;

  public OAbstract2pcTask() {
  }

  public void add(final OAbstractRecordReplicatedTask iTask) {
    tasks.add(iTask);
  }

  @Override
  public boolean isIdempotent() {
    for (OAbstractRecordReplicatedTask t : tasks)
      if (t != null && !t.isIdempotent())
        return false;
    return true;
  }

  /**
   * Return the partition keys of all the sub-tasks.
   */
  @Override
  public int[] getPartitionKey() {
    if (tasks.size() == 1)
      // ONE TASK, USE THE INNER TASK'S PARTITION KEY
      return tasks.get(0).getPartitionKey();

    // MULTIPLE PARTITIONS
    final int[] partitions = new int[tasks.size()];
    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractRecordReplicatedTask task = tasks.get(i);
      partitions[i] = task.getPartitionKey()[0];
    }

    return partitions;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeInt(tasks.size());
    for (OAbstractRecordReplicatedTask task : tasks) {
      out.writeByte(task.getFactoryId());
      task.toStream(out);
    }
    if (lastLSN != null) {
      out.writeBoolean(true);
      lastLSN.toStream(out);
    } else
      out.writeBoolean(false);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    final int size = in.readInt();
    for (int i = 0; i < size; ++i) {
      final ORemoteTask task = factory.createTask(in.readByte());
      task.fromStream(in, factory);
      tasks.add((OAbstractRecordReplicatedTask) task);
    }
    final boolean hasLastLSN = in.readBoolean();
    if (hasLastLSN)
      lastLSN = new OLogSequenceNumber(in);
  }

  /**
   * Computes the timeout according to the transaction size.
   *
   * @return
   */
  @Override
  public long getDistributedTimeout() {
    final long to = OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
    return to + ((to / 2) * tasks.size());
  }

  public List<OAbstractRecordReplicatedTask> getTasks() {
    return tasks;
  }

  @Override
  public void setNodeSource(final String nodeSource) {
    super.setNodeSource(nodeSource);
    for (OAbstractRecordReplicatedTask t : tasks) {
      t.setNodeSource(nodeSource);
    }
  }

  public void setLocalUndoTasks(final List<OAbstractRemoteTask> undoTasks) {
    this.localUndoTasks = undoTasks;
  }

  public void setLastLSN(final OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();

    buffer.append("tx[");
    buffer.append(tasks.size());
    buffer.append("]{");

    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractRecordReplicatedTask task = tasks.get(i);
      if (i > 0)
        buffer.append(',');
      buffer.append(task);
    }

    buffer.append("}");
    return buffer.toString();
  }
}
