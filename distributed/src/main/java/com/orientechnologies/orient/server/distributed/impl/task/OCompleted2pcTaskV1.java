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
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Task to manage the end of distributed transaction when no fix is needed (OFixTxTask) and all the locks must be released. Locks
 * are necessary to prevent concurrent modification of records before the transaction is finished. <br> This task uses the same
 * partition keys used by TxTask to avoid synchronizing all the worker threads (and queues).
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OCompleted2pcTaskV1 extends OCompleted2pcTask {
  private int[] partitionKey;

  public OCompleted2pcTaskV1() {
  }

  public OCompleted2pcTaskV1 init(final ODistributedRequestId iRequestId, final boolean iSuccess, final int[] partitionKey) {
    this.requestId = iRequestId;
    this.success = iSuccess;
    this.partitionKey = partitionKey;
    return this;
  }

  /**
   * Uses the same partition keys used by the original TX, so the 2 tasks are executed in sequence.
   */
  @Override
  public int[] getPartitionKey() {
    return partitionKey;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    requestId.toStream(out);
    out.writeBoolean(success);
    out.writeInt(fixTasks.size());
    for (ORemoteTask task : fixTasks) {
      out.writeByte(task.getFactoryId());
      task.toStream(out);
    }

    // ADDED IN V1
    out.writeInt(partitionKey.length);
    for (int p : partitionKey)
      out.writeInt(p);
  }

  @Override
  public void fromStream(final DataInput in, ORemoteTaskFactory taskFactory) throws IOException {
    requestId = new ODistributedRequestId();
    requestId.fromStream(in);
    success = in.readBoolean();
    final int tasksSize = in.readInt();
    for (int i = 0; i < tasksSize; ++i) {
      final ORemoteTask task = taskFactory.createTask(in.readByte());
      task.fromStream(in, taskFactory);
      fixTasks.add(task);
    }

    // ADDED IN V1
    final int partitionKeyLength = in.readInt();
    partitionKey = new int[partitionKeyLength];
    for (int i = 0; i < partitionKeyLength; ++i)
      partitionKey[i] = in.readInt();
  }
}
