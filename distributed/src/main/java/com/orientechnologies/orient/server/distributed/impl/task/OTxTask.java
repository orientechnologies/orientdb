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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Distributed transaction task.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OTxTask extends OAbstractReplicatedTask {
  private static final long                   serialVersionUID  = 1L;
  public static final int                     FACTORYID         = 7;
  public static final String                  NON_LOCAL_CLUSTER = "_non_local_cluster";

  private List<OAbstractRecordReplicatedTask> tasks             = new ArrayList<OAbstractRecordReplicatedTask>();

  private transient List<OAbstractRemoteTask> localUndoTasks    = new ArrayList<OAbstractRemoteTask>();
  private transient OTxTaskResult             result;

  public OTxTask() {
  }

  public void add(final OAbstractRecordReplicatedTask iTask) {
    tasks.add(iTask);
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "Committing transaction against db=%s...", database.getName());

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    // CREATE A CONTEXT OF TX
    final ODistributedTxContext reqContext = ddb.registerTxContext(requestId);

    final ODistributedConfiguration dCfg = iManager.getDatabaseConfiguration(database.getName());

    database.begin();
    try {
      final OTransactionOptimistic tx = (OTransactionOptimistic) database.getTransaction();

      result = new OTxTaskResult();

      // REGISTER CREATE FIRST TO RESOLVE TEMP RIDS
      for (OAbstractRecordReplicatedTask task : tasks) {
        if (task instanceof OCreateRecordTask) {
          final OCreateRecordTask createRT = (OCreateRecordTask) task;

          final ORecordId rid = createRT.getRid();
          if (rid != null && rid.isPersistent()) {
            if (rid.getRecord() != null)
              // ALREADY CREATED: SKIP REGISTERING IN TX
              continue;
          }

          final int clId = createRT.clusterId > -1 ? createRT.clusterId
              : createRT.getRid().isValid() ? createRT.getRid().getClusterId() : -1;
          final String clusterName = clId > -1 ? database.getClusterNameById(clId) : null;

          if (dCfg.isServerContainingCluster(iManager.getLocalNodeName(), clusterName))
            tx.addRecord(createRT.getRecord(), ORecordOperation.CREATED, clusterName);
        }
      }

      for (OAbstractRecordReplicatedTask task : tasks) {
        final Object taskResult;

        // CHECK LOCAL CLUSTER IS AVAILABLE ON CURRENT SERVER
        if (!task.checkForClusterAvailability(iManager.getLocalNodeName(), dCfg))
          // SKIP EXECUTION BECAUSE THE CLUSTER IS NOT ON LOCAL NODE: THIS CAN HAPPENS IN CASE OF DISTRIBUTED TX WITH SHARDING
          taskResult = NON_LOCAL_CLUSTER;
        else {
          task.setLockRecords(false);

          reqContext.lock(task.getRid());

          task.checkRecordExists();

          taskResult = task.execute(requestId, iServer, iManager, database);

          reqContext.addUndoTask(task.getUndoTask(requestId));
        }

        result.results.add(taskResult);
      }

      database.commit();

      // SEND BACK CHANGED VALUE TO UPDATE
      for (int i = 0; i < result.results.size(); ++i) {
        final Object currentResult = result.results.get(i);

        if (currentResult == NON_LOCAL_CLUSTER)
          // SKIP IT
          continue;

        final OAbstractRecordReplicatedTask task = tasks.get(i);
        if (task instanceof OCreateRecordTask) {
          // SEND RID + VERSION
          final OCreateRecordTask t = (OCreateRecordTask) task;
          result.results.set(i, new OPlaceholder(t.getRecord()));
        } else if (task instanceof OUpdateRecordTask) {
          // SEND VERSION
          result.results.set(i, task.getRecord().getVersion());
        }
      }

      return result;

    } catch (Throwable e) {
      database.rollback();
      // ddb.popTxContext(requestId);
      reqContext.unlock();

      if (!(e instanceof ONeedRetryException || e instanceof OTransactionException || e instanceof ORecordDuplicatedException
          || e instanceof ORecordNotFoundException))
        // DUMP ONLY GENERIC EXCEPTIONS
        ODistributedServerLog.info(this, getNodeSource(), null, DIRECTION.NONE, "Error on distributed transaction commit", e);

      return e;
    }
  }

  /**
   * Return the partition keys of all the sub-tasks.
   */
  @Override
  public int[] getPartitionKey() {
    if (tasks.size() == 1)
      // ONE TASK, USE THE INNER TASK'S PARTITION KEY
      return tasks.get(0).getPartitionKey();

    // MULTIPLE PARTITION
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
  public ORemoteTask getFixTask(final ODistributedRequest iRequest, final ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, final String executorNodeName, final ODistributedServerManager dManager) {
    if (!(iGoodResponse instanceof OTxTaskResult)) {
      // TODO: MANAGE ERROR ON LOCAL NODE
      ODistributedServerLog.debug(this, getNodeSource(), null, DIRECTION.NONE,
          "Error on creating fix-task for request: '%s' because good response is not expected type: %s", iRequest, iBadResponse);
      return null;
    }

    final OCompletedTxTask fixTask = new OCompletedTxTask(iRequest.getId(), false, null);

    for (int i = 0; i < tasks.size(); ++i) {
      final OAbstractRecordReplicatedTask t = tasks.get(i);

      final Object badResult = iBadResponse instanceof Throwable ? iBadResponse : ((OTxTaskResult) iBadResponse).results.get(i);
      final Object goodResult = ((OTxTaskResult) iGoodResponse).results.get(i);

      final ORemoteTask undoTask = t.getFixTask(iRequest, t, badResult, goodResult, executorNodeName, dManager);
      if (undoTask != null)
        fixTask.addFixTask(undoTask);
    }

    return fixTask;
  }

  @Override
  public ORemoteTask getUndoTask(final ODistributedRequestId reqId) {
    final OCompletedTxTask fixTask = new OCompletedTxTask(reqId, false, null);

    for (ORemoteTask undoTask : localUndoTasks)
      fixTask.addFixTask(undoTask);

    return fixTask;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(tasks.size());
    for (OAbstractRecordReplicatedTask task : tasks)
      out.writeObject(task);
    if (lastLSN != null) {
      out.writeBoolean(true);
      lastLSN.writeExternal(out);
    } else
      out.writeBoolean(false);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    final int size = in.readInt();
    for (int i = 0; i < size; ++i)
      tasks.add((OAbstractRecordReplicatedTask) in.readObject());
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

  @Override
  public String getName() {
    return "tx";
  }

  @Override
  public String getPayload() {
    return null;
  }

  public List<OAbstractRecordReplicatedTask> getTasks() {
    return tasks;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
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

  @Override
  public OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  public void setLastLSN(final OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }
}
