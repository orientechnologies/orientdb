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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Repairs a cluster through the distributed server. This task creates the missing records to realign all the servers to the same
 * clusterPosition, in order to guarantee the RID integrity.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORepairClusterTask extends OTxTask {
  public static final int FACTORYID = 18;
  private int             clusterId;

  public ORepairClusterTask() {
  }

  public ORepairClusterTask(final int clusterId) {
    this.clusterId = clusterId;
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    final String clusterName = database.getClusterNameById(clusterId);

    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
        "Repair cluster: repairing cluster '%s' db=%s (reqId=%s)...", clusterName, database.getName(), requestId);

    ODatabaseRecordThreadLocal.INSTANCE.set(database);
    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    // CREATE A CONTEXT OF TX
    final ODistributedTxContext reqContext = ddb.registerTxContext(requestId);
    try {

      final ODistributedConfiguration dCfg = iManager.getDatabaseConfiguration(database.getName());

      // LOCK INSERTION ON THIS CLUSTER
      reqContext.lock(new ORecordId(clusterId, -1));

      for (OAbstractRecordReplicatedTask task : tasks) {
        final Object taskResult;

        // CHECK LOCAL CLUSTER IS AVAILABLE ON CURRENT SERVER
        if (!task.checkForClusterAvailability(iManager.getLocalNodeName(), dCfg))
          // SKIP EXECUTION BECAUSE THE CLUSTER IS NOT ON LOCAL NODE: THIS CAN HAPPENS IN CASE OF DISTRIBUTED TX WITH SHARDING
          taskResult = NON_LOCAL_CLUSTER;
        else {
          task.setLockRecords(false);

          task.checkRecordExists();

          task.execute(requestId, iServer, iManager, database);

          reqContext.addUndoTask(task.getUndoTask(requestId));
        }
      }
      return null;

    } catch (Throwable e) {
      // if (e instanceof ODistributedRecordLockedException)
      // ddb.dumpLocks();
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
          "Repair cluster: rolling back transaction db=%s (reqId=%s error=%s)...", database.getName(), requestId, e);

      // ddb.popTxContext(requestId);
      reqContext.unlock();

      return e;
    } finally {
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
          "Repair cluster: transaction completed db=%s (reqId=%s)...", database.getName(), requestId);
    }
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    super.toStream(out);
    out.writeInt(clusterId);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    super.fromStream(in, factory);
    clusterId = in.readInt();
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public String getName() {
    return "repair_cluster";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public ORemoteTask getUndoTask(ODistributedRequestId reqId) {
    return null;
  }

  @Override
  public ORemoteTask getFixTask(ODistributedRequest iRequest, ORemoteTask iOriginalTask, Object iBadResponse, Object iGoodResponse,
      String executorNodeName, ODistributedServerManager dManager) {
    return null;
  }
}
