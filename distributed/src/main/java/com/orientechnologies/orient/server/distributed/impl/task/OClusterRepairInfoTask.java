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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * Returns the range of positions for a cluster. This task is used by auto repairer.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OClusterRepairInfoTask extends OAbstractReplicatedTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 19;

  private int               clusterId;

  public OClusterRepairInfoTask() {
  }

  public OClusterRepairInfoTask(final int clusterId) {
    this.clusterId = clusterId;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    final String clusterName = database.getClusterNameById(clusterId);

    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
        "Repair cluster acquiring information about cluster '%s' db=%s (reqId=%s)...", clusterName, database.getName(), requestId);

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    // CREATE A CONTEXT OF TX
    final ODistributedTxContext reqContext = ddb.registerTxContext(requestId);

    // LOCK THE ENTIRE CLUSTER
    reqContext.lock(new ORecordId(clusterId, -1), OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong() * 3);

    // SEND BACK LAST RECORD POSITION
    final long nextPosition = database.getStorage().getUnderlying().getClusterById(clusterId).getNextPosition();
    return nextPosition - 1;
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(clusterId);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    clusterId = in.readInt();
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong() * 6;
  }

  @Override
  public String getName() {
    return "repair_cluster_info";
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
