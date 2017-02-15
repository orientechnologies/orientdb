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
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

/**
 * Repairs records through the distributed server.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ORepairRecordsTask extends OTxTask {
  public static final int FACTORYID = 17;

  public ORepairRecordsTask() {
  }

  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  protected long getRecordLock() {
    return OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong() * 3;
  }

  @Override
  public long getSynchronousTimeout(final int iSynchNodes) {
    return 3000;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public String getName() {
    return "repair_records";
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
