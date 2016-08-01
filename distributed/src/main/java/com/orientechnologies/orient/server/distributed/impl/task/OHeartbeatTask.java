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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.util.ODateHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Task to manage the end of distributed transaction when no fix is needed (OFixTxTask) and all the locks must be released. Locks
 * are necessary to prevent concurrent modification of records before the transaction is finished. <br>
 * This task uses the same partition keys used by TxTask to avoid synchronizing all the worker threads (and queues).
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OHeartbeatTask extends OAbstractRemoteTask {
  private static final long             serialVersionUID = 1L;
  public static final int               FACTORYID        = 16;

  private long                          timestamp        = System.currentTimeMillis();

  private final static SimpleDateFormat dateFormat       = new SimpleDateFormat(ODateHelper.DEF_DATETIME_FORMAT);

  public OHeartbeatTask() {
  }

  @Override
  public Object execute(final ODistributedRequestId msgId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (ODistributedServerLog.isDebugEnabled())
      synchronized (dateFormat) {
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
            "Received heartbeat (sourceTimeStamp=%s)", dateFormat.format(new Date(timestamp)));
      }

    // RETURN LOCAL TIME
    return System.currentTimeMillis();
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public boolean isUsingDatabase() {
    return false;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeLong(timestamp);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    timestamp = in.readLong();
  }

  /**
   * Computes the timeout according to the transaction size.
   *
   * @return
   */
  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_HEARTBEAT_TIMEOUT.getValueAsLong();
  }

  @Override
  public long getSynchronousTimeout(final int iSynchNodes) {
    return getDistributedTimeout();
  }

  @Override
  public long getTotalTimeout(final int iTotalNodes) {
    return getDistributedTimeout();
  }

  @Override
  public int[] getPartitionKey() {
    return ANY;
  }

  @Override
  public String getName() {
    return "heartbeat";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public String toString() {
    return getName() + " timestamp: " + timestamp;
  }
}
