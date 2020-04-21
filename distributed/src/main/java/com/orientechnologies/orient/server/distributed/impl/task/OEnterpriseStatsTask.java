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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Distributed message between server to exchange stats.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OEnterpriseStatsTask extends OAbstractRemoteTask {
  public static final int FACTORYID = 29;

  public OEnterpriseStatsTask() {
  }

  @Override
  public Object execute(final ODistributedRequestId msgId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    if (Orient.instance().getProfiler().getStatsAsJson() != null) {
      return new ODocument().fromJSON(Orient.instance().getProfiler().getStatsAsJson());
    } else {
      return new ODocument();
    }
  }

  /**
   * Uses the UNLOCK queue that is never blocked.
   */
  @Override
  public int[] getPartitionKey() {
    return FAST_NOLOCK;
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
  public String getName() {
    return "enterprise_stats";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
