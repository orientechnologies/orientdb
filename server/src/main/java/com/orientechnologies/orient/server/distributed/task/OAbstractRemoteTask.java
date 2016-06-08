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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

/**
 * Base class for Tasks to be executed remotely.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public abstract class OAbstractRemoteTask implements ORemoteTask {
  private static final long  serialVersionUID = 1L;

  protected transient String nodeSource;

  /**
   * Constructor used from unmarshalling.
   */
  public OAbstractRemoteTask() {
  }

  @Override
  public abstract String getName();

  public abstract OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType();

  @Override
  public abstract Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception;

  @Override
  public int getPartitionKey() {
    return -1;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public long getSynchronousTimeout(final int iSynchNodes) {
    return getDistributedTimeout() * iSynchNodes;
  }

  @Override
  public long getTotalTimeout(final int iTotalNodes) {
    return getDistributedTimeout() * iTotalNodes;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.ANY;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public String getNodeSource() {
    return nodeSource;
  }

  @Override
  public void setNodeSource(String nodeSource) {
    this.nodeSource = nodeSource;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return true;
  }
}
