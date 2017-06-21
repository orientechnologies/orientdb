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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.List;

/**
 * Base class for Replicated tasks.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 *
 */
public abstract class OAbstractReplicatedTask extends OAbstractRemoteTask {
  protected OLogSequenceNumber lastLSN;

  public ORemoteTask getFixTask(final ODistributedRequest iRequest, final ORemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse, final String executorNodeName, final ODistributedServerManager dManager) {
    return null;
  }

  public ORemoteTask getUndoTask(final ODistributedServerManager dManager, final ODistributedRequestId reqId,
      final List<String> servers) {
    return null;
  }

  public OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }
}
