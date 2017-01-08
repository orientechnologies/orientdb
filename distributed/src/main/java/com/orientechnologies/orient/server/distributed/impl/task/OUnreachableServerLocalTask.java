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
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Task executed when a server becomes unreachable. It's responsible to rollback all the transactions and free all the locks owned
 * by the unreachable server. This is executed as task to block any concurrent operation and to be in the request queue after any
 * request sent by the unreachable server and not executed yet.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OUnreachableServerLocalTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final  int  FACTORYID        = 28;

  private String unreachableServer;

  public OUnreachableServerLocalTask(final String unreachableServer) {
    this.unreachableServer = unreachableServer;
  }

  /**
   * Execute the task with no concurrency.
   */
  @Override
  public int[] getPartitionKey() {
    return ALL;
  }

  @Override
  public Object execute(final ODistributedRequestId msgId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "Freeing all the resources owned by the unreachable server '%s'...", unreachableServer);

    final ODistributedDatabase dDatabase = iManager.getMessageService().getDatabase(database.getName());

    dDatabase.unlockResourcesOfServer(database, unreachableServer);

    return Boolean.TRUE;
  }

  @Override
  public String getName() {
    return "unreachable-server";
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public String toString() {
    return getName() + " server: " + unreachableServer;
  }
}
