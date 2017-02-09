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
import java.util.TimerTask;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * Start the replication with a server. The command is executed to the target server that will require a SYNC DATABASE command.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OStartReplicationTask extends OAbstractReplicatedTask {
  public static final int FACTORYID = 22;

  private String          databaseName;
  private boolean         tryWithDeltaFirst;

  public OStartReplicationTask() {
  }

  public OStartReplicationTask(final String databaseName, final boolean tryWithDeltaFirst) {
    this.databaseName = databaseName;
    this.tryWithDeltaFirst = tryWithDeltaFirst;
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager dManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (dManager.getDatabaseStatus(dManager.getLocalNodeName(), databaseName) == ODistributedServerManager.DB_STATUS.ONLINE)
      // ALREADY ONLINE
      return false;

    dManager.setDatabaseStatus(dManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
    
    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);

    // EXECUTE THE INSTALL DATABASE ASYNCHRONOUSLY
    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        dManager.installDatabase(true, databaseName, dCfg.getDocument(), true, tryWithDeltaFirst);
      }
    }, 1000, 0);

    return true;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.writeBoolean(tryWithDeltaFirst);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    databaseName = in.readUTF();
    tryWithDeltaFirst = in.readBoolean();
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getName() {
    return "start_replication";
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return false;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
