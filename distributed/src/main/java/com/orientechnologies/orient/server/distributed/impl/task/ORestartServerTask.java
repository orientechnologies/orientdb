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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Distributed task to restart a node.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORestartServerTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final int FACTORYID = 10;

  public ORestartServerTask() {}

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      final OServer iServer,
      final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {

    ODistributedServerLog.warn(
        this,
        iManager.getLocalNodeName(),
        getNodeSource(),
        ODistributedServerLog.DIRECTION.IN,
        "Restarting server...");

    iManager.setNodeStatus(ODistributedServerManager.NODE_STATUS.OFFLINE);

    Orient.instance()
        .scheduleTask(
            new Runnable() {
              @Override
              public void run() {
                try {
                  iServer.restart();
                } catch (Exception e) {
                  ODistributedServerLog.error(
                      this,
                      iManager.getLocalNodeName(),
                      getNodeSource(),
                      ODistributedServerLog.DIRECTION.IN,
                      "Error on restarting server",
                      e);
                }
              }
            },
            1,
            0);

    return true;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public String getName() {
    return "restart_server";
  }

  @Override
  public void toStream(DataOutput out) throws IOException {}

  @Override
  public void fromStream(DataInput in, final ORemoteTaskFactory factory) throws IOException {}

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public boolean isUsingDatabase() {
    return false;
  }
}
