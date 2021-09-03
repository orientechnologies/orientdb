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
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

/**
 * Distributed task to stop a server.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OStopServerTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final int FACTORYID = 9;

  public OStopServerTask() {}

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
        "Stopping current server...");

    Orient.instance()
        .scheduleTask(
            new Runnable() {
              @Override
              public void run() {
                try {
                  iServer.shutdown();
                } catch (Exception e) {
                  ODistributedServerLog.error(
                      this,
                      iManager.getLocalNodeName(),
                      getNodeSource(),
                      ODistributedServerLog.DIRECTION.IN,
                      "Error on stopping current server",
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
    return "stop_server";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public boolean isUsingDatabase() {
    return false;
  }
}
