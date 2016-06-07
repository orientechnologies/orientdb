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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.TimerTask;

/**
 * Distributed task to restart a node.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class ORestartServerTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final int   FACTORYID        = 10;

  public ORestartServerTask() {
  }

  @Override
  public Object execute(ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
        "Restarting server...");

    iManager.setNodeStatus(ODistributedServerManager.NODE_STATUS.OFFLINE);

    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        try {
          iServer.restart();
        } catch (Exception e) {
          ODistributedServerLog.error(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
              "Error on restarting server", e);
        }
      }
    }, 1, 0);

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
  public void writeExternal(ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
