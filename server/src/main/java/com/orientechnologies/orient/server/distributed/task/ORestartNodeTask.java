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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

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
public class ORestartNodeTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;

  public ORestartNodeTask() {
  }

  @Override
  public Object execute(final OServer iServer, final ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
        "restarting server...");

    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        try {
          iServer.restart();
        } catch (Exception e) {
          ODistributedServerLog.error(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.IN,
              "error on restarting server", e);
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
    return "unjoin_node";
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
  }
}
