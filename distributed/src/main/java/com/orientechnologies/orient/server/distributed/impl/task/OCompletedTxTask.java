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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Task to manage the end of distributed transaction when no fix is needed (OFixTxTask) and all the locks must be released. Locks
 * are necessary to prevent concurrent modification of records before the transaction is finished.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OCompletedTxTask extends OAbstractReplicatedTask {
  private static final long     serialVersionUID = 1L;
  public static final int       FACTORYID        = 8;

  private ODistributedRequestId requestId;
  private boolean               success;
  private List<ORemoteTask>     fixTasks         = new ArrayList<ORemoteTask>();

  public OCompletedTxTask() {
  }

  public OCompletedTxTask(final ODistributedRequestId iRequestId, final boolean iSuccess) {
    requestId = iRequestId;
    success = iSuccess;
  }

  public void addFixTask(final ORemoteTask fixTask) {
    fixTasks.add(fixTask);
  }

  @Override
  public Object execute(final ODistributedRequestId msgId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "%s transaction db=%s originalReqId=%s...", (success ? "Committing" : fixTasks.isEmpty() ? "Rolling back" : "Fixing"),
        database.getName(), requestId, requestId);

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    // UNLOCK ALL LOCKS ACQUIRED IN TX
    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    final ODistributedTxContext pRequest = ddb.popTxContext(requestId);
    try {
      if (success) {
        // COMMIT
        if (pRequest != null)
          pRequest.commit();
        else {
          // UNABLE TO FIND TX CONTEXT
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
              "Error on committing transaction %s db=%s", requestId, database.getName());
        }

      } else if (fixTasks.isEmpty()) {
        // ROLLBACK
        if (pRequest != null)
          pRequest.rollback(database);
        else {
          // UNABLE TO FIND TX CONTEXT
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
              "Error on rolling back transaction %s db=%s", requestId, database.getName());
        }
      } else {

        // FIX TRANSACTION CONTENT
        pRequest.fix(database, fixTasks);

      }
    } finally {
      if (pRequest != null)
        pRequest.destroy();
    }

    return Boolean.TRUE;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeObject(requestId);
    out.writeBoolean(success);
    out.writeInt(fixTasks.size());
    for (ORemoteTask task : fixTasks)
      out.writeObject(task);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    requestId = (ODistributedRequestId) in.readObject();
    success = in.readBoolean();
    final int size = in.readInt();
    for (int i = 0; i < size; ++i)
      fixTasks.add((ORemoteTask) in.readObject());
  }

  /**
   * Computes the timeout according to the transaction size.
   *
   * @return
   */
  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getName() {
    return "tx-completed";
  }

  @Override
  public String getPayload() {
    return null;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public String toString() {
    return getName() + " type: " + (success ? "commit" : (fixTasks.isEmpty() ? "rollback" : "fix (" + fixTasks.size() + " ops) [" + fixTasks + "]"));
  }
}
