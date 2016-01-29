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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * Task to manage the end of distributed transaction when no fix is needed (OFixTxTask) and all the locks must be released. Locks
 * are necessary to prevent concurrent modification of records before the transaction is finished.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OCompletedTxTask extends OAbstractReplicatedTask {
  private static final long serialVersionUID = 1L;
  private final Set<ORID>   locks;

  public OCompletedTxTask() {
    locks = new HashSet<ORID>();
  }

  public OCompletedTxTask(final Set<ORID> iLocks) {
    locks = iLocks;
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "completing transaction against db=%s locks=%s...", database.getName(), locks);

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    // UNLOCK ALL LOCKS ACQUIRED IN TX
    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    for (ORID r : locks)
      ddb.unlockRecord(r);

    return Boolean.TRUE;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public OFixTxTask getFixTask(final ODistributedRequest iRequest, OAbstractRemoteTask iOriginalTask, final Object iBadResponse,
      final Object iGoodResponse) {
    return null;
  }

  @Override
  public OAbstractRemoteTask getUndoTask(final ODistributedRequest iRequest, final Object iBadResponse) {
    return null;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeInt(locks.size());
    for (ORID task : locks)
      out.writeObject(task);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    locks.clear();
    final int size = in.readInt();
    for (int i = 0; i < size; ++i)
      locks.add((ORID) in.readObject());
  }

  /**
   * Computes the timeout according to the transaction size.
   *
   * @return
   */
  @Override
  public long getDistributedTimeout() {
    final long to = OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.getValueAsLong();
    return to + (100 * locks.size());
  }

  @Override
  public String getName() {
    return "tx-completed";
  }

  @Override
  public String getPayload() {
    return null;
  }
}
