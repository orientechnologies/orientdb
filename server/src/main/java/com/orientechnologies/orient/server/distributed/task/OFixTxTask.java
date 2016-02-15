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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Distributed create record task used for synchronization.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OFixTxTask extends OAbstractRemoteTask {
  private static final long         serialVersionUID = 1L;
  private List<OAbstractRemoteTask> tasks            = new ArrayList<OAbstractRemoteTask>();
  private Set<ORID>                 locks;

  public OFixTxTask() {
  }

  public OFixTxTask(final Set<ORID> iLocks) {
    locks = iLocks;
  }

  public List<OAbstractRemoteTask> getTasks() {
    return tasks;
  }

  public void add(final OAbstractRemoteTask iTask) {
    tasks.add(iTask);
  }

  @Override
  public Object execute(final OServer iServer, final ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {
    ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
        "fixing %d conflicts found during committing transaction against db=%s...", tasks.size(), database.getName());

    ODatabaseRecordThreadLocal.INSTANCE.set(database);
    try {
      OScenarioThreadLocal.executeAsDistributed(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          for (OAbstractRemoteTask task : tasks) {
            if (task instanceof OAbstractRecordReplicatedTask)
              // AVOID LOCKING RECORDS AGAIN BECAUSE ARE ALREADY LOCKED
              ((OAbstractRecordReplicatedTask) task).setLockRecord(false);

            task.execute(iServer, iManager, database);
          }
          return null;
        }
      });

    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during attempt to fix inconsistency between nodes", e);
      return Boolean.FALSE;
    } finally {
      // UNLOCK ALL RIDS IN ANY CASE
      final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());
      if (locks != null)
        for (ORID r : locks)
          ddb.unlockRecord(r);
    }

    return Boolean.TRUE;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    // TASKS
    out.writeInt(tasks.size());
    for (OAbstractRemoteTask task : tasks)
      out.writeObject(task);
    // LOCKS
    if (locks != null) {
      out.writeInt(locks.size());
      for (ORID r : locks)
        out.writeObject(r);
    } else
      out.writeInt(0);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    // TASKS
    final int size = in.readInt();
    for (int i = 0; i < size; ++i)
      tasks.add((OAbstractRemoteTask) in.readObject());
    // LOCKS
    final int lockSize = in.readInt();
    locks = new HashSet<ORID>(lockSize);
    for (int i = 0; i < lockSize; ++i)
      locks.add((ORID) in.readObject());
  }

  @Override
  public String getName() {
    return "fix_tx";
  }
  //
  // @Override
  // public String getQueueName(final String iOriginalQueueName) {
  // return iOriginalQueueName + OCreateRecordTask.SUFFIX_QUEUE_NAME;
  // }
}
