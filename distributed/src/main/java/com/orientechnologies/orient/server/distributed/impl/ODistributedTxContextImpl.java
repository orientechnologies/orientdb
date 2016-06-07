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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores a transaction request that is waiting for the "completed" message (2-phase) by the leader node. Objects of this class are
 * immutable.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedTxContextImpl implements ODistributedTxContext {
  private final ODistributedDatabase  db;
  private final ODistributedRequestId reqId;
  private final List<ORemoteTask>     undoTasks     = new ArrayList<ORemoteTask>();
  private final List<ORID>            acquiredLocks = new ArrayList<ORID>();

  public ODistributedTxContextImpl(final ODistributedDatabase iDatabase, final ODistributedRequestId iRequestId) {
    db = iDatabase;
    reqId = iRequestId;
  }

  public synchronized void lock(final ORID rid) {
    final ODistributedRequestId lockHolder = db.lockRecord(rid, reqId);
    if (lockHolder != null)
      throw new ODistributedRecordLockedException(rid, lockHolder);

    acquiredLocks.add(rid);
  }

  public ODistributedRequestId getReqId() {
    return reqId;
  }

  public synchronized void addUndoTask(final ORemoteTask undoTask) {
    undoTasks.add(undoTask);
  }

  public synchronized void commit() {
    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: committing transaction", reqId);
  }

  public synchronized void fix(final ODatabaseDocumentInternal database, final List<ORemoteTask> fixTasks) {
    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: fixing transaction (db=%s tasks=%d)", reqId, db.getDatabaseName(), fixTasks.size());

    for (ORemoteTask fixTask : fixTasks) {
      try {
        if (fixTask instanceof OAbstractRecordReplicatedTask)
          ((OAbstractRecordReplicatedTask) fixTask).setLockRecords(false);

        fixTask.execute(reqId, db.getManager().getServerInstance(), db.getManager(), database);

      } catch (Exception e) {
        ODistributedServerLog.error(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Error on fixing transaction %s db=%s task=%s", e, reqId, db.getDatabaseName(), fixTask);
      }
    }
  }

  public synchronized int rollback(final ODatabaseDocumentInternal database) {
    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: rolling back transaction (%d ops)", reqId, undoTasks.size());

    for (ORemoteTask task : undoTasks) {
      try {

        db.getManager().executeOnLocalNode(reqId, task, database);

      } catch (Exception e) {
        ODistributedServerLog.error(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Error on rolling back transaction %s task %s", e, reqId, task);
      }
    }
    return undoTasks.size();
  }

  @Override
  public synchronized void destroy() {
    unlock();
    undoTasks.clear();
  }

  @Override
  public void unlock() {
    if (!acquiredLocks.isEmpty()) {
      for (ORID lockedRID : acquiredLocks)
        db.unlockRecord(lockedRID, reqId);
      acquiredLocks.clear();
    }
  }
}
