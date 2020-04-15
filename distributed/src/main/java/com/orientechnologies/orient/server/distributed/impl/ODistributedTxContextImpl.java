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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stores a transaction request that is waiting for the "completed" message (2-phase) by the leader node. Objects of this class are
 * immutable.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODistributedTxContextImpl implements ODistributedTxContext {
  private final ODistributedDatabase  db;
  private final ODistributedRequestId reqId;
  private final List<ORemoteTask>     undoTasks     = new ArrayList<ORemoteTask>();
  private final List<ORID>            acquiredLocks = new ArrayList<ORID>();
  private final long                  startedOn     = System.currentTimeMillis();
  private final AtomicBoolean         canceled      = new AtomicBoolean(false);

  public ODistributedTxContextImpl(final ODistributedDatabase iDatabase, final ODistributedRequestId iRequestId) {
    db = iDatabase;
    reqId = iRequestId;
  }

  @Override
  public String toString() {
    return "reqId=" + reqId + " undoTasks=" + undoTasks.size() + " startedOn=" + startedOn;
  }

  @Override
  public Set<ORecordId> cancel(final ODistributedServerManager dManager, final ODatabaseDocumentInternal database) {
    canceled.set(true);

    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: canceled (locks=%s undo=%d startedOn=%s thread=%d)", reqId, acquiredLocks, undoTasks.size(),
        new Date(this.startedOn), Thread.currentThread().getId());

    final ODistributedResponseManager respMgr = dManager.getMessageService().getResponseManager(reqId);
    if (respMgr != null)
      respMgr.cancel();

    final Set<ORecordId> ridsInvolved = rollback(database);

    destroy();

    return ridsInvolved;
  }

  @Override
  public synchronized void lock(ORID rid) {
    lock(rid, -1);
  }

  @Override
  public synchronized void lock(ORID rid, long timeout) {
    if (timeout < 0)
      timeout = OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsInteger();

    if (!rid.isPersistent())
      // CREATE A COPY TO MAINTAIN THE LOCK ON THE CLUSTER AVOIDING THE RID IS TRANSFORMED IN PERSISTENT. THIS ALLOWS TO HAVE
      // PARALLEL TX BECAUSE NEW RID LOCKS THE ENTIRE CLUSTER.
      rid = new ORecordId(rid.getClusterId(), -1l);

    if (db.lockRecord(rid, reqId, timeout))
      // NEW LOCK (FALSE=LOCK WAS ALREADY TAKEN. THIS CAN HAPPEN WITH CREATE, BECAUSE THE RID IS ON CLUSTER ID ONLY (LIKE #25:-1),
      // SO 2 CREATE OPERATIONS AGAIN THE SAME CLUSTER RESULT IN 2 LOCKS AGAINST THE SAME RESOURCE
      acquiredLocks.add(rid);
  }

  @Override
  public void lockIndexKey(Object rid) {
    throw new UnsupportedOperationException();
  }

  public ODistributedRequestId getReqId() {
    return reqId;
  }

  public synchronized void addUndoTask(final ORemoteTask undoTask) {
    undoTasks.add(undoTask);
  }

  public synchronized void commit(ODatabaseDocumentInternal database) {
    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: committing transaction on database '%s' (locks=%s thread=%d)", reqId, db.getDatabaseName(),
        acquiredLocks, Thread.currentThread().getId());
  }

  public synchronized void fix(final ODatabaseDocumentInternal database, final List<ORemoteTask> fixTasks) {
    executeFix(this, this, database, fixTasks, reqId, db);
  }

  public synchronized Set<ORecordId> rollback(final ODatabaseDocumentInternal database) {
    ODistributedServerLog.debug(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: rolling back transaction on database '%s' (undo=%d tx=%s)", reqId,
        database != null ? database.getName() : "?", undoTasks.size(), database.getTransaction().isActive());

    final Set<ORecordId> rids = new HashSet<ORecordId>();

    for (ORemoteTask task : undoTasks) {
      try {

        if (task != null) {
          db.getManager().executeOnLocalNode(reqId, task, database);

          if (task instanceof OAbstractRecordReplicatedTask)
            rids.add(((OAbstractRecordReplicatedTask) task).getRid());
        }

      } catch (Exception e) {
        ODistributedServerLog.error(this, db.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Error on rolling back transaction %s task %s", e, reqId, task);
      }
    }

    return rids;
  }

  public boolean isCanceled() {
    return canceled.get();
  }

  @Override
  public synchronized void destroy() {
    unlock();
    clearUndo();
  }

  @Override
  public synchronized void clearUndo() {
    undoTasks.clear();
  }

  @Override
  public synchronized void unlock() {
    if (!acquiredLocks.isEmpty()) {
      for (ORID lockedRID : acquiredLocks)
        db.unlockRecord(lockedRID, reqId);
      acquiredLocks.clear();
    }
  }

  public long getStartedOn() {
    return startedOn;
  }

  public static void executeFix(final Object me, final ODistributedTxContext context, final ODatabaseDocumentInternal database,
      final List<ORemoteTask> fixTasks, ODistributedRequestId requestId, final ODistributedDatabase ddb) {
    ODistributedServerLog.debug(me, ddb.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
        "Distributed transaction %s: fixing transaction (db=%s tasks=%d)", requestId, ddb.getDatabaseName(), fixTasks.size());

    // LOCK ALL THE RECORDS FIRST BY CANCELING ANY PENDING LOCKS
    final Set<ORID> locked = new HashSet<ORID>();
    for (ORemoteTask fixTask : fixTasks) {
      if (fixTask instanceof OAbstractRecordReplicatedTask) {
        final ORecordId rid = ((OAbstractRecordReplicatedTask) fixTask).getRid();
        if (ddb.forceLockRecord(rid, requestId))
          locked.add(rid);
      }
    }

    try {
      for (ORemoteTask fixTask : fixTasks) {
        try {
          if (fixTask instanceof OAbstractRecordReplicatedTask)
            // AVOID LOCKING BECAUSE LOCKS WAS ALREADY ACQUIRED IN CONTEXT
            ((OAbstractRecordReplicatedTask) fixTask).setLockRecords(false);

          fixTask.execute(requestId, ddb.getManager().getServerInstance(), ddb.getManager(), database);

        } catch (Exception e) {
          ODistributedServerLog.debug(me, ddb.getManager().getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Error on fixing transaction %s db=%s task=%s", e, requestId, ddb.getDatabaseName(), fixTask);
        }
      }
    } finally {
      // UNLOCK ALL THE RECORDS (THE TX IS FINISHED)
      for (ORID r : locked)
        ddb.unlockRecord(r, requestId);
    }
  }

  @Override
  public OTransactionOptimistic getTransaction() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void begin(ODatabaseDocumentInternal distributed, boolean local) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OTransactionId getTransactionId() {
    throw new UnsupportedOperationException();
  }
}
