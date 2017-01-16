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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
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
  private final List<ORemoteTask> undoTasks     = new ArrayList<ORemoteTask>();
  private final List<ORID>        acquiredLocks = new ArrayList<ORID>();
  private final long              startedOn     = System.currentTimeMillis();

  public ODistributedTxContextImpl(final ODistributedDatabase iDatabase, final ODistributedRequestId iRequestId) {
    db = iDatabase;
    reqId = iRequestId;
  }

  @Override
  public String toString() {
    return "reqId=" + reqId + " undoTasks=" + undoTasks.size() + " startedOn=" + startedOn;
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
        "Distributed transaction %s: rolling back transaction (%d ops) on database '%s'", reqId, undoTasks.size(), database);

    for (ORemoteTask task : undoTasks) {
      try {

        if (task != null)
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
}
