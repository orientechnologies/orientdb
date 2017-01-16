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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OPlaceholder;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Distributed transaction task.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OTxTask extends OAbstract2pcTask {
  public static final int FACTORYID = 7;

  transient ODistributedTxContext reqContext;

  public OTxTask() {
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {
    ODistributedServerLog
        .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN, "Executing transaction db=%s (reqId=%s)...",
            database.getName(), requestId);

    ODatabaseRecordThreadLocal.INSTANCE.set(database);

    final ODistributedDatabase ddb = iManager.getMessageService().getDatabase(database.getName());

    // CREATE A CONTEXT OF TX
    reqContext = ddb.registerTxContext(requestId);

    final ODistributedConfiguration dCfg = iManager.getDatabaseConfiguration(database.getName());

    result = new OTxTaskResult();

    if (tasks.size() == 0)
      // RETURN AFTER REGISTERED THE CONTEXT
      return result;

    database.begin();
    try {
      final OTransactionOptimistic tx = (OTransactionOptimistic) database.getTransaction();

      // REGISTER CREATE FIRST TO RESOLVE TEMP RIDS
      for (OAbstractRecordReplicatedTask task : tasks) {
        if (task instanceof OCreateRecordTask) {
          final OCreateRecordTask createRT = (OCreateRecordTask) task;

          final ORecordId rid = createRT.getRid();
          if (rid != null && rid.isPersistent()) {
            if (rid.getRecord() != null)
              // ALREADY CREATED: SKIP REGISTERING IN TX
              continue;
          }

          final int clId =
              createRT.clusterId > -1 ? createRT.clusterId : createRT.getRid().isValid() ? createRT.getRid().getClusterId() : -1;
          final String clusterName = clId > -1 ? database.getClusterNameById(clId) : null;

          if (dCfg.isServerContainingCluster(iManager.getLocalNodeName(), clusterName))
            tx.addRecord(createRT.getRecord(), ORecordOperation.CREATED, clusterName);
        }
      }

      final List<ORecordId> rids2Lock = new ArrayList<ORecordId>();
      // LOCK ALL THE RECORDS FIRST (ORDERED TO AVOID DEADLOCK)
      for (OAbstractRecordReplicatedTask task : tasks)
        rids2Lock.add(task.getRid());

      Collections.sort(rids2Lock);
      for (ORecordId rid : rids2Lock)
        reqContext.lock(rid, getRecordLock());

      for (OAbstractRecordReplicatedTask task : tasks) {
        final Object taskResult;

        // CHECK LOCAL CLUSTER IS AVAILABLE ON CURRENT SERVER
        if (!task.checkForClusterAvailability(iManager.getLocalNodeName(), dCfg))
          // SKIP EXECUTION BECAUSE THE CLUSTER IS NOT ON LOCAL NODE: THIS CAN HAPPENS IN CASE OF DISTRIBUTED TX WITH SHARDING
          taskResult = NON_LOCAL_CLUSTER;
        else {
          task.setLockRecords(false);

          task.checkRecordExists();

          taskResult = task.execute(requestId, iServer, iManager, database);

          reqContext.addUndoTask(task.getUndoTask(requestId));
        }

        result.results.add(taskResult);
      }

      database.commit();

      // SEND BACK CHANGED VALUE TO UPDATE
      for (int i = 0; i < result.results.size(); ++i) {
        final Object currentResult = result.results.get(i);

        if (currentResult == NON_LOCAL_CLUSTER)
          // SKIP IT
          continue;

        final OAbstractRecordReplicatedTask task = tasks.get(i);
        if (task instanceof OCreateRecordTask) {
          // SEND RID + VERSION
          final OCreateRecordTask t = (OCreateRecordTask) task;
          result.results.set(i, new OPlaceholder(t.getRecord()));
        } else if (task instanceof OUpdateRecordTask) {
          // SEND VERSION
          result.results.set(i, task.getRecord().getVersion());
        }
      }

      return result;

    } catch (Throwable e) {
      // if (e instanceof ODistributedRecordLockedException)
      // ddb.dumpLocks();
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.IN,
          "Rolling back transaction on local server db=%s (reqId=%s error=%s)...", database.getName(), requestId, e);

      database.rollback();
      // ddb.popTxContext(requestId);
      reqContext.unlock();

      if (!(e instanceof ONeedRetryException || e instanceof OTransactionException || e instanceof ORecordDuplicatedException
          || e instanceof ORecordNotFoundException))
        // DUMP ONLY GENERIC EXCEPTIONS
        ODistributedServerLog.info(this, getNodeSource(), null, DIRECTION.NONE, "Error on distributed transaction commit", e);

      return e;
    }
  }

  protected long getRecordLock() {
    return OGlobalConfiguration.DISTRIBUTED_ATOMIC_LOCK_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getName() {
    return "tx";
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
