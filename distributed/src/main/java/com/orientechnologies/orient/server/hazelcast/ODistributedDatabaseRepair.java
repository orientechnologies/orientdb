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
package com.orientechnologies.orient.server.hazelcast;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.conflict.ODistributedConflictResolver;
import com.orientechnologies.orient.server.distributed.impl.task.ODeleteRecordTask;
import com.orientechnologies.orient.server.distributed.impl.task.ORepairReadRecordTask;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateRecordTask;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background thread that assure the database is always consistent even in case of conflicts.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedDatabaseRepair extends OSoftThread {
  private final ODistributedServerManager             dManager;
  private final ArrayBlockingQueue<ORecordRepairData> queue               = new ArrayBlockingQueue<ORecordRepairData>(10000);
  private final AtomicLong                            processed           = new AtomicLong(0);
  private final AtomicLong                            coherent            = new AtomicLong(0);
  private final AtomicLong                            conflicts           = new AtomicLong(0);
  private final AtomicLong                            totalTimeProcessing = new AtomicLong(0);

  private class ORecordRepairData {
    final String    databaseName;
    final ORecordId rid;

    public ORecordRepairData(final String databaseName, final ORecordId rid) {
      this.databaseName = databaseName;
      this.rid = rid;
    }
  }

  public ODistributedDatabaseRepair(final ODistributedServerManager manager) {
    this.dManager = manager;
  }

  /**
   * Add to the queue the record to check. If the record is full, the object is simply discarded.
   *
   * @param databaseName
   * @param rid
   */
  public void repair(final String databaseName, final ORecordId rid) {
    try {
      queue.offer(new ORecordRepairData(databaseName, rid));
    } catch (IllegalStateException e) {
      ODistributedServerLog.warn(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error on adding record %s to the check queue because it is full");
    }
  }

  @Override
  protected void execute() throws Exception {
    try {
      final ORecordRepairData data = queue.take();
      processed.incrementAndGet();

      final long start = System.currentTimeMillis();

      final ODatabaseDocumentInternal db = dManager.getMessageService().getDatabase(data.databaseName).getDatabaseInstance();
      try {

        final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(data.databaseName);
        final String clusterName = db.getClusterNameById(data.rid.clusterId);

        final List<String> clusterNames = new ArrayList(1);
        clusterNames.add(clusterName);

        final Collection<String> involvedServers = dCfg.getServers(clusterName);
        involvedServers.remove(dManager.getLocalNodeName());

        final OStorageOperationResult<ORawBuffer> localResult = db.getStorage().readRecord(data.rid, null, true, null);

        ODistributedResponse response = dManager.sendRequest(data.databaseName, clusterNames, involvedServers,
            new ORepairReadRecordTask(data.rid), dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE,
            localResult.getResult(), null);

        if (response != null) {
          final Object payload = response.getPayload();
          if (payload instanceof Map) {
            // GROUP RESPONSES BY VALUE
            final Map<Object, List<String>> groupedResult = new HashMap<Object, List<String>>();
            final Map<String, Object> map = (Map<String, Object>) payload;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
              final Object v = entry.getValue();
              List<String> group = groupedResult.get(v);
              if (group == null) {
                group = new ArrayList<String>();
                groupedResult.put(v, group);
              }
              group.add(entry.getKey());
            }

            if (groupedResult.size() == 1) {
              // NO CONFLICT, SKIP IT
              coherent.incrementAndGet();
              return;
            }

            conflicts.incrementAndGet();

            final ODistributedConflictResolver conflictResolver = dManager.getConflictResolver();
            final Object winnerValue = conflictResolver.onConflict(data.databaseName, clusterName, data.rid, dManager,
                groupedResult);
            if (winnerValue == null)
              // SKIP IT
              return;

            // REMOVE THE WINNING RESULT
            groupedResult.remove(winnerValue);

            for (Map.Entry<Object, List<String>> entry : groupedResult.entrySet()) {
              final Object value = entry.getKey();
              final List<String> servers = entry.getValue();

              if (winnerValue instanceof ORawBuffer && value instanceof ORawBuffer) {
                // UPDATE THE RECORD
                final ORawBuffer winnerRecord = (ORawBuffer) winnerValue;
                response = dManager.sendRequest(data.databaseName, clusterNames, servers,
                    new OUpdateRecordTask(data.rid, winnerRecord.buffer, ORecordVersionHelper.setRollbackMode(winnerRecord.version),
                        winnerRecord.recordType),
                    dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

              } else if (winnerValue instanceof ORecordNotFoundException && value instanceof ORawBuffer) {
                // DELETE THE RECORD
                response = dManager.sendRequest(data.databaseName, clusterNames, servers, new ODeleteRecordTask(data.rid, -1),
                    dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

              } else if (value instanceof Throwable) {
                // MANAGE EXCEPTION
              }
            }

          }
        }

      } finally {
        db.close();
        totalTimeProcessing.addAndGet(System.currentTimeMillis() - start);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      interruptCurrentOperation();
    }
  }

  public long getCoherent() {
    return coherent.get();
  }

  public long getConflicts() {
    return conflicts.get();
  }

  public long getProcessed() {
    return processed.get();
  }

  public long getTotalTimeProcessing() {
    return totalTimeProcessing.get();
  }
}
