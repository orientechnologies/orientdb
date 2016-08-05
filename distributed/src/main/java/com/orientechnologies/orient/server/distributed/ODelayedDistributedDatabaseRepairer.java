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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.server.distributed.conflict.ODistributedConflictResolver;
import com.orientechnologies.orient.server.distributed.impl.ODistributedTransactionManager;
import com.orientechnologies.orient.server.distributed.impl.task.*;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed database repairer delayed respect to the reporting. This assure better performance by grouping multiple requests.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODelayedDistributedDatabaseRepairer implements ODistributedDatabaseRepairer {
  private final ODistributedServerManager       dManager;
  private final String                          databaseName;

  private final AtomicLong                      processed           = new AtomicLong(0);
  private final AtomicLong                      totalTimeProcessing = new AtomicLong(0);

  private ConcurrentHashMap<ORecordId, Boolean> records             = new ConcurrentHashMap<ORecordId, Boolean>();
  private final TimerTask                       checkTask;

  public ODelayedDistributedDatabaseRepairer(final ODistributedServerManager manager, final String databaseName) {
    this.dManager = manager;
    this.databaseName = databaseName;

    checkTask = new TimerTask() {
      @Override
      public void run() {

        final long start = System.currentTimeMillis();
        try {

          check();

        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error on repairing distributed database", t);
          // IGNORE THE EXCEPTION
        } finally {
          totalTimeProcessing.addAndGet(System.currentTimeMillis() - start);
        }
      }

    };
    Orient.instance().scheduleTask(checkTask, OGlobalConfiguration.DISTRIBUTED_DELAYED_AUTO_REPAIRER_CHECK_EVERY.getValueAsLong(),
        OGlobalConfiguration.DISTRIBUTED_DELAYED_AUTO_REPAIRER_CHECK_EVERY.getValueAsLong());

  }

  /**
   * Add the record to repair int the map of records and cluster. The decision about repairing is taken by the timer task.
   *
   * @param rid
   *          RecordId to repair
   */
  @Override
  public void repairRecord(final ORecordId rid) {
    if (rid.clusterPosition < -1)
      // SKIP TRANSACTIONAL RIDS
      return;

    processed.incrementAndGet();

    // ADD RECORD TO REPAIR
    records.put(rid, Boolean.TRUE);
  }

  private void check() throws Exception {
    // OPEN THE DATABASE ONLY IF NEEDED
    ODatabaseDocumentTx db = null;
    try {
      final int batchMax = OGlobalConfiguration.DISTRIBUTED_DELAYED_AUTO_REPAIRER_BATCH.getValueAsInteger();
      final List<ORecordId> rids = new ArrayList<ORecordId>(batchMax);

      for (ORecordId rid : records.keySet()) {
        rids.add(rid);
        if (rids.size() >= batchMax)
          // REACHED MAXIMUM FOR BATCH
          break;
      }

      if (!rids.isEmpty()) {
        // REPAIR RECORDS IN BATCH
        db = getDatabase();
        if (repairRecords(db, rids)) {
          // SUCCEED: REMOVE REPAIRED RECORDS
          for (ORecordId rid : rids)
            records.remove(rid);
        }
      }

    } finally {
      if (db != null)
        db.close();
    }
  }

  private boolean repairRecords(final ODatabaseDocumentInternal db, final List<ORecordId> rids) throws Exception {
    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);

    final int maxAutoRetry = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
    final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();

    final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    final ODistributedDatabase localDistributedDatabase = dManager.getMessageService().getDatabase(databaseName);

    final ODistributedTxContext ctx = localDistributedDatabase.registerTxContext(requestId);

    try {
      // ACQUIRE LOCKS WITH A LARGER TIMEOUT
      ODistributedTransactionManager.acquireMultipleRecordLocks(this, dManager, localDistributedDatabase, rids, maxAutoRetry,
          autoRetryDelay, null, ctx, 2000);
      try {

        final Set<String> clusterNames = new HashSet();
        for (ORecordId rid : rids)
          clusterNames.add(db.getClusterNameById(rid.clusterId));

        final Collection<String> involvedServers = dCfg.getServers(clusterNames);
        final Set<String> nonLocalServers = new HashSet<String>(involvedServers);
        nonLocalServers.remove(dManager.getLocalNodeName());

        // CREATE LOCAL RESULT
        final OTxTaskResult localResult = new OTxTaskResult();
        for (ORecordId rid : rids) {
          final OStorageOperationResult<ORawBuffer> res;
          if (rid.clusterPosition > -1)
            res = db.getStorage().readRecord(rid, null, true, null);
          else
            res = null;

          if (res != null)
            localResult.results.add(res.getResult());
          else
            localResult.results.add(null);
        }

        ODistributedServerLog.info(this, dManager.getLocalNodeName(), involvedServers.toString(),
            ODistributedServerLog.DIRECTION.OUT, "Auto repairing records %s on servers %s (reqId=%s)...", rids, involvedServers,
            requestId);

        // CREATE TX TASK
        final ORepairRecordsTask tx = new ORepairRecordsTask();
        for (ORecordId rid : rids)
          tx.add(new OReadRecordTask(rid));

        ODistributedResponse response = dManager.sendRequest(databaseName, clusterNames, nonLocalServers, tx,
            requestId.getMessageId(), ODistributedRequest.EXECUTION_MODE.RESPONSE, localResult, null);

        // MAP OF OCompletedTxTask SERVER/RECORDS. RECORD == NULL MEANS DELETE
        final Map<String, OCompletedTxTask> repairMap = new HashMap<String, OCompletedTxTask>(rids.size());
        for (String server : involvedServers) {
          final OCompletedTxTask completedTask = new OCompletedTxTask(requestId, false, tx.getPartitionKey());
          repairMap.put(server, completedTask);
        }

        try {
          if (response != null) {
            final Object payload = response.getPayload();
            if (payload instanceof Map) {

              final List<ODistributedConflictResolver> conflictResolvers = dManager.getConflictResolver();

              final Map<String, Object> map = (Map<String, Object>) payload;

              // BROWSE FROM LOCAL RESULT
              for (int i = 0; i < localResult.results.size(); ++i) {
                final Map<Object, List<String>> groupedResult = new HashMap<Object, List<String>>();

                final ORecordId rid = rids.get(i);

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                  if (entry.getValue() instanceof Throwable) {
                    // ABORT IT
                    ODistributedServerLog.info(this, dManager.getLocalNodeName(), entry.getKey(),
                        ODistributedServerLog.DIRECTION.IN, "Error on auto repairing record %s on servers %s (error=%s)", rid,
                        entry.getKey(), entry.getValue());
                    return false;
                  }

                  final OTxTaskResult v = (OTxTaskResult) entry.getValue();
                  final Object remoteValue = v.results.get(i);

                  List<String> group = groupedResult.get(remoteValue);
                  if (group == null) {
                    group = new ArrayList<String>();
                    groupedResult.put(remoteValue, group);
                  }
                  group.add(entry.getKey());
                }

                if (groupedResult.size() == 1)
                  // NO CONFLICT, SKIP IT
                  continue;

                ODocument config = null;

                // EXECUTE THE CONFLICT RESOLVE PIPELINE: CONTINUE UNTIL THE WINNER IS NOT NULL (=RESOLVED)
                Object winner = null;
                Map<Object, List<String>> candidates = groupedResult;
                for (ODistributedConflictResolver conflictResolver : conflictResolvers) {
                  final ODistributedConflictResolver.OConflictResult conflictResult = conflictResolver.onConflict(databaseName,
                      db.getClusterNameById(rid.clusterId), rid, dManager, candidates, config);

                  winner = conflictResult.winner;
                  if (winner != null)
                    // FOUND WINNER
                    break;

                  candidates = conflictResult.candidates;
                }

                if (winner == null)
                  // NO WINNER, SKIP IT
                  continue;

                for (Map.Entry<Object, List<String>> entry : groupedResult.entrySet()) {
                  final Object value = entry.getKey();
                  final List<String> servers = entry.getValue();

                  for (String server : servers) {
                    ODistributedServerLog.info(this, dManager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                        "Preparing fix for record %s on servers %s, value=%s...", rid, server, winner);

                    if (!winner.equals(value)) {
                      final OCompletedTxTask completedTask = repairMap.get(server);

                      if (winner instanceof ORawBuffer && value instanceof ORawBuffer) {
                        // UPDATE THE RECORD
                        final ORawBuffer winnerRecord = (ORawBuffer) winner;

                        completedTask.addFixTask(new OUpdateRecordTask(rid, winnerRecord.buffer,
                            ORecordVersionHelper.setRollbackMode(winnerRecord.version), winnerRecord.recordType));

                      } else if (winner instanceof ORecordNotFoundException && value instanceof ORawBuffer) {
                        // DELETE THE RECORD

                        completedTask.addFixTask(new ODeleteRecordTask(rid, -1));

                      } else if (value instanceof Throwable) {
                        // MANAGE EXCEPTION
                      }
                    }
                  }
                }
              }
            }
          }
        } finally {
          int repaired = 0;
          for (Map.Entry<String, OCompletedTxTask> entry : repairMap.entrySet()) {
            final String server = entry.getKey();
            final OCompletedTxTask task = entry.getValue();

            repaired += task.getFixTasks().size();

            if (dManager.getLocalNodeName().equals(server))
              // EXECUTE IT LOCALLY
              dManager.executeOnLocalNode(requestId, task, db);
            else {
              // EXECUTE REMOTELY
              final List<String> servers = new ArrayList<String>();
              servers.add(server);

              // FILTER ONLY THE SERVER ONLINE
              dManager.getAvailableNodes(servers, databaseName);

              if (!servers.isEmpty()) {
                response = dManager.sendRequest(databaseName, clusterNames, servers, task, dManager.getNextMessageIdCounter(),
                    ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);
              }
            }
          }

          if (repaired == 0)
            ODistributedServerLog.info(this, dManager.getLocalNodeName(), involvedServers.toString(),
                ODistributedServerLog.DIRECTION.OUT, "Auto repairing completed. No fix is needed (reqId=%s)", repaired, requestId);
          else
            ODistributedServerLog.info(this, dManager.getLocalNodeName(), involvedServers.toString(),
                ODistributedServerLog.DIRECTION.OUT, "Auto repairing completed. Sent %d fix messages for %d records (reqId=%s)",
                repaired, rids.size(), requestId);
        }

      } finally {
        // RELEASE LOCKS AND REMOVE TX CONTEXT
        localDistributedDatabase.popTxContext(requestId);
        ctx.destroy();
      }
    } catch (ODistributedRecordLockedException e) {
      // IGNORE IT
    }

    return true;
  }

  @Override
  public long getProcessed() {
    return processed.get();
  }

  @Override
  public long getTotalTimeProcessing() {
    return totalTimeProcessing.get();
  }

  private ODatabaseDocumentTx getDatabase() {
    return dManager.getMessageService().getDatabase(databaseName).getDatabaseInstance();
  }

  @Override
  public void shutdown() {
    if (checkTask != null) {
      checkTask.cancel();
    }
    records.clear();
  }
}
