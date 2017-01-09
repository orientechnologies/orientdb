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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.conflict.ODistributedConflictResolver;
import com.orientechnologies.orient.server.distributed.impl.task.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed database repairer that, based on the reported records to check, executes repair of record in configurable batches.
 * This assure better performance by grouping multiple requests. The repair is based on the chain of conflict resolver.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OConflictResolverDatabaseRepairer implements ODistributedDatabaseRepairer {
  private final ODistributedServerManager dManager;
  private final String                    databaseName;

  private final AtomicLong recordProcessed     = new AtomicLong(0);
  private final AtomicLong recordCanceled      = new AtomicLong(0);
  private final AtomicLong totalTimeProcessing = new AtomicLong(0);
  private final boolean active;

  private ConcurrentMap<ORecordId, Boolean> records  = new ConcurrentHashMap<ORecordId, Boolean>();
  private ConcurrentMap<Integer, Boolean>   clusters = new ConcurrentHashMap<Integer, Boolean>();

  private final TimerTask checkTask;

  private List<ODistributedConflictResolver> conflictResolvers = new ArrayList<ODistributedConflictResolver>();

  public OConflictResolverDatabaseRepairer(final ODistributedServerManager manager, final String databaseName) {
    this.dManager = manager;
    this.databaseName = databaseName;

    // REGISTER THE CHAIN OF CONFLICT RESOLVERS
    final String chain = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHAIN.getValueAsString();
    final String[] items = chain.split(",");
    for (String item : items) {
      final ODistributedConflictResolver cr = manager.getConflictResolverFactory().getImplementation(item);
      if (cr == null)
        throw new OConfigurationException(
            "Cannot find '" + item + "' conflict resolver implementation. Available are: " + manager.getConflictResolverFactory()
                .getRegisteredImplementationNames());
      conflictResolvers.add(cr);
    }

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

    final long time = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHECK_EVERY.getValueAsLong();
    if (time > 0) {
      Orient.instance().scheduleTask(checkTask, time, time);
      active = true;
    } else
      active = false;
  }

  /**
   * Adds the record to repair int the map of records and cluster. The decision about repairing is taken by the timer task.
   *
   * @param rid RecordId to repair
   */
  @Override
  public void enqueueRepairRecord(final ORecordId rid) {
    if (!active)
      return;

    if (rid == null || !rid.isPersistent())
      return;

    if (rid.getClusterPosition() < -1)
      // SKIP TRANSACTIONAL RIDS
      return;

    recordProcessed.incrementAndGet();

    // ADD RECORD TO REPAIR
    records.put(rid, Boolean.TRUE);
  }

  /**
   * Cancel the repair against a record because the update succeed.
   *
   * @param rid RecordId to remove from repair
   */
  @Override
  public void cancelRepairRecord(final ORecordId rid) {
    if (!active)
      return;

    if (rid.getClusterPosition() < -1)
      // SKIP TRANSACTIONAL RIDS
      return;

    // REMOVE THE RECORD TO REPAIR
    if (records.remove(rid) != null)
      // REMOVED
      recordCanceled.incrementAndGet();
  }

  /**
   * Enqueues the request to repair a cluster. The decision about repairing is taken by the timer task.
   *
   * @param clusterId Broken cluster id to start repairing
   */
  @Override
  public void enqueueRepairCluster(final int clusterId) {
    if (!active)
      return;

    if (clusterId < -1)
      // SKIP TRANSACTIONAL RIDS
      return;

    recordProcessed.incrementAndGet();

    // ADD CLUSTER TO REPAIR
    clusters.put(clusterId, Boolean.TRUE);
  }

  private void check() throws Exception {
    // OPEN THE DATABASE ONLY IF NEEDED
    ODatabaseDocumentTx db = null;
    try {
      final int batchMax = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.getValueAsInteger();
      final List<ORecordId> rids = new ArrayList<ORecordId>(batchMax);

      // REPAIR CLUSTER FIRST
      for (Integer clusterId : clusters.keySet()) {
        repairCluster(db, clusterId);
      }
      clusters.clear();

      // REPAIR RECORDS
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

  private void repairCluster(ODatabaseDocumentInternal db, final Integer clusterId) throws Exception {
    if (clusterId < 0)
      return;

    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);

    final int maxAutoRetry = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
    final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();

    final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    final ODistributedDatabase localDistributedDatabase = dManager.getMessageService().getDatabase(databaseName);

    if (db == null)
      db = getDatabase();

    final String clusterName = db.getClusterNameById(clusterId);

    final ODistributedTxContext ctx = localDistributedDatabase.registerTxContext(requestId);

    // ASSURE LOCAL NODE IS THE CLUSTER OWNER
    final String serverOwner = dCfg.getClusterOwner(clusterName);
    if (serverOwner == null || !serverOwner.equals(dManager.getLocalNodeName())) {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Cannot auto repair cluster '%s' (%d) because current server (%s) is not the owner (owner=%s reqId=%s)", clusterName,
          clusterId, dManager.getLocalNodeName(), serverOwner, requestId);
      return;
    }

    try {
      // ACQUIRE LOCK ON THE CLUSTER (LOCKING -1 AS CLUSTER POSITION)
      final List<ORecordId> rids = new ArrayList<ORecordId>(1);
      rids.add(new ORecordId(clusterId, -1));

      // ACQUIRE LOCKS ON LOCAL SERVER FIRST
      ODistributedTransactionManager
          .acquireMultipleRecordLocks(this, dManager, localDistributedDatabase, rids, maxAutoRetry, autoRetryDelay, null, ctx,
              2000);

      try {

        final List<String> clusterNames = new ArrayList<String>();
        clusterNames.add(clusterName);

        final Collection<String> involvedServers = dCfg.getServers(clusterNames);
        final Set<String> nonLocalServers = new HashSet<String>(involvedServers);
        nonLocalServers.remove(dManager.getLocalNodeName());

        if (nonLocalServers.isEmpty())
          return;

        ODistributedServerLog
            .debug(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                "Auto repairing cluster '%s' (%d) on servers %s (reqId=%s)...", clusterName, clusterId, involvedServers, requestId);

        // CREATE TX TASK
        final OClusterRepairInfoTask task = new OClusterRepairInfoTask(clusterId);

        final ODistributedResponse response = dManager
            .sendRequest(databaseName, clusterNames, nonLocalServers, task, requestId.getMessageId(),
                ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

        int repaired = 0;

        try {
          if (response != null) {
            final Object payload = response.getPayload();
            if (payload instanceof Map)
              repaired = repairClusterAtBlocks(db, clusterNames, clusterId, (Map<String, Object>) payload);
          }
        } finally {
          if (repaired == 0)
            ODistributedServerLog
                .debug(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                    "Auto repairing of cluster '%s' completed. No fix is needed (reqId=%s)", clusterName, repaired, requestId);
          else
            ODistributedServerLog
                .info(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                    "Auto repairing of cluster '%s' completed. Repaired %d records (reqId=%s)", clusterName, repaired, requestId);
        }

      } finally {
        // RELEASE LOCKS AND REMOVE TX CONTEXT
        localDistributedDatabase.popTxContext(requestId);
        ctx.destroy();
      }

    } catch (Throwable e) {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error executing auto repairing on cluster '%s' (error=%s, reqId=%s)", clusterName, e.toString(), requestId);
      return;
    }

    return;
  }

  private int repairClusterAtBlocks(final ODatabaseDocumentInternal db, final List<String> clusterNames, final int clusterId,
      final Map<String, Object> repairInfoResult) throws IOException {
    final OStorage storage = db.getStorage().getUnderlying();
    final long localEnd = storage.getClusterById(clusterId).getNextPosition() - 1;

    final int batchMax = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.getValueAsInteger();

    int recordRepaired = 0;

    for (Map.Entry<String, Object> entry : repairInfoResult.entrySet()) {
      final String server = entry.getKey();

      final ODistributedServerManager.DB_STATUS status = dManager.getDatabaseStatus(server, databaseName);

      if (status != ODistributedServerManager.DB_STATUS.ONLINE) {
        ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "Cannot align missing records of cluster '%s' on server %s, because is not ONLINE (status=%s)", clusterNames.get(0),
            server, status);
        return 0;
      }

      final Object result = entry.getValue();

      if (result instanceof Long) {
        final long remoteEnd = (Long) result;

        ORepairClusterTask task = new ORepairClusterTask(clusterId);

        for (long pos = remoteEnd + 1; pos <= localEnd; ++pos) {
          final ORecordId rid = new ORecordId(clusterId, pos);
          final ORawBuffer rawRecord = storage.readRecord(rid, null, true, false, null).getResult();
          if (rawRecord == null)
            continue;

          task.add(new OCreateRecordTask(rid, rawRecord.buffer, rawRecord.version, rawRecord.recordType));

          recordRepaired++;

          if (task.getTasks().size() > batchMax) {
            // SEND BATCH OF CHANGES
            final List<String> servers = new ArrayList<String>(1);
            servers.add(server);

            final ODistributedResponse response = dManager
                .sendRequest(databaseName, clusterNames, servers, task, dManager.getNextMessageIdCounter(),
                    ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);

            task = new ORepairClusterTask(clusterId);
          }
        }

        if (!task.getTasks().isEmpty()) {
          // SEND FINAL BATCH OF CHANGES
          final List<String> servers = new ArrayList<String>(1);
          servers.add(server);

          final ODistributedResponse response = dManager
              .sendRequest(databaseName, clusterNames, servers, task, dManager.getNextMessageIdCounter(),
                  ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null);
        }

        if (task.getTasks().size() == 0)
          ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Auto repair aligned %d records of cluster '%s'", task.getTasks().size(), clusterNames.get(0));
        else
          ODistributedServerLog.info(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "Auto repair aligned %d records of cluster '%s'", task.getTasks().size(), clusterNames.get(0));
      }
    }

    return recordRepaired;
  }

  @Override
  public void repairRecord(final ORecordId rid) {
    final List<ORecordId> rids = new ArrayList<ORecordId>();
    rids.add(rid);
    repairRecords(getDatabase(), rids);
  }

  private boolean repairRecords(final ODatabaseDocumentInternal db, final List<ORecordId> rids) {
    final ODistributedConfiguration dCfg = dManager.getDatabaseConfiguration(databaseName);

    final int maxAutoRetry = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.getValueAsInteger();
    final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();

    final ODistributedRequestId requestId = new ODistributedRequestId(dManager.getLocalNodeId(),
        dManager.getNextMessageIdCounter());

    final ODistributedDatabase localDistributedDatabase = dManager.getMessageService().getDatabase(databaseName);

    final ODistributedTxContext ctx = localDistributedDatabase.registerTxContext(requestId);

    try {
      // ACQUIRE LOCKS WITH A LARGER TIMEOUT
      ODistributedTransactionManager
          .acquireMultipleRecordLocks(this, dManager, localDistributedDatabase, rids, maxAutoRetry, autoRetryDelay, null, ctx,
              2000);
      try {

        final Set<String> clusterNames = new HashSet();
        for (ORecordId rid : rids)
          clusterNames.add(db.getClusterNameById(rid.getClusterId()));

        final Collection<String> involvedServers = dCfg.getServers(clusterNames);
        final Set<String> nonLocalServers = new HashSet<String>(involvedServers);
        nonLocalServers.remove(dManager.getLocalNodeName());

        if (nonLocalServers.isEmpty())
          return true;

        // CREATE LOCAL RESULT
        final OTxTaskResult localResult = new OTxTaskResult();
        for (ORecordId rid : rids) {
          final OStorageOperationResult<ORawBuffer> res;
          if (rid.getClusterPosition() > -1)
            res = db.getStorage().readRecord(rid, null, true, false, null);
          else
            res = null;

          if (res != null)
            localResult.results.add(res.getResult());
          else
            localResult.results.add(null);
        }

        ODistributedServerLog
            .debug(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                "Auto repairing records %s on servers %s (reqId=%s)...", rids, involvedServers, requestId);

        // CREATE TX TASK
        final ORepairRecordsTask tx = new ORepairRecordsTask();
        for (ORecordId rid : rids)
          tx.add(new OReadRecordTask(rid));

        ODistributedResponse response = dManager
            .sendRequest(databaseName, clusterNames, nonLocalServers, tx, requestId.getMessageId(),
                ODistributedRequest.EXECUTION_MODE.RESPONSE, localResult, null);

        // MAP OF OCompletedTxTask SERVER/RECORDS. RECORD == NULL MEANS DELETE
        final Map<String, OCompleted2pcTask> repairMap = new HashMap<String, OCompleted2pcTask>(rids.size());
        for (String server : involvedServers) {
          final OCompleted2pcTask completedTask = new OCompleted2pcTask(requestId, false, tx.getPartitionKey());
          repairMap.put(server, completedTask);
        }

        try {
          if (response != null) {
            final Object payload = response.getPayload();
            if (payload instanceof Map) {

              final Map<String, Object> map = (Map<String, Object>) payload;

              // BROWSE FROM LOCAL RESULT
              for (int i = 0; i < localResult.results.size(); ++i) {
                final Map<Object, List<String>> groupedResult = new HashMap<Object, List<String>>();

                final ORecordId rid = rids.get(i);

                for (Map.Entry<String, Object> entry : map.entrySet()) {
                  if (entry.getValue() instanceof Throwable) {
                    // ABORT IT
                    ODistributedServerLog
                        .info(this, dManager.getLocalNodeName(), entry.getKey(), ODistributedServerLog.DIRECTION.IN,
                            "Error on auto repairing record %s on servers %s (error=%s)", rid, entry.getKey(), entry.getValue());
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
                  final ODistributedConflictResolver.OConflictResult conflictResult = conflictResolver
                      .onConflict(databaseName, db.getClusterNameById(rid.getClusterId()), rid, dManager, candidates, config);

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
                    ODistributedServerLog.debug(this, dManager.getLocalNodeName(), server, ODistributedServerLog.DIRECTION.OUT,
                        "Preparing fix for record %s on servers %s, value=%s...", rid, server, winner);

                    if (!winner.equals(value)) {
                      final OCompleted2pcTask completedTask = repairMap.get(server);

                      if (winner instanceof ORawBuffer && value instanceof ORawBuffer) {
                        // UPDATE THE RECORD
                        final ORawBuffer winnerRecord = (ORawBuffer) winner;

                        completedTask.addFixTask(new OFixUpdateRecordTask(rid, winnerRecord.buffer,
                            ORecordVersionHelper.setRollbackMode(winnerRecord.version), winnerRecord.recordType));

                      } else if (winner instanceof ORecordNotFoundException && value instanceof ORawBuffer) {
                        // DELETE THE RECORD

                        completedTask.addFixTask(new OFixCreateRecordTask(rid, -1));

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
          for (Map.Entry<String, OCompleted2pcTask> entry : repairMap.entrySet()) {
            final String server = entry.getKey();
            final OCompleted2pcTask task = entry.getValue();

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
            ODistributedServerLog
                .debug(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                    "Auto repairing completed. No fix is needed (reqId=%s)", repaired, requestId);
          else
            ODistributedServerLog
                .info(this, dManager.getLocalNodeName(), involvedServers.toString(), ODistributedServerLog.DIRECTION.OUT,
                    "Auto repairing completed. Sent %d fix messages for %d records (reqId=%s)", repaired, rids.size(), requestId);
        }

      } finally {
        // RELEASE LOCKS AND REMOVE TX CONTEXT
        localDistributedDatabase.popTxContext(requestId);
        ctx.destroy();
      }

    } catch (Throwable e) {
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Error executing auto repairing (error=%s, reqId=%s)", e.toString(), requestId);
      return false;
    }

    return true;
  }

  public long getRecordProcessed() {
    return recordProcessed.get();
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
