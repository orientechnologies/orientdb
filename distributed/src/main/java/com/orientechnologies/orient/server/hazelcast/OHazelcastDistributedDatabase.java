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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.task.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * Hazelcast implementation of distributed peer. There is one instance per database. Each node creates own instance to talk with
 * each others.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastDistributedDatabase implements ODistributedDatabase {

  private final static int                            LOCAL_QUEUE_MAXSIZE            = 2000;
  private static final String                         NODE_LOCK_PREFIX               = "orientdb.reqlock.";
  private static final String                         DISTRIBUTED_SYNC_JSON_FILENAME = "/distributed-sync.json";
  protected final OHazelcastPlugin                    manager;
  protected final OHazelcastDistributedMessageService msgService;
  protected final String                              databaseName;
  protected final Lock                                requestLock;
  protected ODistributedSyncConfiguration             syncConfiguration;
  protected AtomicBoolean                             status                         = new AtomicBoolean(false);
  protected ConcurrentHashMap<ORID, String>           lockManager                    = new ConcurrentHashMap<ORID, String>();
  protected final List<ODistributedWorker>            workerThreads                  = new ArrayList<ODistributedWorker>();

  public OHazelcastDistributedDatabase(final OHazelcastPlugin manager, final OHazelcastDistributedMessageService msgService,
      final String iDatabaseName) {
    this.manager = manager;
    this.msgService = msgService;
    this.databaseName = iDatabaseName;

    this.requestLock = manager.getHazelcastInstance().getLock(NODE_LOCK_PREFIX + iDatabaseName);

    // START ALL THE WORKER THREADS (CONFIGURABLE)
    for (int i = 0; i < OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.getValueAsInteger(); ++i) {
      final ODistributedWorker workerThread = new ODistributedWorker(this, databaseName, i);
      workerThreads.add(workerThread);
      workerThread.start();
    }

    checkLocalNodeInConfiguration();
  }

  /**
   * Distributed requests against the available workers. This guarantee the sequence of the operations against the same record
   * cluster.
   */
  public void processRequest(final ODistributedRequest request) {
    final int partitionKey = Math.abs(request.getTask().getPartitionKey());

    final int partition = partitionKey % workerThreads.size();

    final ODistributedWorker worker = workerThreads.get(partition);
    worker.processRequest(request);
  }

  @Override
  public ODistributedResponse send2Nodes(final ODistributedRequest iRequest, final Collection<String> iClusterNames,
      final List<String> iNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode, final int quorumOffset) {
    checkForServerOnline(iRequest);

    final String databaseName = iRequest.getDatabaseName();

    if (iNodes.isEmpty()) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "No nodes configured for database '%s' request: %s", databaseName, iRequest);
      throw new ODistributedException("No nodes configured for partition '" + databaseName + "' request: " + iRequest);
    }

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    iRequest.setSenderNodeId(manager.getLocalNodeId());

    final int availableNodes = manager.getAvailableNodes(iNodes, databaseName);

    final int quorum = calculateQuorum(iRequest, iClusterNames, cfg, availableNodes, iExecutionMode, quorumOffset);

    int expectedSynchronousResponses = availableNodes;

    final boolean groupByResponse;
    if (iRequest.getTask().getResultStrategy() == OAbstractRemoteTask.RESULT_STRATEGY.UNION) {
      expectedSynchronousResponses = availableNodes;
      groupByResponse = false;
    } else {
      groupByResponse = true;
    }

    final boolean waitLocalNode = waitForLocalNode(cfg, iClusterNames, iNodes);

    // CREATE THE RESPONSE MANAGER
    final ODistributedResponseManager currentResponseMgr = new ODistributedResponseManager(manager, iRequest, iNodes,
        expectedSynchronousResponses, quorum, waitLocalNode, iRequest.getTask().getSynchronousTimeout(expectedSynchronousResponses),
        iRequest.getTask().getTotalTimeout(availableNodes), groupByResponse);

    Collections.sort(iNodes);

    try {
      iRequest.setId(manager.getLocalMessageIdCounter().getAndIncrement());

      msgService.registerRequest(iRequest.getId(), currentResponseMgr);

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "Sending request %s", iRequest);

      for (String node : iNodes) {
        // CATCH ANY EXCEPTION LOG IT AND IGNORE TO CONTINUE SENDING REQUESTS TO OTHER NODES
        try {
          final ORemoteServerController remoteServer = manager.getRemoteServer(node);
          remoteServer.sendRequest(iRequest, node);
        } catch (Throwable e) {
          ODistributedServerLog.error(this, manager.getLocalNodeName(), node, ODistributedServerLog.DIRECTION.OUT,
              "Error on sending distributed request %s. Active nodes: %s", e, iRequest,
              manager.getAvailableNodeNames(databaseName));
        }
      }

      if (ODistributedServerLog.isDebugEnabled())
        ODistributedServerLog.debug(this, getLocalNodeName(), iNodes.toString(), DIRECTION.OUT, "Sent request %s", iRequest);

      Orient.instance().getProfiler().updateCounter("distributed.db." + databaseName + ".msgSent",
          "Number of replication messages sent from current node", +1, "distributed.db.*.msgSent");

      return waitForResponse(iRequest, currentResponseMgr);

    } catch (Exception e) {
      throw OException.wrapException(new ODistributedException("Error on executing distributed request (" + iRequest
          + ") against database '" + databaseName + (iClusterNames != null ? "." + iClusterNames : "") + "' to nodes " + iNodes),
          e);
    }
  }

  public OHazelcastDistributedDatabase configureDatabase(final Callable<Void> iCallback) {
    if (iCallback != null)
      try {
        iCallback.call();
      } catch (Exception e) {
        throw OException.wrapException(new ODistributedException("Database cannot be configured"), e);
      }

    setOnline();

    return this;
  }

  @Override
  public void setOnline() {
    if (status.compareAndSet(false, true)) {
      ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "Publishing ONLINE status for database %s.%s...",
          getLocalNodeName(), databaseName);

      // SET THE NODE.DB AS ONLINE
      manager.setDatabaseStatus(getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);
    }
  }

  @Override
  public boolean lockRecord(final ORID iRecord, final String iNodeName) {
    final boolean locked = lockManager.putIfAbsent(iRecord, iNodeName) == null;

    // if (!locked) {
    // final String lockingNode = lockManager.get(iRecord);
    // if (iNodeName.equals(lockingNode))
    // // SAME NODE, ALREADY LOCKED
    // return true;
    // }
    //
    if (ODistributedServerLog.isDebugEnabled())
      if (locked)
        ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Distributed transaction: locked record %s in database '%s' owned by server '%s'", iRecord, databaseName, iNodeName);
      else
        ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
            "Distributed transaction: cannot lock record %s in database '%s' owned by server '%s'", iRecord, databaseName,
            iNodeName);

    return locked;
  }

  @Override
  public void unlockRecord(final ORID iRecord) {
    lockManager.remove(iRecord);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Distributed transaction: unlocked record %s in database '%s'", iRecord, databaseName);
  }

  public ODistributedSyncConfiguration getSyncConfiguration() {
    if (syncConfiguration == null) {
      final String path = manager.getServerInstance().getDatabaseDirectory() + databaseName + DISTRIBUTED_SYNC_JSON_FILENAME;
      final File cfgFile = new File(path);
      try {
        syncConfiguration = new ODistributedSyncConfiguration(cfgFile);
      } catch (IOException e) {
        throw new ODistributedException("Cannot open database sync configuration file: " + cfgFile);
      }
    }

    return syncConfiguration;
  }

  @Override
  public void unlockRecords(final String iNodeName) {
    int unlocked = 0;
    final Iterator<Map.Entry<ORID, String>> it = lockManager.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<ORID, String> v = it.next();
      if (v != null && iNodeName.equals(v.getValue())) {
        // FOUND: UNLOCK IT
        it.remove();
        unlocked++;
      }
    }

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, getLocalNodeName(), null, DIRECTION.NONE,
          "Distributed transaction: unlocked %d locks in database '%s' owned by server '%s'", unlocked, databaseName, iNodeName);
  }

  public void shutdown() {
    for (ODistributedWorker workerThread : workerThreads) {
      if (workerThread != null)
        workerThread.shutdown();
    }
  }

  protected void checkForServerOnline(ODistributedRequest iRequest) throws ODistributedException {
    final ODistributedServerManager.NODE_STATUS srvStatus = manager.getNodeStatus();
    if (srvStatus == ODistributedServerManager.NODE_STATUS.OFFLINE
        || srvStatus == ODistributedServerManager.NODE_STATUS.SHUTTINGDOWN) {
      ODistributedServerLog.error(this, getLocalNodeName(), null, DIRECTION.OUT,
          "Local server is not online (status='%s'). Request %s will be ignored", srvStatus, iRequest);
      throw new ODistributedException(
          "Local server is not online (status='" + srvStatus + "'). Request " + iRequest + " will be ignored");
    }
  }

  protected boolean waitForLocalNode(final ODistributedConfiguration cfg, final Collection<String> iClusterNames,
      final Collection<String> iNodes) {
    boolean waitLocalNode = false;
    if (iNodes.contains(getLocalNodeName()))
      if (iClusterNames == null || iClusterNames.isEmpty()) {
        // DEFAULT CLUSTER (*)
        if (cfg.isReadYourWrites(null))
          waitLocalNode = true;
      } else
        // BROWSE FOR ALL CLUSTER TO GET THE FIRST 'waitLocalNode'
        for (String clName : iClusterNames) {
          if (cfg.isReadYourWrites(clName)) {
            waitLocalNode = true;
            break;
          }
        }
    return waitLocalNode;
  }

  protected int calculateQuorum(final ODistributedRequest iRequest, final Collection<String> clusterNames,
      final ODistributedConfiguration cfg, final int availableNodes, final ODistributedRequest.EXECUTION_MODE iExecutionMode,
      final int quorumOffset) {

    if (availableNodes == 0 && iExecutionMode == ODistributedRequest.EXECUTION_MODE.RESPONSE)
      throw new ODistributedException("Quorum cannot be reached because there are no nodes available");

    final String clusterName = clusterNames == null || clusterNames.isEmpty() ? null : clusterNames.iterator().next();

    int quorum = 1;

    final OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType = iRequest.getTask().getQuorumType();

    switch (quorumType) {
    case NONE:
      // IGNORE IT
      break;
    case READ:
      quorum = cfg.getReadQuorum(clusterName, availableNodes);
      break;
    case WRITE:
      quorum = cfg.getWriteQuorum(clusterName, availableNodes);
      break;
    case ALL:
      quorum = availableNodes;
      break;
    }

    final int originalQuorum = quorum;

    // CHECK THE QUORUM OFFSET IF ANY
    quorum -= quorumOffset;
    if (quorum < 0)
      quorum = 0;

    if (quorum > availableNodes)
      throw new ODistributedException(
          "Quorum (" + originalQuorum + ") cannot be reached because it is major than available nodes (" + availableNodes + ")");

    return quorum;
  }

  protected ODistributedResponse waitForResponse(final ODistributedRequest iRequest,
      final ODistributedResponseManager currentResponseMgr) throws InterruptedException {
    if (iRequest.getExecutionMode() == ODistributedRequest.EXECUTION_MODE.NO_RESPONSE)
      return null;

    final long beginTime = System.currentTimeMillis();

    // WAIT FOR THE MINIMUM SYNCHRONOUS RESPONSES (QUORUM)
    if (!currentResponseMgr.waitForSynchronousResponses()) {
      final long elapsed = System.currentTimeMillis() - beginTime;

      if (elapsed > currentResponseMgr.getSynchTimeout()) {

        ODistributedServerLog.warn(this, getLocalNodeName(), null, DIRECTION.IN,
            "timeout (%dms) on waiting for synchronous responses from nodes=%s responsesSoFar=%s request=%s", elapsed,
            currentResponseMgr.getExpectedNodes(), currentResponseMgr.getRespondingNodes(), iRequest);
      }
    }

    return currentResponseMgr.getFinalResponse();
  }

  protected String getLocalNodeName() {
    return manager.getLocalNodeName();
  }

  /**
   * Checks if last pending operation must be re-executed or not. In some circustamces the exception
   * OHotAlignmentNotPossibleExeption is raised because it's not possible to recover the database state.
   *
   * @throws OHotAlignmentNotPossibleException
   */
  protected void hotAlignmentError(final ODistributedRequest iLastPendingRequest, final String iMessage, final Object... iParams)
      throws OHotAlignmentNotPossibleException {
    final String msg = String.format(iMessage, iParams);

    ODistributedServerLog.warn(this, getLocalNodeName(), manager.getNodeNameById(iLastPendingRequest.getSenderNodeId()),
        DIRECTION.IN, "- " + msg);
    throw new OHotAlignmentNotPossibleException(msg);
  }

  protected void checkLocalNodeInConfiguration() {
    final Lock lock = manager.getLock(databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      final List<String> foundPartition = cfg.addNewNodeInServerList(getLocalNodeName());
      if (foundPartition != null) {
        // SET THE NODE.DB AS OFFLINE, READY TO BE SYNCHRONIZED
        manager.setDatabaseStatus(getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

        ODistributedServerLog.info(this, getLocalNodeName(), null, DIRECTION.NONE, "adding node '%s' in partition: db=%s %s",
            getLocalNodeName(), databaseName, foundPartition);

        manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
      }

    } finally {
      lock.unlock();
    }
  }

  protected void removeNodeInConfiguration(final String iNode, final boolean iForce) {
    final Lock lock = manager.getLock(databaseName + ".cfg");
    lock.lock();
    try {
      // GET LAST VERSION IN LOCK
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

      if (!cfg.isHotAlignment()) {
        final List<String> foundPartition = cfg.removeNodeInServerList(iNode, iForce);
        if (foundPartition != null) {
          ODistributedServerLog.info(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
              "removing node '%s' in partitions: db=%s %s", iNode, databaseName, foundPartition);
        }

        // CHANGED: RE-DEPLOY IT
        manager.updateCachedDatabaseConfiguration(databaseName, cfg.serialize(), true, true);
      }

    } catch (Exception e) {
      ODistributedServerLog.debug(this, getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "unable to remove node or change mastership for '%s' in distributed configuration, db=%s", e, iNode, databaseName);

    } finally {
      lock.unlock();
    }
  }

  protected boolean checkIfOperationHasBeenExecuted(final ODistributedRequest lastPendingRequest, final ORemoteTask task) {
    boolean executeLastPendingRequest = false;

    // ASK FOR RECORD
    if (task instanceof ODeleteRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN DELETED YET
      executeLastPendingRequest = ((ODeleteRecordTask) task).getRid().getRecord() != null;
    } else if (task instanceof OUpdateRecordTask) {
      final ORecord rec = ((OUpdateRecordTask) task).getRid().getRecord();
      if (rec == null)
        ODistributedServerLog.warn(this, getLocalNodeName(), manager.getNodeNameById(lastPendingRequest.getSenderNodeId()),
            DIRECTION.IN, "- cannot update deleted record %s, database could be not aligned", ((OUpdateRecordTask) task).getRid());
      else
        // EXECUTE ONLY IF VERSIONS DIFFER
        executeLastPendingRequest = rec.getVersion() != ((OUpdateRecordTask) task).getVersion();
    } else if (task instanceof OCreateRecordTask) {
      // EXECUTE ONLY IF THE RECORD HASN'T BEEN CREATED YET
      executeLastPendingRequest = ((OCreateRecordTask) task).getRid().getRecord() == null;
    } else if (task instanceof OSQLCommandTask) {
      if (!task.isIdempotent()) {
        hotAlignmentError(lastPendingRequest, "Not able to assure last command has been completed before last crash. Command='%s'",
            ((OSQLCommandTask) task).getPayload());
      }
    } else if (task instanceof OResurrectRecordTask) {
      if (((OResurrectRecordTask) task).getRid().getRecord() == null)
        // ALREADY DELETED: CANNOT RESTORE IT
        hotAlignmentError(lastPendingRequest, "Not able to resurrect deleted record '%s'", ((OResurrectRecordTask) task).getRid());
    } else if (task instanceof OTxTask) {
      // CHECK EACH TX ITEM IF HAS BEEN COMMITTED
      for (ORemoteTask t : ((OTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else if (task instanceof OFixTxTask) {
      // CHECK EACH FIX-TX ITEM IF HAS BEEN COMMITTED
      for (ORemoteTask t : ((OFixTxTask) task).getTasks()) {
        executeLastPendingRequest = checkIfOperationHasBeenExecuted(lastPendingRequest, t);
        if (executeLastPendingRequest)
          // REPEAT THE ENTIRE TX
          return true;
      }
    } else
      hotAlignmentError(lastPendingRequest, "Not able to assure last operation has been completed before last crash. Task='%s'",
          task);
    return executeLastPendingRequest;
  }
}
