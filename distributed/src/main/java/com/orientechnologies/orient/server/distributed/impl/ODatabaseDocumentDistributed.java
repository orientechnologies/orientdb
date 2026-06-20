package com.orientechnologies.orient.server.distributed.impl;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION;
import static com.orientechnologies.orient.server.distributed.impl.TxContextStatus.FAILED;
import static com.orientechnologies.orient.server.distributed.impl.TxContextStatus.SUCCESS;
import static com.orientechnologies.orient.server.distributed.impl.TxContextStatus.TIMEDOUT;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OLowDiskSpaceException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionAlreadyPresentException;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAllocation;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.retryable.ORetryInfo;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.exception.ODistributedTxPromiseRequestIsOldException;
import com.orientechnologies.orient.server.distributed.impl.lock.OTxPromise;
import com.orientechnologies.orient.server.distributed.impl.metadata.OClassDistributed;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.ORunQueryExecutionPlanTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSQLCommandTaskFirstPhase;
import com.orientechnologies.orient.server.distributed.impl.task.OSQLCommandTaskSecondPhase;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxInvalidSequential;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/** Created by tglman on 30/03/17. */
public class ODatabaseDocumentDistributed extends ODatabaseDocumentEmbedded {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ODatabaseDocumentDistributed.class);

  private final ODistributedPlugin distributedManager;
  private boolean neverWaited = true;

  public ODatabaseDocumentDistributed(
      OSharedContextEmbedded sharedContext, ODistributedPlugin distributedPlugin) {
    super(sharedContext);
    this.distributedManager = distributedPlugin;
  }

  /**
   * return the name of local node in the cluster
   *
   * @return the name of local node in the cluster
   */
  public String getLocalNodeName() {
    return getContext().getNodeName();
  }

  public ODatabaseId getDatabaseId() {
    return this.getStorage().getDatabaseId();
  }

  @Override
  public boolean isSharded() {
    /*
    Map<String, Set<String>> clusterMap = getActiveClusterMap();
    Iterator<Set<String>> iter = clusterMap.values().iterator();
    Set<String> firstClusterSet = null;
    if (iter.hasNext()) {
      firstClusterSet = iter.next();
    }
    while (iter.hasNext()) {
      if (!firstClusterSet.equals(iter.next())) {
        return true;
      }
    }
    */
    return false;
  }

  @Override
  public boolean isDistributed() {
    return true;
  }

  @Override
  public ODatabaseDocumentDistributed copy() {
    ODatabaseDocumentDistributed database =
        new ODatabaseDocumentDistributed(getSharedContext(), distributedManager);
    database.init(getConfig());
    String user;
    if (getUser() != null) {
      user = getUser().getName();
    } else {
      user = null;
    }
    database.internalOpen(user, null, false);
    database.callOnOpenListeners();
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean sync(boolean forceDeployment, boolean tryWithDelta) {
    checkSecurity(ORule.ResourceGeneric.DATABASE, "sync", ORole.PERMISSION_UPDATE);

    final String databaseName = getName();

    try {
      return getContext().installDatabase(databaseName, forceDeployment, tryWithDelta).get();
    } catch (InterruptedException | ExecutionException e) {
      return false;
    }
  }

  @Override
  public Map<String, Object> getHaStatus(
      boolean servers, boolean db, boolean latency, boolean messages) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    if (getContext().isDistributedDisabled(getName()))
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    Map<String, Object> row = new HashMap<>();
    if (servers) row.put("servers", distributedManager.getClusterConfiguration());
    if (db) row.put("database", getDistributedInfo().toElement());
    if (latency)
      row.put(
          "latency",
          ODistributedOutput.formatLatency(
              distributedManager, distributedManager.getClusterConfiguration()));
    if (messages)
      row.put(
          "messages",
          ODistributedOutput.formatMessages(
              distributedManager, distributedManager.getClusterConfiguration()));

    return row;
  }

  @Override
  public boolean removeHaServer(String serverName) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE);

    if (getContext().isDistributedDisabled(getName()))
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    return getContext().removeDatabaseMember(getDatabaseId(), new ONodeId(serverName));
  }

  @Override
  public OExecutionStream queryOnNode(
      String nodeName, OInternalExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    ORunQueryExecutionPlanTask task =
        new ORunQueryExecutionPlanTask(executionPlan, inputParameters, nodeName);
    ODistributedResponse result = executeTaskOnNode(task, nodeName);
    return task.getResult(result, this);
  }

  public ODistributedResponse executeTaskOnNode(ORemoteTask task, String nodeName) {

    if (getContext().isDistributedDisabled(getName()))
      throw new ODistributedException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return distributedManager.sendSingleRequest(databaseName, nodeName, task);
  }

  @Override
  public void init(OrientDBConfig config) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.init(config);
          return null;
        });
  }

  protected void createMetadata(OSharedContext ctx) {
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    metadata.init(ctx);
    ((OSharedContextDistributed) ctx).create(this);
  }

  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= ORID.CLUSTER_POS_INVALID && iClusterName != null) {
      rid.setClusterId(getClusterIdByName(iClusterName));
      if (rid.getClusterId() == -1)
        throw new IllegalArgumentException("Cluster name '" + iClusterName + "' is not configured");
    }
    OClass schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= ORID.CLUSTER_ID_INVALID && getStorage().isAssigningClusterIds()) {
      if (record instanceof ODocument) {
        // Immutable Schema Class not support distributed yet.
        schemaClass = ((ODocument) record).getSchemaClass();
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException(
                "Document belongs to abstract class "
                    + schemaClass.getName()
                    + " and cannot be saved");
          rid.setClusterId(
              ((OClassDistributed) schemaClass).getClusterForNewInstance(this, (ODocument) record));
        } else
          throw new ODatabaseException(
              "Cannot save (4) document " + record + ": no class or cluster defined");
      } else {
        throw new ODatabaseException(
            "Cannot save (5) document " + record + ": no class or cluster defined");
      }
    } else if (record instanceof ODocument) {
      schemaClass = ((ODocument) record).getSchemaClass();
    }
    // If the cluster id was set check is validity
    if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '"
                  + messageClusterName
                  + "' (id="
                  + rid.getClusterId()
                  + ") is not configured to store the class '"
                  + schemaClass.getName()
                  + "', valid are "
                  + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  @Override
  public void internalCommit(OTransactionInternal iTx) {
    int protocolVersion = DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValueAsInteger();
    if (OScenarioThreadLocal.instance().isRunModeDistributed()
        || (iTx.isSequenceTransaction() && protocolVersion == 2)) {
      // Exclusive for handling schema manipulation, remove after refactor for distributed schema
      super.internalCommit(iTx);
    } else {
      switch (protocolVersion) {
        case 1:
          distributedCommitV1(iTx);
          break;
        default:
          throw new IllegalStateException(
              "Invalid distributed replicaiton protocol version: "
                  + DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValueAsInteger());
      }
    }
  }

  @Override
  public void internalCommitPreallocate(OTransactionOptimistic iTx) {
    int protocolVersion = DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValueAsInteger();
    if (OScenarioThreadLocal.instance().isRunModeDistributed()
        || (iTx.isSequenceTransaction() && protocolVersion == 2)) {
      // Exclusive for handling schema manipulation, remove after refactor for distributed schema
      super.internalCommitPreallocate(iTx);
    } else {
      switch (protocolVersion) {
        case 1:
          distributedCommitV1(iTx);
          break;
        default:
          throw new IllegalStateException(
              "Invalid distributed replicaiton protocol version: "
                  + DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValueAsInteger());
      }
    }
  }

  @Override
  public <T> T sendSequenceAction(OSequenceAction action)
      throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  public void distributedCommitV1(OTransactionInternal iTx) {
    // This is future may handle a retry
    try {
      for (ORecordOperation txEntry : iTx.getRecordOperations()) {
        if (txEntry.getType() == ORecordOperation.CREATED
            || txEntry.getType() == ORecordOperation.UPDATED) {
          final ORecord record = txEntry.getRecord();
          if (record instanceof ODocument) ((ODocument) record).validate();
        }
      }
      ODistributedDatabase localDistributedDatabase = getDistributedShared();
      ODistributedServerManager dManager = getDistributedManager();
      getContext().checkNodeIsMaster(getLocalNodeId(), getName(), "Transaction Commit");

      int nretry =
          this.getConfiguration()
              .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
      int delay =
          this.getConfiguration()
              .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
      ODistributedTxCoordinator txManager =
          new ODistributedTxCoordinator(
              getName(), dManager, localDistributedDatabase, getLocalNodeName(), nretry, delay);
      int quorum = getContext().getNodeState().getOps().getDatabaseQuorum(getDatabaseId());

      final int availableNodes = getContext().getOnlineMasters(getName());

      if (quorum > availableNodes) {
        Set<String> online = getContext().getAvailableNodeNames(getName());
        throw new ODistributedException(
            String.format(
                "Not enough nodes online to execute the operation. Available nodes:%s, quorum:%s",
                online, quorum));
      }

      txManager.commit(this, iTx);
      return;
    } catch (OValidationException e) {
      throw e;
    } catch (HazelcastInstanceNotActiveException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");

    } catch (HazelcastException e) {
      throw new OOfflineNodeException("Hazelcast instance is not available");
    } catch (Exception e) {

      handleDistributedException("Cannot route TX operation against distributed node", e);
    }
  }

  private int getVersionForIndexKey(
      OTransactionInternal tx, String index, Object key, boolean isCoordinator) {
    if (isCoordinator) {
      return tx.getDatabase().getStorage().getVersionForKey(index, key);
    }
    return ((OTransactionOptimisticDistributed) tx).getVersionForKey(index, key);
  }

  /**
   * @param tx
   * @param txContext
   * @param isCoordinator specifies whether this node is the tx coordinator
   * @param force whether to use the force flag for acquiring the promises required for this tx.
   */
  public void acquireLocksForTx(
      OTransactionInternal tx,
      ODistributedTxContext txContext,
      boolean isCoordinator,
      boolean force) {
    Set<OTransactionId> txsWithBrokenPromises = new HashSet<>();
    Set<OPair<ORID, Integer>> rids = new TreeSet<>();
    for (ORecordOperation entry : tx.getRecordOperations()) {
      if (ORecordInternal.isContentChanged(entry.getRecord())) {
        rids.add(new OPair<>(entry.getRID().copy(), entry.getRecord().getVersion()));
      }
    }
    for (OPair<ORID, Integer> rid : rids) {
      OTransactionId txId = txContext.acquirePromise(rid.getKey(), rid.getValue(), force);
      if (txId != null) {
        txsWithBrokenPromises.add(txId);
      }
    }

    // using OPair because there could be different types of values here, so falling back to
    // lexicographic sorting
    Set<OPair<String, Integer>> keys = new TreeSet<>();
    for (Map.Entry<String, OTransactionIndexChanges> change : tx.getIndexOperations().entrySet()) {
      OIndex index = getMetadata().getIndexManagerInternal().getIndex(this, change.getKey());
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType())
          || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name().equals(index.getType())
          || OClass.INDEX_TYPE.DICTIONARY.name().equals(index.getType())
          || OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name().equals(index.getType())) {

        String name = index.getName();
        for (OTransactionIndexChangesPerKey changesPerKey :
            change.getValue().changesPerKey.values()) {
          int keyVersion = getVersionForIndexKey(tx, name, changesPerKey.key, isCoordinator);
          keys.add(new OPair<>(name + "#" + changesPerKey.key, keyVersion));
        }
        if (!change.getValue().nullKeyChanges.isEmpty()) {
          int keyVersion = getVersionForIndexKey(tx, name, null, isCoordinator);
          keys.add(new OPair<>(name + "#null", keyVersion));
        }
      }
    }
    for (OPair<String, Integer> key : keys) {
      OTransactionId txId = txContext.acquireIndexKeyPromise(key.getKey(), key.getValue(), force);
      if (txId != null) {
        txsWithBrokenPromises.add(txId);
      }
    }

    if (!txsWithBrokenPromises.isEmpty() && logger.isDebugEnabled()) {
      logger.debug(
          "Tx '%s' forcefully took over promises from transactions '%s'.",
          txContext.getTransactionId(), txsWithBrokenPromises.toString());
    }
  }

  public boolean beginDistributedTx(
      ODistributedRequestId requestId,
      OTransactionIdPromise id,
      OTransactionInternal tx,
      boolean isCoordinator,
      int retryCount) {
    final ODistributedDatabase localDistributedDatabase = getDistributedShared();
    final ONewDistributedTxContextImpl txContext =
        new ONewDistributedTxContextImpl(getSharedContext(), requestId, tx, id);
    try {
      internalBegin2pc(txContext, isCoordinator);
      txContext.setStatus(SUCCESS);
      register(requestId, localDistributedDatabase, txContext);
    } catch (OConcurrentCreateException ex) {
      if (retryCount >= 0
          && retryCount
              < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getExpectedRid().getClusterPosition() > ex.getActualRid().getClusterPosition()) {
          logger.debug(
              "Allocation of rid not match, expected:%s actual:%s waiting for re-enqueue"
                  + " request",
              ex.getExpectedRid(), ex.getActualRid());
          txContext.releasePromises();
          return false;
        }
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (OConcurrentModificationException ex) {
      if (retryCount >= 0
          && retryCount
              < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getEnhancedRecordVersion() > ex.getEnhancedDatabaseVersion()) {
          logger.info(
              "Persistent version not match, record:%s expected:%s actual:%s waiting for"
                  + " re-enqueue request",
              ex.getRid(), ex.getEnhancedRecordVersion(), ex.getEnhancedDatabaseVersion());
          txContext.releasePromises();
          return false;
        }
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (ORecordNotFoundException e) {
      // This error can happen only in deserialization before locks happen, no need to unlock
      if (retryCount >= 0
          && retryCount
              < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        return false;
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw e;
    } catch (ODistributedRecordLockedException
        | ODistributedKeyLockedException
        | OInvalidSequentialException ex) {
      /// ?? do i've to save this state as well ?
      txContext.setStatus(TIMEDOUT);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (ORecordDuplicatedException ex) {
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (OLowDiskSpaceException ex) {
      getContext().setDatabaseStatus(getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } catch (OModificationOperationProhibitedException e) {
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw e;
    }
    return true;
  }

  private void waitQuorumOnline() {
    if (this.neverWaited) {
      long waitTime =
          getConfiguration()
              .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DATABASE_ONLINE_GRACE_PERIOD);

      try {
        getContext()
            .getNodeState()
            .getOps()
            .waitOnlineQuorum(getDatabaseId(), Optional.of(waitTime));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      this.neverWaited = false;
    }
  }

  public ODistributedDatabase getDistributedShared() {
    return getSharedContext().getDistributedContext();
  }

  public void register(
      ODistributedRequestId requestId,
      ODistributedDatabase localDistributedDatabase,
      ODistributedTxContext txContext) {
    localDistributedDatabase.registerTxContext(requestId, txContext);
  }

  /**
   * The Local commit is different from a remote commit due to local rid pre-allocation
   *
   * @param transactionId
   */
  public void commit2pcLocal(ODistributedRequestId transactionId) {
    commit2pc(transactionId, true, transactionId);
  }

  /**
   * @param transactionId
   * @return null returned means that commit failed
   */
  public boolean commit2pc(
      ODistributedRequestId transactionId, boolean isCoordinator, ODistributedRequestId requestId) {
    waitQuorumOnline();
    ODistributedDatabaseImpl localDistributedDatabase =
        (ODistributedDatabaseImpl) getDistributedShared();
    localDistributedDatabase.resetLastValidBackup();

    var transactionSequence = getSharedContext().getTransactionSequence();
    ONewDistributedTxContextImpl txContext =
        (ONewDistributedTxContextImpl) localDistributedDatabase.getTxContext(transactionId);

    if (txContext != null) {
      if (SUCCESS.equals(txContext.getStatus())) {
        try {
          // make sure you still have the promises
          for (OTxPromise<ORID> p : txContext.getPromisedRids()) {
            txContext.acquirePromise(p.getKey(), p.getVersion(), false);
          }
          for (OTxPromise<Object> p : txContext.getPromisedKeys()) {
            txContext.acquireIndexKeyPromise(p.getKey(), p.getVersion(), false);
          }
        } catch (ODistributedRecordLockedException | ODistributedKeyLockedException e) {
          // This should not happen!
          throw new ODistributedException(
              String.format(
                  "Locks for tx '%s' are no longer valid in the second phase despite successful"
                      + " first phase",
                  transactionId));
        }
        try {
          txContext.commit(this);
          localDistributedDatabase.popTxContext(transactionId);
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
        } catch (OTransactionAlreadyPresentException e) {
          // DO Nothing already present
          txContext.destroy();
          localDistributedDatabase.popTxContext(transactionId);
        } catch (RuntimeException | Error e) {
          txContext.destroy();
          localDistributedDatabase.popTxContext(transactionId);
          getContext().execute(this::maybeSync);
          throw e;
        } finally {
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }
        return true;
      } else { // commit although first phase failed
        int nretry = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
        int delay = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);

        Random random = new Random();
        for (int i = 0; i < nretry; i++) {
          try {
            if (i > 0) {
              try {
                Thread.sleep(random.nextInt(delay));
              } catch (InterruptedException e) {
                OException.wrapException(new OInterruptedException(e.getMessage()), e);
              }
            }
            ValidationResult validateResult = transactionSequence.validate(txContext.getPromise());

            if (validateResult == ValidationResult.ALREADY_PRESENT) {
              // Already present do nothing.
              txContext.destroy();
              localDistributedDatabase.popTxContext(transactionId);
              return true;
            } else if (validateResult != ValidationResult.MISSING_PREVIOUS) {
              // must commit. try to force the promise.
              internalBegin2pc(txContext, isCoordinator, true);
              txContext.setStatus(SUCCESS);
              break;
            }
          } catch (ODistributedRecordLockedException | ODistributedKeyLockedException ex) {
            // Just retry
          } catch (ODistributedTxPromiseRequestIsOldException ex) {
            logger.warn("Error committing transaction %s ", ex, transactionId);
            return true;
          } catch (Exception ex) {
            logger.warn("Error beginning timed out transaction: %s ", ex, transactionId);
            break;
          }
        }
        if (SUCCESS.equals(txContext.getStatus())) {
          try {
            txContext.commit(this);
            localDistributedDatabase.popTxContext(transactionId);
            OLiveQueryHook.notifyForTxChanges(this);
            OLiveQueryHookV2.notifyForTxChanges(this);
            return true;
          } catch (OTransactionAlreadyPresentException e) {
            // DO Nothing already present
            txContext.destroy();
            localDistributedDatabase.popTxContext(transactionId);
          } catch (RuntimeException | Error e) {
            txContext.destroy();
            localDistributedDatabase.popTxContext(transactionId);
            getContext().execute(this::maybeSync);

            throw e;
          } finally {
            OLiveQueryHook.removePendingDatabaseOps(this);
            OLiveQueryHookV2.removePendingDatabaseOps(this);
          }
        } else {
          txContext.destroy();
          localDistributedDatabase.popTxContext(transactionId);
          getContext()
              .execute(
                  () -> {
                    logger.warn(
                        "Reached limit of retry for commit tx:%s forcing database re-install",
                        transactionId);
                    maybeSync();
                  });
          return true;
        }
      }
    }
    return false;
  }

  public void maybeSync() {
    getContext().installDatabase(this.getName(), false, true);
  }

  public boolean rollback2pc(ODistributedRequestId transactionId) {
    ODistributedDatabase localDistributedDatabase = getDistributedShared();
    ODistributedTxContext txContext = localDistributedDatabase.popTxContext(transactionId);
    if (txContext != null) {
      txContext.destroy();
      OLiveQueryHook.removePendingDatabaseOps(this);
      OLiveQueryHookV2.removePendingDatabaseOps(this);
      return true;
    }
    return false;
  }

  public void internalCommit2pc(ONewDistributedTxContextImpl txContext) {
    OTransaction pre = this.currentTx;
    try {
      OTransactionInternal tx = txContext.getTransaction();
      this.currentTx = tx;
      tx.setDatabase(this);
      this.getStorage().commitPreAllocated(tx);
    } catch (OLowDiskSpaceException ex) {
      getContext().setDatabaseStatus(getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } finally {
      this.currentTx = pre;
      txContext.destroy();
    }
  }

  public void internalBegin2pc(ONewDistributedTxContextImpl txContext, boolean isCoordinator) {
    internalBegin2pc(txContext, isCoordinator, false);
  }

  /**
   * @param txContext
   * @param isCoordinator specifies whether this node is the tx coordinator
   * @param force whether to use the force flag for acquiring the promises required for this tx.
   */
  public void internalBegin2pc(
      ONewDistributedTxContextImpl txContext, boolean isCoordinator, boolean force) {
    OTransaction pre = this.currentTx;
    OTransactionInternal transaction = txContext.getTransaction();
    // This is moved before checks because also the coordinator first node allocate before checks
    try {
      currentTx = transaction;
      if (!isCoordinator) {
        ((OTransactionOptimisticDistributed) transaction).setDatabase(this);
        ((OTransactionOptimistic) transaction).begin();
      }

      if (isCoordinator) {
        // make sure the create record operations have a valid id assigned that is used also on the
        // followers.
        getStorage().preallocateRids(transaction);
      }

      acquireLocksForTx(transaction, txContext, isCoordinator, force);

      firstPhaseDataChecks(isCoordinator, transaction, txContext);
    } finally {
      this.currentTx = pre;
    }
  }

  private void firstPhaseDataChecks(
      final boolean isCoordinator,
      final OTransactionInternal transaction,
      final ONewDistributedTxContextImpl txContext) {

    if (!isCoordinator) {
      getStorage().preallocateRids(transaction);
    }

    for (Map.Entry<String, OTransactionIndexChanges> change :
        transaction.getIndexOperations().entrySet()) {
      final String indexName = change.getKey();
      final OIndex index = getSharedContext().getIndexManager().getRawIndex(indexName);
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType())
          || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name().equals(index.getType())) {
        OTransactionIndexChangesPerKey nullKeyChanges = change.getValue().nullKeyChanges;
        if (!nullKeyChanges.isEmpty()) {
          OIdentifiable old;
          try (Stream<ORID> stream = index.getInternal().getRids(null)) {
            old = stream.findFirst().orElse(null);
          }
          Object newValue =
              nullKeyChanges.getEntriesAsList().get(nullKeyChanges.size() - 1).getValue();
          if (old != null && !old.equals(newValue)) {
            boolean oldValueRemoved = false;
            for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
                nullKeyChanges.getEntriesAsList()) {
              if (entry.getValue() != null
                  && entry.getValue().equals(old)
                  && entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
                oldValueRemoved = true;
                break;
              }
            }
            if (!oldValueRemoved) {
              throw new ORecordDuplicatedException(
                  String.format(
                      "Cannot index record %s: found duplicated key '%s' in index '%s' previously"
                          + " assigned to the record %s",
                      newValue, null, getName(), old.getIdentity()),
                  getName(),
                  old.getIdentity(),
                  null);
            }
          }
        }

        for (final OTransactionIndexChangesPerKey changesPerKey :
            change.getValue().changesPerKey.values()) {
          OIdentifiable old;
          if (!isCoordinator) {
            // Version check need to be done only from the nodes that are not coordinating the
            // transaction.
            final long version = getStorage().getVersionForKey(indexName, changesPerKey.key);
            int sourceVersion =
                ((OTransactionOptimisticDistributed) transaction)
                    .getVersionForKey(indexName, changesPerKey.key);
            if (version != sourceVersion) {
              throw new OInvalidSequentialException();
            }
          }
          try (final Stream<ORID> rids = index.getInternal().getRidsIgnoreTx(changesPerKey.key)) {
            old = rids.findFirst().orElse(null);
          }
          if (!changesPerKey.isEmpty()) {
            Object newValue =
                changesPerKey.getEntriesAsList().get(changesPerKey.size() - 1).getValue();
            if (old != null && !old.equals(newValue)) {
              boolean oldValueRemoved = false;
              for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
                  changesPerKey.getEntriesAsList()) {
                if (entry.getValue() != null
                    && entry.getValue().equals(old)
                    && entry.getOperation() == OTransactionIndexChanges.OPERATION.REMOVE) {
                  oldValueRemoved = true;
                  break;
                }
              }
              if (!oldValueRemoved) {
                throw new ORecordDuplicatedException(
                    String.format(
                        "Cannot index record %s: found duplicated key '%s' in index '%s' previously"
                            + " assigned to the record %s",
                        newValue, changesPerKey.key, getName(), old.getIdentity()),
                    getName(),
                    old.getIdentity(),
                    changesPerKey.key);
              }
            }
          }
        }
      }
    }
    for (ORecordOperation entry : transaction.getRecordOperations()) {
      if (entry.getType() != ORecordOperation.CREATED) {
        int changeVersion = entry.getRecord().getVersion();
        ORecordMetadata metadata = getStorage().getRecordMetadata(entry.getRID());
        if (metadata == null) {
          if (getStorage().isDeleted(entry.getRID())) {
            throw new OConcurrentModificationException(
                entry.getRID(), changeVersion, changeVersion, entry.getType());
          } else {
            // don't exist i get -1, -1 rid that put the operation in queue for retry.
            throw new OConcurrentCreateException(new ORecordId(-1, -1), entry.getRID());
          }
        }
        int persistentVersion = metadata.getVersion();
        boolean checkVersion;
        if (entry.getType() == ORecordOperation.UPDATED) {
          checkVersion = ORecordInternal.isContentChanged(entry.getRecord());
        } else {
          checkVersion = true;
        }
        if (changeVersion != persistentVersion && checkVersion) {
          throw new OConcurrentModificationException(
              entry.getRID(), persistentVersion, changeVersion, entry.getType());
        }
      }
    }
  }

  @Override
  public OView getViewFromCluster(int cluster) {
    OImmutableSchema schema = getMetadata().getImmutableSchemaSnapshot();
    OView view = schema.getViewByClusterId(cluster);
    if (view == null) {
      String viewName = getSharedContext().getViewManager().getViewFromOldCluster(cluster);
      if (viewName != null) {
        view = schema.getView(viewName);
      }
    }
    return view;
  }

  public OEnterpriseEndpoint getEnterpriseEndpoint() {
    OServer server = getContext().getServer();
    return server.getPlugins().stream()
        .map(OServerPluginInfo::getInstance)
        .filter(OEnterpriseEndpoint.class::isInstance)
        .findFirst()
        .map(OEnterpriseEndpoint.class::cast)
        .orElse(null);
  }

  public ODistributedServerManager getDistributedManager() {
    return distributedManager;
  }

  public void sendDDLCommand(String command, boolean excludeLocal) {
    twoPhaseDDL(command);
  }

  public void twoPhaseDDL(String command) {
    if (isLocalEnv()) {
      // ALREADY DISTRIBUTED
      super.command(command, new Object[] {}).close();
      return;
    }
    getContext().checkNodeIsMaster(getLocalNodeId(), getName(), "Command '" + command + "'");
    waitQuorumOnline();
    var transactionSequence = getSharedContext().getTransactionSequence();
    // The plus 1 is for make sure it runs once even if retry is 0
    int nretry =
        getConfiguration()
            .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
    int retryDelay =
        this.getConfiguration()
            .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);

    var retryInfo = new ORetryInfo(nretry, retryDelay);

    while (!retryInfo.isFinished()) {
      var ids = transactionSequence.nextDDL();
      if (ids.isPresent()) {
        if (coordinateTwoPhaseDDL(command, ids.get(), retryInfo.isFinished())) {
          return;
        }
        if (retryInfo.isFinished()) {
          getContext().execute(this::maybeSync);
        }
      } else {
        var delay = retryInfo.nextRetry();
        if (delay.isPresent()) {
          try {
            Thread.sleep(delay.get());
          } catch (InterruptedException e) {
            OException.wrapException(new OInterruptedException(e.getMessage()), e);
          }
        }
      }
    }
    throw new ODistributedOperationException("Reached number of retry to execute ddl");
  }

  private boolean coordinateTwoPhaseDDL(
      String command,
      ORawPair<OTransactionIdPromise, OTransactionIdPromise> ids,
      boolean excpetionOnFail) {
    OSQLCommandTaskFirstPhase task =
        new OSQLCommandTaskFirstPhase(command, ids.getFirst(), ids.getSecond());
    logger.debugNode(
        getLocalNodeId(),
        "Starting two phase ddl '%s' before:%s after:%s",
        command,
        ids.getFirst(),
        ids.getSecond());
    ODistributedServerManager dManager = getDistributedManager();
    Set<String> nodes = getContext().getAvailableNodeNames(getName());

    ODistributedRequestId reqId = dManager.nextRequestId();
    ODistributedTxResponseManagerImpl responseManager = sendTask(nodes, task, null, reqId);

    if (responseManager.isQuorumReached()) {
      var results = responseManager.getDistributedTxFinalResponse();
      assert results.isPresent();
      OTransactionResultPayload resultPayload = results.get();
      switch (resultPayload.getResponseType()) {
        case OTxSuccess.ID:
          // Success send ok
          confirmPhase2DDL(nodes, reqId, ids, true);
          logger.debugNode(getLocalNodeId(), "Success of two phase ddl '%s' ", command);
          return true;
        case OTxException.ID:
          // Exception send ko and throws the exception
          confirmPhase2DDL(nodes, reqId, ids, false);
          logger.debugNode(
              getLocalNodeId(),
              "Quorum exception of two phase ddl '%s' '%s' ",
              command,
              resultPayload);
          throw ((OTxException) resultPayload).getException();
        case OTxInvalidSequential.ID:
          logger.debugNode(
              getLocalNodeId(),
              "Quorum invalid sequential of two phase ddl '%s' ",
              command,
              resultPayload);
          confirmPhase2DDL(nodes, reqId, ids, false);
          return false;
      }

      for (OTransactionResultPayload result : responseManager.getAllResponses()) {
        if (result.getResponseType() == OTxException.ID) {
          logger.warn("One node on error", ((OTxException) result).getException());
        }
      }
      return false;
    } else {
      confirmPhase2DDL(nodes, reqId, ids, false);
      List<OTransactionResultPayload> results = responseManager.getAllResponses();
      // If quorum is not reached is enough on a Lock timeout to trigger a deadlock retry.
      List<Exception> exceptions = new ArrayList<>();
      List<String> messages = new ArrayList<>();
      for (OTransactionResultPayload result : results) {
        String node = responseManager.getNodeNameFromPayload(result);
        switch (result.getResponseType()) {
          case OTxSuccess.ID:
            messages.add("node: " + node + " success");
            break;
          case OTxException.ID:
            exceptions.add(((OTxException) result).getException());
            logger.debug("distributed exception", ((OTxException) result).getException());
            messages.add(
                String.format(
                    "exception (node %s ): '%s'",
                    node, ((OTxException) result).getException().getMessage()));
            break;
          case OTxInvalidSequential.ID:
            logger.debugNode(
                getLocalNodeId(),
                "Failed two phase ddl '%s' before:%s after:%s, Invalid sequential",
                command,
                ids.getFirst(),
                ids.getSecond());
            return false;
        }
      }
      logger.debugNode(
          getLocalNodeId(),
          "Failed two phase ddl '%s' before:%s after:%s, reason: %s",
          command,
          ids.getFirst(),
          ids.getSecond(),
          messages);
      if (!excpetionOnFail) {
        ODistributedOperationException ex =
            new ODistributedOperationException(
                String.format(
                    "Command `%s` didn't reach the quorum of '%d', responses: [%s]",
                    command, responseManager.getQuorum(), String.join(",", messages)));
        for (Exception e : exceptions) {
          ex.addSuppressed(e);
        }
        throw ex;
      } else {
        return false;
      }
    }
  }

  private void confirmPhase2DDL(
      Set<String> nodes,
      ODistributedRequestId messageId,
      ORawPair<OTransactionIdPromise, OTransactionIdPromise> ids,
      boolean apply) {
    ODistributedServerManager dManager = getDistributedManager();
    ODistributedResponse response =
        dManager.sendRequest(
            getName(),
            nodes,
            new OSQLCommandTaskSecondPhase(messageId, ids.getFirst(), ids.getSecond(), apply));
    if (response != null && response.getPayload() instanceof RuntimeException) {
      throw (RuntimeException) response.getPayload();
    }
  }

  private ODistributedTxResponseManagerImpl sendTask(
      Collection<String> nodes, ORemoteTask task, Object localResult, ODistributedRequestId next) {
    ODistributedServerManager dManager = getDistributedManager();
    final class HoldResponseManager {
      ODistributedTxResponseManagerImpl responseManager;
    }

    final HoldResponseManager holder = new HoldResponseManager();
    ((ODistributedPlugin) dManager)
        .sendRequest(
            getName(),
            nodes,
            task,
            next,
            localResult,
            ((iRequest,
                iNodes,
                iTask,
                nodesConcurToTheQuorum,
                availableNodes,
                expectedResponses,
                quorum,
                groupByResponse,
                waitLocalNode) -> {
              holder.responseManager =
                  new ODistributedTxResponseManagerImpl(
                      iTask,
                      iNodes,
                      nodesConcurToTheQuorum,
                      availableNodes,
                      expectedResponses,
                      quorum);
              return holder.responseManager;
            }));
    return holder.responseManager;
  }

  @Override
  public int addCluster(String iClusterName) {
    if (!isLocalEnv()) {
      final StringBuilder cmd = new StringBuilder("create cluster `");
      cmd.append(iClusterName);
      cmd.append("`");
      sendDDLCommand(cmd.toString(), false);
      return getClusterIdByName(iClusterName);
    } else {
      return super.addCluster(iClusterName);
    }
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId) {
    if (!isLocalEnv()) {
      final StringBuilder cmd = new StringBuilder("create cluster `");
      cmd.append(iClusterName);
      cmd.append("`");
      cmd.append(" ID ");
      cmd.append(iRequestedId);
      sendDDLCommand(cmd.toString(), false);
      return iRequestedId;
    } else {
      return super.addCluster(iClusterName, iRequestedId);
    }
  }

  @Override
  protected boolean dropClusterInternal(String clusterName) {
    if (!isLocalEnv()) {
      final String cmd = "drop cluster `" + clusterName + "`";
      sendDDLCommand(cmd, false);
      return true;
    } else {
      return super.dropClusterInternal(clusterName);
    }
  }

  @Override
  public boolean dropClusterInternal(int clusterId) {
    if (!isLocalEnv()) {
      final String cmd = "drop cluster " + clusterId + "";
      sendDDLCommand(cmd, false);
      return true;
    } else {
      return super.dropClusterInternal(clusterId);
    }
  }

  public boolean isLocalEnv() {
    return OScenarioThreadLocal.instance().isRunModeDistributed();
  }

  /** {@inheritDoc} */
  @Override
  public void freeze(final boolean throwException) {
    ((ODistributedDatabaseImpl) getDistributedShared()).freezeStatus();
    super.freeze(throwException);
  }

  @Override
  public void release() {
    ((ODistributedDatabaseImpl) getDistributedShared()).releaseStatus();
    super.release();
  }

  @Override
  public List<String> backup(
      OutputStream out,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener,
      int compressionLevel,
      int bufferSize)
      throws IOException {
    try {
      return super.backup(out, options, callable, iListener, compressionLevel, bufferSize);
    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error on executing backup"), e);
    }
  }

  protected void handleDistributedException(
      final String iMessage, final Exception e, final Object... iParams) {
    if (e != null) {
      if (e instanceof OException) throw (OException) e;
      else if (e.getCause() instanceof OException) throw (OException) e.getCause();
      else if (e.getCause() != null && e.getCause().getCause() instanceof OException)
        throw (OException) e.getCause().getCause();
    }

    logger.error(iMessage, e, iParams);
    throw OException.wrapException(new OStorageException(String.format(iMessage, iParams)), e);
  }

  @Override
  public String getStorageId() {
    return getLocalNodeName() + "." + getName();
  }

  protected Set<String> getInvolvedClusters(final Iterable<ORecordOperation> uResult) {
    final Set<String> involvedClusters = new HashSet<>();
    for (ORecordOperation op : uResult) {
      final ORecord record = op.getRecord();
      involvedClusters.add(getClusterNameById(record.getIdentity().getClusterId()));
    }
    return involvedClusters;
  }

  @Override
  public void syncCommit(OTransactionData data) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          assert !this.getTransaction().isActive();
          OTransactionOptimistic tx = new OTransactionOptimistic(this);
          data.fill(tx, this);
          ONodeId nodeId = getLocalNodeId();

          OTransactionIdPromise primise =
              new OTransactionIdPromise(nodeId, data.getTransactionId());
          ONewDistributedTxContextImpl txContext =
              new ONewDistributedTxContextImpl(
                  getSharedContext(), new ODistributedRequestId(getLocalNodeId(), -1), tx, primise);

          var transactionSequence = getSharedContext().getTransactionSequence();
          transactionSequence.validate(primise);
          getStorage().preallocateRids(tx);
          txContext.commit(this);
          return null;
        });
  }

  public OTransactionResultPayload firstPhaseDDL(
      String query,
      OTransactionIdPromise preChangeId,
      OTransactionIdPromise afterChangeId,
      ODistributedRequestId requestId) {
    OSharedContextDistributed distributedSharedContext = getSharedContext();
    var transactionSequence = distributedSharedContext.getTransactionSequence();
    ODistributedDatabase localDistributedDatabase = getDistributedShared();
    ODDLContextImpl ddlContext =
        new ODDLContextImpl(distributedSharedContext, query, preChangeId, afterChangeId, requestId);
    register(requestId, localDistributedDatabase, ddlContext);
    ValidationResult first = transactionSequence.validate(preChangeId);
    ValidationResult second = transactionSequence.validate(afterChangeId);
    if (first == ValidationResult.ALREADY_PROMISED || second == ValidationResult.ALREADY_PROMISED) {
      ddlContext.setStatus(TIMEDOUT);
      return new OTxInvalidSequential();
    } else if (first == ValidationResult.MISSING_PREVIOUS
        || second == ValidationResult.MISSING_PREVIOUS) {
      var preC = transactionSequence.debugStatus(preChangeId.getId().getPosition());
      var afterC = transactionSequence.debugStatus(afterChangeId.getId().getPosition());
      ddlContext.setStatus(TIMEDOUT);
      getContext()
          .execute(
              () -> {
                logger.warnNode(
                    getLocalNodeName(),
                    "Missing DDL operation pre:%d!=%d after %d!=%d, forcing database '%s'"
                        + " re-install, operation '%s'",
                    preC,
                    preChangeId.getId().getSequence(),
                    afterC,
                    afterChangeId.getId().getSequence(),
                    getName(),
                    query);

                maybeSync();
              });
      return new OTxInvalidSequential();
    } else if (first == ValidationResult.ALREADY_PRESENT
        || second == ValidationResult.ALREADY_PRESENT) {
      ddlContext.setStatus(TIMEDOUT);
      return new OTxInvalidSequential();
    }
    ddlContext.setStatus(SUCCESS);
    return new OTxSuccess();
  }

  public void secondPhaseDDL(ODistributedRequestId confirmSentRequest, boolean apply) {
    ODistributedDatabase localDistributedDatabase = getDistributedShared();
    ODDLContextImpl context =
        (ODDLContextImpl) localDistributedDatabase.popTxContext(confirmSentRequest);
    var transactionSequence = getSharedContext().getTransactionSequence();
    OStorage storage = getStorage();
    if (apply) {
      ((ODistributedDatabaseImpl) localDistributedDatabase).resetLastValidBackup();
      if (context.getStatus() == SUCCESS) {
        OTxMetadataHolder preMetadata =
            transactionSequence.notifySuccess(context.getPreChangePromise());

        storage.metadataOnly(preMetadata.metadata());
        preMetadata.notifyMetadataRead();
        String query = context.getQuery();
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              command(query, new Object[] {});
              return null;
            });

        OTxMetadataHolder afterMetadata =
            transactionSequence.notifySuccess(context.getAfterChangePromise());
        storage.metadataOnly(afterMetadata.metadata());
        afterMetadata.notifyMetadataRead();
      } else {
        int nretry = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
        int delay = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);

        Random random = new Random();
        for (int i = 0; i < nretry; i++) {
          try {
            if (i > 0) {
              try {
                Thread.sleep(random.nextInt(delay));
              } catch (InterruptedException e) {
                OException.wrapException(new OInterruptedException(e.getMessage()), e);
              }
            }
            OTransactionResultPayload firstPhase =
                firstPhaseDDL(
                    context.getQuery(),
                    context.getPreChangePromise(),
                    context.getAfterChangePromise(),
                    context.getReqId());
            context = (ODDLContextImpl) localDistributedDatabase.popTxContext(confirmSentRequest);
            if (firstPhase instanceof OTxSuccess) {
              break;
            }
          } catch (Exception ex) {
            logger.warn("Error beginning timed out transaction: %s ", ex, context.getReqId());
            break;
          }
        }
        if (SUCCESS.equals(context.getStatus())) {
          try {
            String query = context.getQuery();
            OScenarioThreadLocal.executeAsDistributed(
                () -> {
                  command(query, new Object[] {});
                  return null;
                });
          } catch (RuntimeException | Error e) {
            getContext().execute(this::maybeSync);

            throw e;
          }
        } else {
          ODistributedRequestId id = context.getReqId();
          getContext()
              .execute(
                  () -> {
                    logger.warn(
                        "Reached limit of retry for commit tx:%s forcing database re-install", id);
                    maybeSync();
                  });
        }
      }
    } else if (context != null) {
      transactionSequence.notifyFailure(context.getPreChangePromise());
      transactionSequence.notifyFailure(context.getAfterChangePromise());
    }
  }

  public OrientDBDistributed getContext() {
    return this.getSharedContext().getOrientDB();
  }

  public ONodeId getLocalNodeId() {
    return getContext().getNodeId();
  }

  @Override
  public OSharedContextDistributed getSharedContext() {
    return (OSharedContextDistributed) super.getSharedContext();
  }

  @Override
  public OResult getDistributedInfo() {

    var id = getDatabaseId();
    ODatabasesTopology topology = getContext().getNodeState().getDatabaseTopology();
    var members = topology.getMembers(id);
    var name = getName();
    var mi =
        members.stream()
            .map(
                (x) -> {
                  return new OMemberInfo(x, topology.getRole(id, x));
                })
            .toList();

    var listCl = new ArrayList<OAllocationInfoOClass>();
    for (var cl : getMetadata().getSchema().getClasses()) {
      OClassAllocation allocation = cl.getAllocation();
      if (allocation != null) {
        var infoNodes = new ArrayList<OAllocationInfoOClassNode>();
        var nodes = allocation.getDefinedNodes();
        for (var node : nodes) {
          var clus = allocation.getAllocationClusters(node);
          infoNodes.add(new OAllocationInfoOClassNode(new ONodeId(node), clus));
        }
        listCl.add(new OAllocationInfoOClass(cl.getName(), infoNodes));
      }
    }

    OAllocationInfo ai = new OAllocationInfo(listCl);

    ODistributedDatabaseInfo info =
        new ODistributedDatabaseInfo(name, id, mi, topology.getQuorum(id), ai);

    return info.toLegacyResult();
  }

  @Override
  public void autoAssignAllocations(boolean canCreateNewClusters) {
    if (!getContext().isNodeMaster(getLocalNodeName(), getName()))
      // NO MASTER, DON'T CREATE LOCAL CLUSTERS
      return;
    var context = getSharedContext();
    context.getSchema().acquireSchemaWriteLock(this);
    try {
      logger.infoNode(
          getLocalNodeId(), "Reassigning ownership of clusters for database %s...", getName());
      final Set<String> availableNodes = getContext().getAvailableNodeNames(getName());

      final OSchema schema = getMetadata().getSchema();
      // FILTER OUT NON MASTER SERVER
      for (Iterator<String> it = availableNodes.iterator(); it.hasNext(); ) {
        final String node = it.next();
        if (getContext().isNodeMaster(node, getName())) it.remove();
      }

      for (final OClass clazz : schema.getClasses()) {
        ((OClassDistributed) clazz)
            .autoAssignClusterOwnership(this, availableNodes, canCreateNewClusters);
      }

      logger.infoNode(
          getLocalNodeId(),
          "Reassignment of clusters for database '%s' completed (classes=%d)",
          getName(),
          schema.getClasses().size());

    } finally {
      context.getSchema().releaseSchemaWriteLock(this);
    }
  }
}
