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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
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
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequest.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedTxContext;
import com.orientechnologies.orient.server.distributed.OWriteOperationNotPermittedException;
import com.orientechnologies.orient.server.distributed.exception.ODistributedTxPromiseRequestIsOldException;
import com.orientechnologies.orient.server.distributed.exception.OTransactionAlreadyPresentException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

  private final ODistributedPlugin distributedManager;

  public ODatabaseDocumentDistributed(OStorage storage, ODistributedPlugin distributedPlugin) {
    super(storage);
    this.distributedManager = distributedPlugin;
  }

  /**
   * return the name of local node in the cluster
   *
   * @return the name of local node in the cluster
   */
  public String getLocalNodeName() {
    return distributedManager.getLocalNodeName();
  }

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values
   * contain names of clusters (data files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  public Map<String, Set<String>> getActiveClusterMap() {
    if (distributedManager.isOffline()
        || !distributedManager.isNodeOnline(distributedManager.getLocalNodeName(), getName())
        || OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      return super.getActiveClusterMap();
    }
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getDistributedConfiguration();

    for (String server : distributedManager.getActiveServers()) {
      if (getClustersOnServer(cfg, server).contains("*")) {
        // TODO check this!!!
        result.put(server, new HashSet<String>(getClusterNames()));
      } else {
        result.put(server, getClustersOnServer(cfg, server));
      }
    }
    return result;
  }

  public Set<String> getClustersOnServer(ODistributedConfiguration cfg, String server) {
    Set<String> result = cfg.getClustersOnServer(server);
    if (result.contains("*")) {
      result.remove("*");
      HashSet<String> more = new HashSet<>();
      more.addAll(getClusterNames());
      for (String s : cfg.getClusterNames()) {
        if (!cfg.getServers(s, null).contains(s)) {
          more.remove(s);
        }
      }
      result.addAll(more);
    }
    return result;
  }

  /**
   * returns the data center map for current deploy. The keys are data center names, the values are
   * node names per data center
   *
   * @return data center map for current deploy
   */
  public Map<String, Set<String>> getActiveDataCenterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getDistributedConfiguration();
    Set<String> servers = cfg.getRegisteredServers();
    for (String server : servers) {
      String dc = cfg.getDataCenterOfServer(server);
      Set<String> dcConfig = result.get(dc);
      if (dcConfig == null) {
        dcConfig = new HashSet<>();
        result.put(dc, dcConfig);
      }
      dcConfig.add(server);
    }
    return result;
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
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentDistributed database =
        new ODatabaseDocumentDistributed(getStorage(), distributedManager);
    database.init(getConfig(), getSharedContext());
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

    return distributedManager.installDatabase(true, databaseName, forceDeployment, tryWithDelta);
  }

  @Override
  public Map<String, Object> getHaStatus(
      boolean servers, boolean db, boolean latency, boolean messages) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    if (distributedManager == null || !distributedManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    final ODistributedConfiguration cfg = distributedManager.getDatabaseConfiguration(databaseName);

    Map<String, Object> row = new HashMap<>();
    final StringBuilder output = new StringBuilder();
    if (servers) row.put("servers", distributedManager.getClusterConfiguration());
    if (db) row.put("database", cfg.getDocument());
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

    if (distributedManager == null || !distributedManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    // The last parameter (true) indicates to set the node's database status to OFFLINE.
    // If this is changed to false, the node will be set to NOT_AVAILABLE, and then the
    // auto-repairer will
    // re-synchronize the database on the node, and then set it to ONLINE.
    return distributedManager.removeNodeFromConfiguration(serverName, databaseName, false, true);
  }

  @Override
  public OResultSet queryOnNode(
      String nodeName, OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    ORunQueryExecutionPlanTask task =
        new ORunQueryExecutionPlanTask(executionPlan, inputParameters, nodeName);
    ODistributedResponse result = executeTaskOnNode(task, nodeName);
    return task.getResult(result, this);
  }

  public ODistributedResponse executeTaskOnNode(ORemoteTask task, String nodeName) {

    if (distributedManager == null || !distributedManager.isEnabled())
      throw new ODistributedException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return distributedManager.sendRequest(
        databaseName,
        null,
        Collections.singletonList(nodeName),
        task,
        distributedManager.getNextMessageIdCounter(),
        ODistributedRequest.EXECUTION_MODE.RESPONSE,
        null);
  }

  @Override
  public void init(OrientDBConfig config, OSharedContext sharedContext) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          super.init(config, sharedContext);
          return null;
        });
  }

  protected void createMetadata(OSharedContext ctx) {
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = ctx;
    metadata.init(shared);
    ((OSharedContextDistributed) shared).create(this);
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
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed()
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
  public <T> T sendSequenceAction(OSequenceAction action)
      throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  public void distributedCommitV1(OTransactionInternal iTx) {
    // This is future may handle a retry
    try {
      for (ORecordOperation txEntry : iTx.getRecordOperations()) {
        if (txEntry.type == ORecordOperation.CREATED || txEntry.type == ORecordOperation.UPDATED) {
          final ORecord record = txEntry.getRecord();
          if (record instanceof ODocument) ((ODocument) record).validate();
        }
      }
      ODistributedDatabase localDistributedDatabase = getDistributedShared();
      final ODistributedConfiguration dbCfg =
          localDistributedDatabase.getDistributedConfiguration();
      ODistributedServerManager dManager = getDistributedManager();
      final String localNodeName = dManager.getLocalNodeName();
      checkNodeIsMaster(localNodeName, dbCfg, "Transaction Commit");
      int nretry =
          this.getConfiguration()
              .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
      int delay =
          this.getConfiguration()
              .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);
      ODistributedTxCoordinator txManager =
          new ODistributedTxCoordinator(
              getStorage(),
              dManager,
              localDistributedDatabase,
              dManager.getMessageService(),
              dManager.getLocalNodeId(),
              dManager.getLocalNodeName(),
              nretry,
              delay);
      int quorum = 0;
      Set<String> clusters = getInvolvedClusters(iTx.getRecordOperations());
      for (String clusterName : clusters) {
        final List<String> clusterServers = dbCfg.getServers(clusterName, null);
        final int writeQuorum =
            dbCfg.getWriteQuorum(clusterName, clusterServers.size(), localNodeName);
        quorum = Math.max(quorum, writeQuorum);
      }
      final int availableNodes = dManager.getAvailableNodes(getName());

      if (quorum > availableNodes) {
        Set<String> online = dManager.getAvailableNodeNames(getName());
        throw new ODistributedException(
            String.format(
                "Not enough nodes online to execute the operation. Available nodes:%s, quorum:%s",
                online, quorum));
      }

      txManager.commit(this, iTx, clusters);
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
      return ((OAbstractPaginatedStorage) tx.getDatabase().getStorage())
          .getVersionForKey(index, key);
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

    if (!txsWithBrokenPromises.isEmpty() && OLogManager.instance().isDebugEnabled()) {
      OLogManager.instance()
          .debug(
              this,
              "Tx '%s' forcefully took over promises from transactions '%s'.",
              txContext.getTransactionId(),
              txsWithBrokenPromises.toString());
    }
  }

  public boolean beginDistributedTx(
      ODistributedRequestId requestId,
      OTransactionId id,
      OTransactionInternal tx,
      boolean isCoordinator,
      int retryCount) {
    final ODistributedDatabase localDistributedDatabase = getDistributedShared();
    final ONewDistributedTxContextImpl txContext =
        new ONewDistributedTxContextImpl(
            (ODistributedDatabaseImpl) localDistributedDatabase, requestId, tx, id);
    try {
      internalBegin2pc(txContext, isCoordinator);
      txContext.setStatus(SUCCESS);
      register(requestId, localDistributedDatabase, txContext);
    } catch (OConcurrentCreateException ex) {
      if (retryCount >= 0
          && retryCount
              < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getExpectedRid().getClusterPosition() > ex.getActualRid().getClusterPosition()) {
          OLogManager.instance()
              .debug(
                  this,
                  "Allocation of rid not match, expected:%s actual:%s waiting for re-enqueue request",
                  ex.getExpectedRid(),
                  ex.getActualRid());
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
          OLogManager.instance()
              .info(
                  this,
                  "Persistent version not match, record:%s expected:%s actual:%s waiting for re-enqueue request",
                  ex.getRid(),
                  ex.getEnhancedRecordVersion(),
                  ex.getEnhancedDatabaseVersion());
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
      distributedManager.setDatabaseStatus(
          getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } catch (OModificationOperationProhibitedException e) {
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw e;
    }
    return true;
  }

  public ODistributedDatabase getDistributedShared() {
    return getDistributedManager().getMessageService().getDatabase(getName());
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
    ODistributedDatabaseImpl localDistributedDatabase =
        (ODistributedDatabaseImpl) getDistributedShared();
    localDistributedDatabase.resetLastValidBackup();

    ODistributedServerManager manager = getDistributedManager();
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
                  "Locks for tx '%s' are no longer valid in the second phase despite successful first phase",
                  transactionId));
        }
        try {
          if (manager != null) {
            manager.messageCurrentPayload(requestId, txContext);
            manager.messageBeforeOp("commit", requestId);
          }
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
          Orient.instance()
              .submit(
                  () -> {
                    getDistributedManager().installDatabase(false, getName(), true, true);
                  });
          throw e;
        } finally {
          if (manager != null) {
            manager.messageAfterOp("commit", requestId);
          }
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }
        return true;
      } else { // commit although first phase failed
        int nretry = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
        int delay = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);

        for (int i = 0; i < nretry; i++) {
          try {
            if (i > 0) {
              try {
                Thread.sleep(new Random().nextInt(delay));
              } catch (InterruptedException e) {
                OException.wrapException(new OInterruptedException(e.getMessage()), e);
              }
            }
            ValidationResult validateResult =
                localDistributedDatabase.validate(txContext.getTransactionId());

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
            OLogManager.instance()
                .warn(
                    ODatabaseDocumentDistributed.this,
                    "Error committing transaction %s ",
                    ex,
                    transactionId);
            return true;
          } catch (Exception ex) {
            OLogManager.instance()
                .warn(
                    ODatabaseDocumentDistributed.this,
                    "Error beginning timed out transaction: %s ",
                    ex,
                    transactionId);
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
            Orient.instance()
                .submit(
                    () -> {
                      getDistributedManager().installDatabase(false, getName(), true, true);
                    });
            throw e;
          } finally {
            OLiveQueryHook.removePendingDatabaseOps(this);
            OLiveQueryHookV2.removePendingDatabaseOps(this);
          }
        } else {
          txContext.destroy();
          localDistributedDatabase.popTxContext(transactionId);
          Orient.instance()
              .submit(
                  () -> {
                    OLogManager.instance()
                        .warn(
                            ODatabaseDocumentDistributed.this,
                            "Reached limit of retry for commit tx:%s forcing database re-install",
                            transactionId);
                    distributedManager.installDatabase(
                        false, ODatabaseDocumentDistributed.this.getName(), true, true);
                  });
          return true;
        }
      }
    }
    return false;
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
    try {
      OTransactionInternal tx = txContext.getTransaction();
      tx.setDatabase(this);
      ((OAbstractPaginatedStorage) this.getStorage()).commitPreAllocated(tx);
    } catch (OLowDiskSpaceException ex) {
      distributedManager.setDatabaseStatus(
          getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } finally {
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
    final ODistributedDatabaseImpl localDb = (ODistributedDatabaseImpl) getDistributedShared();

    OTransactionInternal transaction = txContext.getTransaction();
    // This is moved before checks because also the coordinator first node allocate before checks
    if (!isCoordinator) {
      ((OTransactionOptimisticDistributed) transaction).setDatabase(this);
      ((OTransactionOptimistic) transaction).begin();
    }
    localDb.getManager().messageBeforeOp("locks", txContext.getReqId());

    if (isCoordinator) {
      // make sure the create record operations have a valid id assigned that is used also on the
      // followers.
      getDistributedShared().getManager().messageBeforeOp("allocate", txContext.getReqId());
      ((OAbstractPaginatedStorage) getStorage()).preallocateRids(transaction);
      getDistributedShared().getManager().messageAfterOp("allocate", txContext.getReqId());
    }

    acquireLocksForTx(transaction, txContext, isCoordinator, force);

    firstPhaseDataChecks(isCoordinator, transaction, txContext);
  }

  private void firstPhaseDataChecks(
      final boolean isCoordinator,
      final OTransactionInternal transaction,
      final ONewDistributedTxContextImpl txContext) {
    getDistributedShared().getManager().messageAfterOp("locks", txContext.getReqId());

    if (!isCoordinator) {
      getDistributedShared().getManager().messageBeforeOp("allocate", txContext.getReqId());
      ((OAbstractPaginatedStorage) getStorage()).preallocateRids(transaction);
      getDistributedShared().getManager().messageAfterOp("allocate", txContext.getReqId());
    }

    getDistributedShared().getManager().messageBeforeOp("indexCheck", txContext.getReqId());
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
                      "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
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
            final long version =
                ((OAbstractPaginatedStorage) getStorage())
                    .getVersionForKey(indexName, changesPerKey.key);
            int sourceVersion =
                ((OTransactionOptimisticDistributed) transaction)
                    .getVersionForKey(indexName, changesPerKey.key);
            if (version != sourceVersion) {
              throw new OInvalidSequentialException();
            }
          }
          try (final Stream<ORID> rids = index.getInternal().getRids(changesPerKey.key)) {
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
                        "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
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
    getDistributedShared().getManager().messageAfterOp("indexCheck", txContext.getReqId());

    getDistributedShared().getManager().messageBeforeOp("mvccCheck", txContext.getReqId());
    for (ORecordOperation entry : transaction.getRecordOperations()) {
      if (entry.getType() != ORecordOperation.CREATED) {
        int changeVersion = entry.getRecord().getVersion();
        ORecordMetadata metadata = getStorage().getRecordMetadata(entry.getRID());
        if (metadata == null) {
          if (((OAbstractPaginatedStorage) getStorage()).isDeleted(entry.getRID())) {
            throw new OConcurrentModificationException(
                entry.getRID(), changeVersion, changeVersion, entry.getType());
          } else {
            // don't exist i get -1, -1 rid that put the operation in queue for retry.
            throw new OConcurrentCreateException(new ORecordId(-1, -1), entry.getRID());
          }
        }
        int persistentVersion = metadata.getVersion();
        if (changeVersion != persistentVersion) {
          throw new OConcurrentModificationException(
              entry.getRID(), persistentVersion, changeVersion, entry.getType());
        }
      }
    }
    getDistributedShared().getManager().messageAfterOp("mvccCheck", txContext.getReqId());
  }

  @Override
  public OView getViewFromCluster(int cluster) {
    OImmutableSchema schema = getMetadata().getImmutableSchemaSnapshot();
    OView view = schema.getViewByClusterId(cluster);
    if (view == null) {
      String viewName =
          ((OSharedContextDistributed) getSharedContext())
              .getViewManager()
              .getViewFromOldCluster(cluster);
      if (viewName != null) {
        view = schema.getView(viewName);
      }
    }
    return view;
  }

  public OEnterpriseEndpoint getEnterpriseEndpoint() {
    OServer server = distributedManager.getServerInstance();
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

  public ODistributedConfiguration getDistributedConfiguration() {
    ODistributedDatabaseImpl local = distributedManager.getMessageService().getDatabase(getName());
    return local.getDistributedConfiguration();
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
    checkNodeIsMaster(
        getLocalNodeName(), getDistributedConfiguration(), "Command '" + command + "'");
    ODistributedDatabase local = getDistributedShared();
    // The plus 1 is for make sure it runs once even if retry is 0
    int nretry =
        getConfiguration()
                .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)
            + 1;

    retry:
    for (int i = 0; i < nretry; i++) {
      Optional<OTransactionId> beforeId = local.nextId();
      Optional<OTransactionId> afterId = local.nextId();

      OSQLCommandTaskFirstPhase task =
          new OSQLCommandTaskFirstPhase(command, beforeId.get(), afterId.get());
      ODistributedServerManager dManager = getDistributedManager();
      Set<String> nodes = dManager.getAvailableNodeNames(getName());
      long next = dManager.getNextMessageIdCounter();

      ODistributedRequestId reqId = new ODistributedRequestId(dManager.getLocalNodeId(), next);
      ODistributedTxResponseManagerImpl responseManager = sendTask(nodes, task, null, next);

      if (responseManager.isQuorumReached()) {
        Optional<OTransactionResultPayload> results =
            responseManager.getDistributedTxFinalResponse();
        assert results.isPresent();
        OTransactionResultPayload resultPayload = results.get();
        switch (resultPayload.getResponseType()) {
          case OTxSuccess.ID:
            // Success send ok
            confirmPhase2DDL(nodes, reqId, true);
            return;
          case OTxException.ID:
            // Exception send ko and throws the exception
            confirmPhase2DDL(nodes, reqId, false);
            throw ((OTxException) resultPayload).getException();
          case OTxInvalidSequential.ID:
            confirmPhase2DDL(nodes, reqId, false);
            continue retry;
        }

        for (OTransactionResultPayload result : responseManager.getAllResponses()) {
          if (result.getResponseType() == OTxException.ID) {
            OLogManager.instance()
                .warn(this, "One node on error", ((OTxException) result).getException());
          }
        }
      } else {
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
              OLogManager.instance()
                  .debug(this, "distributed exception", ((OTxException) result).getException());
              messages.add(
                  String.format(
                      "exception (node " + node + "): '%s'",
                      ((OTxException) result).getException().getMessage()));
              break;
            case OTxInvalidSequential.ID:
              confirmPhase2DDL(nodes, reqId, false);
              continue retry;
          }
        }
        confirmPhase2DDL(nodes, reqId, false);

        ODistributedOperationException ex =
            new ODistributedOperationException(
                String.format(
                    "Request `%s` didn't reach the quorum of '%d', responses: [%s]",
                    reqId, responseManager.getQuorum(), String.join(",", messages)));
        for (Exception e : exceptions) {
          ex.addSuppressed(e);
        }
        if (i == nretry) {
          throw ex;
        }
      }
    }
    throw new ODistributedOperationException("Reached number of retry to execute ddl");
  }

  private void confirmPhase2DDL(Set<String> nodes, ODistributedRequestId messageId, boolean apply) {
    ODistributedServerManager dManager = getDistributedManager();
    dManager.sendRequest(
        getName(),
        null,
        nodes,
        new OSQLCommandTaskSecondPhase(messageId, apply),
        dManager.getNextMessageIdCounter(),
        EXECUTION_MODE.RESPONSE,
        null);
  }

  private ODistributedTxResponseManagerImpl sendTask(
      Collection<String> nodes, ORemoteTask task, Object localResult, long next) {
    ODistributedServerManager dManager = getDistributedManager();
    final class HoldResponseManager {
      ODistributedTxResponseManagerImpl responseManager;
    };

    final HoldResponseManager holder = new HoldResponseManager();
    ((ODistributedPlugin) dManager)
        .sendRequest(
            getName(),
            null,
            nodes,
            task,
            next,
            EXECUTION_MODE.RESPONSE,
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
  public int addCluster(String iClusterName, Object... iParameters) {
    if (!isLocalEnv()) {
      final StringBuilder cmd = new StringBuilder("create cluster `");
      cmd.append(iClusterName);
      cmd.append("`");
      sendDDLCommand(cmd.toString(), false);
      return getClusterIdByName(iClusterName);
    } else {
      return super.addCluster(iClusterName, iParameters);
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
    return OScenarioThreadLocal.INSTANCE.isRunModeDistributed();
  }

  public void acquireDistributedExclusiveLock(int timeout) {
    distributedManager
        .getLockManagerRequester()
        .acquireExclusiveLock(getName(), distributedManager.getLocalNodeName(), timeout);
  }

  public void releaseDistributedExclusiveLock() {
    distributedManager
        .getLockManagerRequester()
        .releaseExclusiveLock(getName(), distributedManager.getLocalNodeName());
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
    final String localNode = distributedManager.getLocalNodeName();

    final ODistributedServerManager.DB_STATUS prevStatus =
        distributedManager.getDatabaseStatus(localNode, getName());
    if (prevStatus == ODistributedServerManager.DB_STATUS.ONLINE)
      // SET STATUS = BACKUP
      distributedManager.setDatabaseStatus(
          localNode, getName(), ODistributedServerManager.DB_STATUS.BACKUP);

    try {

      return super.backup(out, options, callable, iListener, compressionLevel, bufferSize);

    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Error on executing backup"), e);
    } finally {
      distributedManager.setDatabaseStatus(localNode, getName(), prevStatus);
    }
  }

  protected void checkNodeIsMaster(
      final String localNodeName, final ODistributedConfiguration dbCfg, final String operation) {
    final ODistributedConfiguration.ROLES nodeRole = dbCfg.getServerRole(localNodeName);
    if (nodeRole != ODistributedConfiguration.ROLES.MASTER)
      throw new OWriteOperationNotPermittedException(
          "Cannot execute write operation ("
              + operation
              + ") on node '"
              + localNodeName
              + "' because is non a master");
  }

  protected void handleDistributedException(
      final String iMessage, final Exception e, final Object... iParams) {
    if (e != null) {
      if (e instanceof OException) throw (OException) e;
      else if (e.getCause() instanceof OException) throw (OException) e.getCause();
      else if (e.getCause() != null && e.getCause().getCause() instanceof OException)
        throw (OException) e.getCause().getCause();
    }

    OLogManager.instance().error(this, iMessage, e, iParams);
    throw OException.wrapException(new OStorageException(String.format(iMessage, iParams)), e);
  }

  @Override
  public String getStorageId() {
    return getDistributedManager().getLocalNodeName() + "." + getName();
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
          ODistributedDatabaseImpl ddb = (ODistributedDatabaseImpl) getDistributedShared();
          ONewDistributedTxContextImpl txContext =
              new ONewDistributedTxContextImpl(
                  ddb, new ODistributedRequestId(-1, -1), tx, data.getTransactionId());
          ddb.validate(data.getTransactionId());
          ((OAbstractPaginatedStorage) getStorage().getUnderlying()).preallocateRids(tx);
          txContext.commit(this);
          return null;
        });
  }

  public OTransactionResultPayload firstPhaseDDL(
      String query,
      OTransactionId preChangeId,
      OTransactionId afterChangeId,
      ODistributedRequestId requestId) {
    ODistributedDatabase localDistributedDatabase = getDistributedShared();
    ODDLContextImpl ddlContext = new ODDLContextImpl(query, preChangeId, afterChangeId, requestId);
    ValidationResult first = localDistributedDatabase.validate(preChangeId);
    ValidationResult second = localDistributedDatabase.validate(afterChangeId);
    if ((first == ValidationResult.ALREADY_PROMISED || first == ValidationResult.MISSING_PREVIOUS)
        && (second == ValidationResult.ALREADY_PROMISED
            || second == ValidationResult.MISSING_PREVIOUS)) {
      ddlContext.setStatus(TIMEDOUT);
      return new OTxInvalidSequential();
    } else if (first == ValidationResult.ALREADY_PRESENT
        || second == ValidationResult.ALREADY_PRESENT) {
      ddlContext.setStatus(TIMEDOUT);
      return new OTxInvalidSequential();
    }
    ddlContext.setStatus(SUCCESS);
    register(requestId, localDistributedDatabase, ddlContext);
    return new OTxSuccess();
  }

  public void secondPhaseDDL(ODistributedRequestId confirmSentRequest, boolean apply) {
    ODistributedDatabase localDistributedDatabase = getDistributedShared();
    ODDLContextImpl context =
        (ODDLContextImpl) localDistributedDatabase.popTxContext(confirmSentRequest);
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) getStorage().getUnderlying();
    if (apply) {
      ((ODistributedDatabaseImpl) localDistributedDatabase).resetLastValidBackup();
      if (context.getStatus() == SUCCESS) {
        OTxMetadataHolder preMetadata = localDistributedDatabase.commit(context.getPreChangeId());

        storage.metadataOnly(preMetadata.metadata());
        preMetadata.notifyMetadataRead();
        String query = context.getQuery();
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              command(query, new Object[] {});
              return null;
            });

        OTxMetadataHolder afterMetadata =
            localDistributedDatabase.commit(context.getAfterChangeId());
        storage.metadataOnly(afterMetadata.metadata());
        afterMetadata.notifyMetadataRead();
      } else {
        int nretry = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY);
        int delay = getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY);

        for (int i = 0; i < nretry; i++) {
          try {
            if (i > 0) {
              try {
                Thread.sleep(new Random().nextInt(delay));
              } catch (InterruptedException e) {
                OException.wrapException(new OInterruptedException(e.getMessage()), e);
              }
            }
            OTransactionResultPayload firstPhase =
                firstPhaseDDL(
                    context.getQuery(),
                    context.getPreChangeId(),
                    context.getAfterChangeId(),
                    context.getReqId());
            context = (ODDLContextImpl) localDistributedDatabase.popTxContext(confirmSentRequest);
            if (firstPhase instanceof OTxSuccess) {
              break;
            }
          } catch (Exception ex) {
            OLogManager.instance()
                .warn(
                    ODatabaseDocumentDistributed.this,
                    "Error beginning timed out transaction: %s ",
                    ex,
                    context.getReqId());
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
            Orient.instance()
                .submit(
                    () -> {
                      getDistributedManager().installDatabase(false, getName(), true, true);
                    });
            throw e;
          }
        } else {
          ODistributedRequestId id = context.getReqId();
          Orient.instance()
              .submit(
                  () -> {
                    OLogManager.instance()
                        .warn(
                            ODatabaseDocumentDistributed.this,
                            "Reached limit of retry for commit tx:%s forcing database re-install",
                            id);
                    distributedManager.installDatabase(
                        false, ODatabaseDocumentDistributed.this.getName(), true, true);
                  });
        }
      }
    }
  }
}
