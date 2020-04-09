package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.metadata.OClassDistributed;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.ORunQueryExecutionPlanTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.*;
import java.util.concurrent.Callable;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY;
import static com.orientechnologies.orient.server.distributed.impl.ONewDistributedTxContextImpl.Status.*;

/**
 * Created by tglman on 30/03/17.
 */
public class ODatabaseDocumentDistributed extends ODatabaseDocumentEmbedded {

  private final OHazelcastPlugin hazelcastPlugin;

  public ODatabaseDocumentDistributed(OStorage storage, OHazelcastPlugin hazelcastPlugin) {
    super(storage);
    this.hazelcastPlugin = hazelcastPlugin;
  }

  public ODistributedStorage getStorageDistributed() {
    return (ODistributedStorage) super.getStorage();
  }

  /**
   * return the name of local node in the cluster
   *
   * @return the name of local node in the cluster
   */
  public String getLocalNodeName() {
    return getStorageDistributed().getNodeId();
  }

  /**
   * returns the cluster map for current deploy. The keys of the map are node names, the values contain names of clusters (data
   * files) available on the single node.
   *
   * @return the cluster map for current deploy
   */
  public Map<String, Set<String>> getActiveClusterMap() {
    ODistributedServerManager distributedManager = getStorageDistributed().getDistributedManager();
    if (distributedManager.isOffline() || !distributedManager.isNodeOnline(distributedManager.getLocalNodeName(), getName())
        || OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      return super.getActiveClusterMap();
    }
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getStorageDistributed().getDistributedConfiguration();

    for (String server : distributedManager.getActiveServers()) {
      if (getClustersOnServer(cfg, server).contains("*")) {
        //TODO check this!!!
        result.put(server, getStorage().getClusterNames());
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
      more.addAll(getStorage().getClusterNames());
      for (String s : cfg.getClusterNames()) {
        if (!cfg.getServers(s, null).contains(s)) {
          more.remove(s);
        }
      }
      result.addAll(more);
    }
    return result;
  }

  @Override
  protected void loadMetadata() {
    metadata = new OMetadataDefault(this);
    sharedContext = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextDistributed(getStorage());
        return shared;
      }
    });
    metadata.init(sharedContext);
    sharedContext.load(this);
  }

  /**
   * returns the data center map for current deploy. The keys are data center names, the values are node names per data center
   *
   * @return data center map for current deploy
   */
  public Map<String, Set<String>> getActiveDataCenterMap() {
    Map<String, Set<String>> result = new HashMap<>();
    ODistributedConfiguration cfg = getStorageDistributed().getDistributedConfiguration();
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
    return false;
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentDistributed database = new ODatabaseDocumentDistributed(getStorage(), hazelcastPlugin);
    database.init(getConfig());
    database.internalOpen(getUser().getName(), null, false);
    database.callOnOpenListeners();
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public Map<String, Object> getHaStatus(boolean servers, boolean db, boolean latency, boolean messages) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) getStorageDistributed().getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);

    Map<String, Object> row = new HashMap<>();
    final StringBuilder output = new StringBuilder();
    if (servers)
      row.put("servers", dManager.getClusterConfiguration());
    if (db)
      row.put("database", cfg.getDocument());
    if (latency)
      row.put("latency", ODistributedOutput.formatLatency(dManager, dManager.getClusterConfiguration()));
    if (messages)
      row.put("messages", ODistributedOutput.formatMessages(dManager, dManager.getClusterConfiguration()));

    return row;
  }

  @Override
  public boolean removeHaServer(String serverName) {
    checkSecurity(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) getStorageDistributed().getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    // The last parameter (true) indicates to set the node's database status to OFFLINE.
    // If this is changed to false, the node will be set to NOT_AVAILABLE, and then the auto-repairer will
    // re-synchronize the database on the node, and then set it to ONLINE.
    return dManager.removeNodeFromConfiguration(serverName, databaseName, false, true);
  }

  @Override
  public OResultSet queryOnNode(String nodeName, OExecutionPlan executionPlan, Map<Object, Object> inputParameters) {
    ORunQueryExecutionPlanTask task = new ORunQueryExecutionPlanTask(executionPlan, inputParameters, nodeName);
    ODistributedResponse result = executeTaskOnNode(task, nodeName);
    return task.getResult(result, this);
  }

  public ODistributedResponse executeTaskOnNode(ORemoteTask task, String nodeName) {

    ODistributedServerManager dManager = getStorageDistributed().getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new ODistributedException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return dManager.sendRequest(databaseName, null, Collections.singletonList(nodeName), task, dManager.getNextMessageIdCounter(),
        ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);
  }

  @Override
  public void init(OrientDBConfig config) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      super.init(config);
      return null;
    });
  }

  protected void createMetadata() {
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextDistributed(getStorage());
        return shared;
      }
    });
    metadata.init(shared);
    ((OSharedContextDistributed) shared).create(this);
  }

  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    if (hazelcastPlugin.isEnabled()) {
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
          //Immutable Schema Class not support distributed yet.
          schemaClass = ((ODocument) record).getSchemaClass();
          if (schemaClass != null) {
            if (schemaClass.isAbstract())
              throw new OSchemaException("Document belongs to abstract class " + schemaClass.getName() + " and cannot be saved");
            rid.setClusterId(((OClassDistributed) schemaClass).getClusterForNewInstance(this, (ODocument) record));
          } else
            throw new ODatabaseException("Cannot save (4) document " + record + ": no class or cluster defined");
        } else {
          throw new ODatabaseException("Cannot save (5) document " + record + ": no class or cluster defined");
        }
      } else if (record instanceof ODocument)
        schemaClass = ((ODocument) record).getSchemaClass();
      // If the cluster id was set check is validity
      if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
        if (schemaClass != null) {
          String messageClusterName = getClusterNameById(rid.getClusterId());
          checkRecordClass(schemaClass, messageClusterName, rid);
          if (!schemaClass.hasClusterId(rid.getClusterId())) {
            throw new IllegalArgumentException(
                "Cluster name '" + messageClusterName + "' (id=" + rid.getClusterId() + ") is not configured to store the class '"
                    + schemaClass.getName() + "', valid are " + Arrays.toString(schemaClass.getClusterIds()));
          }
        }
      }
      return rid.getClusterId();
    } else {
      return super.assignAndCheckCluster(record, iClusterName);
    }
  }

  @Override
  public void internalCommit(OTransactionInternal iTx) {
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      //Exclusive for handling schema manipulation, remove after refactor for distributed schema
      super.internalCommit(iTx);
    } else {
      //This is future may handle a retry
      try {
        for (ORecordOperation txEntry : iTx.getRecordOperations()) {
          if (txEntry.type == ORecordOperation.CREATED || txEntry.type == ORecordOperation.UPDATED) {
            final ORecord record = txEntry.getRecord();
            if (record instanceof ODocument)
              ((ODocument) record).validate();
          }
        }
        final ODistributedConfiguration dbCfg = getStorageDistributed().getDistributedConfiguration();
        ODistributedServerManager dManager = getStorageDistributed().getDistributedManager();
        final String localNodeName = dManager.getLocalNodeName();
        getStorageDistributed().checkNodeIsMaster(localNodeName, dbCfg, "Transaction Commit");
        ONewDistributedTransactionManager txManager = new ONewDistributedTransactionManager(getStorageDistributed(), dManager,
            getStorageDistributed().getLocalDistributedDatabase());
        int quorum = 0;
        for (String clusterName : txManager.getInvolvedClusters(iTx.getRecordOperations())) {
          final List<String> clusterServers = dbCfg.getServers(clusterName, null);
          final int writeQuorum = dbCfg.getWriteQuorum(clusterName, clusterServers.size(), localNodeName);
          quorum = Math.max(quorum, writeQuorum);
        }
        final int availableNodes = dManager.getAvailableNodes(getName());

        if (quorum > availableNodes) {
          Set<String> online = dManager.getAvailableNodeNames(getName());
          throw new ODistributedException("No enough nodes online to execute the operation, available nodes: " + online);
        }

        txManager.commit(this, iTx, getStorageDistributed().getEventListener());
        return;
      } catch (OValidationException e) {
        throw e;
      } catch (HazelcastInstanceNotActiveException e) {
        throw new OOfflineNodeException("Hazelcast instance is not available");

      } catch (HazelcastException e) {
        throw new OOfflineNodeException("Hazelcast instance is not available");
      } catch (Exception e) {
        getStorageDistributed().handleDistributedException("Cannot route TX operation against distributed node", e);
      }
    }
  }

  public void acquireLocksForTx(OTransactionInternal tx, ODistributedTxContext txContext) {
    //Sort and lock transaction entry in distributed environment
    Set<ORID> rids = new TreeSet<>();
    for (ORecordOperation entry : tx.getRecordOperations()) {
      if (entry.getType() != ORecordOperation.CREATED) {
        rids.add(entry.getRID().copy());
      } else {
        rids.add(new ORecordId(entry.getRID().getClusterId(), -1));
      }
    }
    for (ORID rid : rids) {
      txContext.lock(rid);
    }

    //using OPair because there could be different types of values here, so falling back to lexicographic sorting
    Set<String> keys = new TreeSet<>();
    for (Map.Entry<String, OTransactionIndexChanges> change : tx.getIndexOperations().entrySet()) {
      OIndex<?> index = getMetadata().getIndexManager().getIndex(change.getKey());
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType()) || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name()
          .equals(index.getType()) || OClass.INDEX_TYPE.DICTIONARY.name().equals(index.getType())
          || OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name().equals(index.getType())) {

        String name = index.getName();
        for (OTransactionIndexChangesPerKey changesPerKey : change.getValue().changesPerKey.values()) {
          keys.add(name + "#" + changesPerKey.key);
        }
        if (!change.getValue().nullKeyChanges.entries.isEmpty()) {
          keys.add(name + "#null");
        }
      }
    }
    for (String key : keys) {
      txContext.lockIndexKey(key);
    }

  }

  public boolean beginDistributedTx(ODistributedRequestId requestId, OTransactionId id, OTransactionInternal tx, boolean local,
      int retryCount) {
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ONewDistributedTxContextImpl txContext = new ONewDistributedTxContextImpl((ODistributedDatabaseImpl) localDistributedDatabase,
        requestId, tx, id);
    try {
      internalBegin2pc(txContext, local);
      txContext.setStatus(SUCCESS);
      register(requestId, localDistributedDatabase, txContext);
    } catch (OConcurrentCreateException ex) {
      if (retryCount >= 0 && retryCount < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getExpectedRid().getClusterPosition() > ex.getActualRid().getClusterPosition()) {
          OLogManager.instance()
              .debug(this, "Allocation of rid not match, expected:%s actual:%s waiting for re-enqueue request", ex.getExpectedRid(),
                  ex.getActualRid());
          txContext.unlock();
          return false;
        }
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (OConcurrentModificationException ex) {
      if (retryCount >= 0 && retryCount < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getEnhancedRecordVersion() > ex.getEnhancedDatabaseVersion()) {
          OLogManager.instance()
              .debug(this, "Persistent version not match, record:%s expected:%s actual:%s waiting for re-enqueue request",
                  ex.getRid(), ex.getEnhancedRecordVersion(), ex.getEnhancedDatabaseVersion());
          txContext.unlock();
          return false;
        }
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (ORecordNotFoundException e) {
      // This error can happen only in deserialization before locks happen, no need to unlock
      if (retryCount >= 0 && retryCount < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        return false;
      }
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw e;
    } catch (ODistributedRecordLockedException | ODistributedKeyLockedException ex) {
      /// ?? do i've to save this state as well ?
      txContext.setStatus(TIMEDOUT);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (ORecordDuplicatedException ex) {
      txContext.setStatus(FAILED);
      register(requestId, localDistributedDatabase, txContext);
      throw ex;
    } catch (OLowDiskSpaceException ex) {
      getStorageDistributed().getDistributedManager()
          .setDatabaseStatus(getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    }
    return true;
  }

  public void register(ODistributedRequestId requestId, ODistributedDatabase localDistributedDatabase,
      ONewDistributedTxContextImpl txContext) {
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

  public boolean commit2pc(ODistributedRequestId transactionId, boolean local, ODistributedRequestId requestId) {
    getStorageDistributed().resetLastValidBackup();
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ODistributedServerManager manager = getStorageDistributed().getDistributedManager();
    ONewDistributedTxContextImpl txContext = (ONewDistributedTxContextImpl) localDistributedDatabase.getTxContext(transactionId);
    if (txContext != null) {
      if (SUCCESS.equals(txContext.getStatus())) {
        try {

          if (manager != null) {
            manager.messageCurrentPayload(requestId, txContext);
            manager.messageBeforeOp("commit", requestId);
          }
          txContext.commit(this);
          if (manager != null) {
            manager.messageAfterOp("commit", requestId);
          }
          localDistributedDatabase.popTxContext(transactionId);
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
        } finally {
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }
        return true;
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
            if (localDistributedDatabase.validate(txContext.getTransactionId()).isPresent()) {
              return false;
            }
            internalBegin2pc(txContext, local);
            txContext.setStatus(SUCCESS);
            break;

          } catch (ODistributedRecordLockedException | ODistributedKeyLockedException ex) {
            // Just retry
          } catch (Exception ex) {
            OLogManager.instance()
                .warn(ODatabaseDocumentDistributed.this, "Error beginning timed out transaction: %s ", ex, transactionId);
            break;
          }
        }
        if (!SUCCESS.equals(txContext.getStatus())) {
          txContext.destroy();
          localDistributedDatabase.popTxContext(transactionId);
          Orient.instance().submit(() -> {
            OLogManager.instance()
                .warn(ODatabaseDocumentDistributed.this, "Reached limit of retry for commit tx:%s forcing database re-install",
                    transactionId);
            manager.installDatabase(false, ODatabaseDocumentDistributed.this.getName(), true, true);
          });
          return true;
        }
        try {
          txContext.commit(this);
          localDistributedDatabase.popTxContext(transactionId);
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
          return true;
        } finally {
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }
      }
    }
    return false;
  }

  public boolean rollback2pc(ODistributedRequestId transactionId) {
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ODistributedTxContext txContext = localDistributedDatabase.popTxContext(transactionId);
    if (txContext != null) {
      localDistributedDatabase.rollback(txContext.getTransactionId());
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
      ((OTransactionInternal) tx).setDatabase(this);
      ((OAbstractPaginatedStorage) this.getStorage().getUnderlying()).commitPreAllocated(tx);
    } catch (OLowDiskSpaceException ex) {
      getStorageDistributed().getDistributedManager()
          .setDatabaseStatus(getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } finally {
      txContext.destroy();
    }

  }

  public void internalBegin2pc(ONewDistributedTxContextImpl txContext, boolean local) {
    getStorageDistributed().resetLastValidBackup();
    OTransactionInternal transaction = txContext.getTransaction();
    //This is moved before checks because also the coordinator first node allocate before checks
    if (!local) {
      ((OTransactionOptimisticDistributed) transaction).setDatabase(this);
      ((OTransactionOptimistic) transaction).begin();
    }

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageBeforeOp("locks", txContext.getReqId());

    acquireLocksForTx(transaction, txContext);

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageAfterOp("locks", txContext.getReqId());

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageBeforeOp("allocate", txContext.getReqId());
    ((OAbstractPaginatedStorage) getStorage().getUnderlying()).preallocateRids(transaction);
    getStorageDistributed().getLocalDistributedDatabase().getManager().messageAfterOp("allocate", txContext.getReqId());

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageBeforeOp("indexCheck", txContext.getReqId());
    for (Map.Entry<String, OTransactionIndexChanges> change : transaction.getIndexOperations().entrySet()) {
      OIndex<?> index = getSharedContext().getIndexManager().getRawIndex(change.getKey());
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType()) || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name()
          .equals(index.getType())) {
        OTransactionIndexChangesPerKey nullKeyChanges = change.getValue().nullKeyChanges;
        if (!nullKeyChanges.entries.isEmpty()) {
          OIdentifiable old = (OIdentifiable) index.get(null);
          Object newValue = nullKeyChanges.entries.get(nullKeyChanges.entries.size() - 1).value;
          if (old != null && !old.equals(newValue)) {
            boolean oldValueRemoved = false;
            for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : nullKeyChanges.entries) {
              if (entry.value != null && entry.value.equals(old) && entry.operation == OTransactionIndexChanges.OPERATION.REMOVE) {
                oldValueRemoved = true;
                break;
              }
            }
            if (!oldValueRemoved) {
              throw new ORecordDuplicatedException(String
                  .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                      newValue, null, getName(), old.getIdentity()), getName(), old.getIdentity(), null);
            }
          }
        }

        for (OTransactionIndexChangesPerKey changesPerKey : change.getValue().changesPerKey.values()) {
          OIdentifiable old = (OIdentifiable) index.get(changesPerKey.key);
          if (!changesPerKey.entries.isEmpty()) {
            Object newValue = changesPerKey.entries.get(changesPerKey.entries.size() - 1).value;
            if (old != null && !old.equals(newValue)) {
              boolean oldValueRemoved = false;
              for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : changesPerKey.entries) {
                if (entry.value != null && entry.value.equals(old)
                    && entry.operation == OTransactionIndexChanges.OPERATION.REMOVE) {
                  oldValueRemoved = true;
                  break;
                }
              }
              if (!oldValueRemoved) {
                throw new ORecordDuplicatedException(String
                    .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                        newValue, changesPerKey.key, getName(), old.getIdentity()), getName(), old.getIdentity(),
                    changesPerKey.key);
              }
            }
          }
        }
      }
    }

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageAfterOp("indexCheck", txContext.getReqId());

    getStorageDistributed().getLocalDistributedDatabase().getManager().messageBeforeOp("mvccCheck", txContext.getReqId());
    for (ORecordOperation entry : transaction.getRecordOperations()) {
      if (entry.getType() != ORecordOperation.CREATED) {
        int changeVersion = entry.getRecord().getVersion();
        ORecordMetadata metadata = getStorage().getRecordMetadata(entry.getRID());
        if (metadata == null) {
          if (((OAbstractPaginatedStorage) getStorage().getUnderlying()).isDeleted(entry.getRID())) {
            throw new OConcurrentModificationException(entry.getRID(), changeVersion, changeVersion, entry.getType());
          } else {
            //don't exist i get -1, -1 rid that put the operation in queue for retry.
            throw new OConcurrentCreateException(new ORecordId(-1, -1), entry.getRID());
          }
        }
        int persistentVersion = metadata.getVersion();
        if (changeVersion != persistentVersion) {
          throw new OConcurrentModificationException(entry.getRID(), persistentVersion, changeVersion, entry.getType());
        }
      }
    }
    getStorageDistributed().getLocalDistributedDatabase().getManager().messageAfterOp("mvccCheck", txContext.getReqId());

  }

  @Override
  public OEnterpriseEndpoint getEnterpriseEndpoint() {
    OServer server = ((ODistributedStorage) getStorage()).getDistributedManager().getServerInstance();
    return server.getPlugins().stream().map(x -> x.getInstance()).filter(OEnterpriseEndpoint.class::isInstance).findFirst()
        .map(OEnterpriseEndpoint.class::cast).orElse(null);
  }

}
