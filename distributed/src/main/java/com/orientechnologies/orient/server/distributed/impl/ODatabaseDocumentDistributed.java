package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OOfflineNodeException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.OCommit37Response;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.tx.*;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.*;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.metadata.OTransactionContext;
import com.orientechnologies.orient.server.distributed.impl.task.OCopyDatabaseChunkTask;
import com.orientechnologies.orient.server.distributed.impl.task.ORunQueryExecutionPlanTask;
import com.orientechnologies.orient.server.distributed.impl.task.OSyncClusterTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedLockException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
    loadMetadata(this.getSharedContext());
  }

  @Override
  protected void loadMetadata(OSharedContext ctx) {
    metadata = new OMetadataDefault(this);
    sharedContext = ctx;
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
    final OStorage stg = getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("SYNC DATABASE command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    return dManager.installDatabase(true, databaseName, forceDeployment, tryWithDelta);
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
  public Map<String, Object> syncCluster(String clusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, "sync", ORole.PERMISSION_UPDATE);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) getStorageDistributed().getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    OServer serverInstance = getStorageDistributed().getServer();
    final String dbPath = serverInstance.getDatabaseDirectory() + databaseName;

    final String nodeName = dManager.getLocalNodeName();

    final List<String> nodesWhereClusterIsCfg = cfg.getServers(clusterName, null);
    nodesWhereClusterIsCfg.remove(nodeName);

    if (nodesWhereClusterIsCfg.isEmpty())
      throw new OCommandExecutionException(
          "Cannot synchronize cluster '" + clusterName + "' because is not configured on any running nodes");

    final OSyncClusterTask task = new OSyncClusterTask(clusterName);
    final ODistributedResponse response = dManager
        .sendRequest(databaseName, null, nodesWhereClusterIsCfg, task, dManager.getNextMessageIdCounter(),
            ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

    final Map<String, Object> results = (Map<String, Object>) response.getPayload();

    File tempFile = null;
    FileOutputStream out = null;
    try {
      tempFile = new File(Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + "_toInstall.zip");
      if (tempFile.exists())
        tempFile.delete();
      else
        tempFile.getParentFile().mkdirs();
      tempFile.createNewFile();

      long fileSize = 0;
      out = new FileOutputStream(tempFile, false);
      for (Map.Entry<String, Object> r : results.entrySet()) {
        final Object value = r.getValue();

        if (value instanceof Boolean) {
          continue;
        } else if (value instanceof Throwable) {
          ODistributedServerLog
              .error(null, nodeName, r.getKey(), ODistributedServerLog.DIRECTION.IN, "error on installing cluster %s in %s",
                  (Exception) value, databaseName, dbPath);
        } else if (value instanceof ODistributedDatabaseChunk) {
          ODistributedDatabaseChunk chunk = (ODistributedDatabaseChunk) value;

          // DELETE ANY PREVIOUS .COMPLETED FILE
          final File completedFile = new File(tempFile.getAbsolutePath() + ".completed");
          if (completedFile.exists())
            completedFile.delete();

          fileSize = writeDatabaseChunk(nodeName, 1, chunk, out);
          for (int chunkNum = 2; !chunk.last; chunkNum++) {
            final Object result = dManager.sendRequest(databaseName, null, OMultiValue.getSingletonList(r.getKey()),
                new OCopyDatabaseChunkTask(chunk.filePath, chunkNum, chunk.offset + chunk.buffer.length, false),
                dManager.getNextMessageIdCounter(), ODistributedRequest.EXECUTION_MODE.RESPONSE, null, null, null);

            if (result instanceof Boolean)
              continue;
            else if (result instanceof Exception) {
              ODistributedServerLog.error(null, nodeName, r.getKey(), ODistributedServerLog.DIRECTION.IN,
                  "error on installing database %s in %s (chunk #%d)", (Exception) result, databaseName, dbPath, chunkNum);
            } else if (result instanceof ODistributedDatabaseChunk) {
              chunk = (ODistributedDatabaseChunk) result;

              fileSize += writeDatabaseChunk(nodeName, chunkNum, chunk, out);
            }
          }

          out.flush();

          // CREATE THE .COMPLETED FILE TO SIGNAL EOF
          new File(tempFile.getAbsolutePath() + ".completed").createNewFile();
        }
      }

      final String tempDirectoryPath = Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + "_toInstall";
      final File tempDirectory = new File(tempDirectoryPath);
      tempDirectory.mkdirs();

      OZIPCompressionUtil.uncompressDirectory(new FileInputStream(tempFile), tempDirectory.getAbsolutePath(), null);

      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      final boolean openDatabaseHere = db == null;
      if (db == null)
        db = serverInstance.openDatabase("plocal:" + dbPath, "", "", null, true);

      try {

        final OAbstractPaginatedStorage stg = (OAbstractPaginatedStorage) db.getStorage().getUnderlying();

        // TODO: FREEZE COULD IT NOT NEEDED
        stg.freeze(false);
        try {

          final OPaginatedCluster cluster = (OPaginatedCluster) stg.getClusterByName(clusterName);

          final File tempClusterFile = new File(tempDirectoryPath + "/" + clusterName + OPaginatedCluster.DEF_EXTENSION);

          cluster.replaceFile(tempClusterFile);

        } finally {
          stg.release();
        }

        db.getLocalCache().invalidate();

      } finally {
        if (openDatabaseHere)
          db.close();
      }

      Map<String, Object> result = new HashMap<>();
      result.put("fileSize", fileSize);
      result.put("message", "Cluster correctly replaced");
      result.put("result", true);
      return result;

    } catch (Exception e) {
      ODistributedServerLog
          .error(null, nodeName, null, ODistributedServerLog.DIRECTION.NONE, "error on transferring database '%s' to '%s'", e,
              databaseName, tempFile);
      throw OException.wrapException(new ODistributedException("Error on transferring database"), e);
    } finally {
      try {
        if (out != null) {
          out.flush();
          out.close();
        }
      } catch (IOException e) {
      }
    }

  }

  protected static long writeDatabaseChunk(final String iNodeName, final int iChunkId, final ODistributedDatabaseChunk chunk,
      final FileOutputStream out) throws IOException {

    ODistributedServerLog
        .warn(null, iNodeName, null, ODistributedServerLog.DIRECTION.NONE, "- writing chunk #%d offset=%d size=%s", iChunkId,
            chunk.offset, OFileUtils.getSizeAsString(chunk.buffer.length));
    out.write(chunk.buffer);

    return chunk.buffer.length;
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
  public void init(OrientDBConfig config, OSharedContext sharedContext) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
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
        //Immutable Schema Class not support distributed yet.
        schemaClass = ((ODocument) record).getSchemaClass();
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException("Document belongs to abstract class " + schemaClass.getName() + " and cannot be saved");
          rid.setClusterId(schemaClass.getClusterForNewInstance((ODocument) record));
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
  }

  @Override
  public void internalCommit(OTransactionInternal iTx) {
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed()) {
      //Exclusive for handling schema manipulation, remove after refactor for distributed schema
      super.internalCommit(iTx);
    } else {
      //realCommit(iTx);

      ODistributedServerManager dManager = getStorageDistributed().getDistributedManager();
      try {
        dManager.waitUntilNodeOnline();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      ODistributedDatabaseImpl sharedDistributeDb = (ODistributedDatabaseImpl) dManager.getMessageService().getDatabase(getName());
      OSubmitContext submitContext = ((OSharedContextDistributed) getSharedContext()).getDistributedContext().getSubmitContext();
      sharedDistributeDb.waitForOnline();
      OSessionOperationId id = new OSessionOperationId();
      id.init();
      Future<OSubmitResponse> future = submitContext.send(id,
          new OTransactionSubmit(iTx.getRecordOperations(), OTransactionSubmit.genIndexes(iTx.getIndexOperations(), iTx)));
      try {
        OTransactionResponse response = (OTransactionResponse) future.get();
        if (!response.isSuccess()) {
          throw new ODatabaseException("failed");
        }
        for (OCreatedRecordResponse created : response.getCreatedRecords()) {
          iTx.updateIdentityAfterCommit(created.getCurrentRid(), created.getCreatedRid());
          ORecordOperation rop = iTx.getRecordEntry(created.getCurrentRid());
          if (rop != null) {
            if (created.getVersion() > rop.getRecord().getVersion() + 1)
              // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
              rop.getRecord().unload();
            ORecordInternal.setVersion(rop.getRecord(), created.getVersion());
          }
        }
        for (OUpdatedRecordResponse updated : response.getUpdatedRecords()) {
          ORecordOperation rop = iTx.getRecordEntry(updated.getRid());
          if (rop != null) {
            if (updated.getVersion() > rop.getRecord().getVersion() + 1)
              // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
              rop.getRecord().unload();
            ORecordInternal.setVersion(rop.getRecord(), updated.getVersion());
          }
        }
        // SET ALL THE RECORDS AS UNDIRTY
        for (ORecordOperation txEntry : iTx.getRecordOperations())
          ORecordInternal.unsetDirty(txEntry.getRecord());

        // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT.
        OTransactionAbstract.updateCacheFromEntries(iTx.getDatabase(), iTx.getRecordOperations(), true);

      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }

    }
  }

  public void realCommit(OTransactionInternal iTx) {
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
      Set<String> otherNodesInQuorum = txManager
          .getAvailableNodesButLocal(dbCfg, txManager.getInvolvedClusters(iTx.getRecordOperations()), getLocalNodeName());
      List<String> online = dManager.getOnlineNodes(getName());
      if (online.size() < ((otherNodesInQuorum.size() + 1) / 2) + 1) {
        throw new ODistributedException("No enough nodes online to execute the operation, online nodes: " + online);
      }

      ((OAbstractPaginatedStorage) getStorage().getUnderlying()).preallocateRids(iTx);

      txManager.commit(this, iTx);
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

  public void acquireLocksForTx(OTransactionInternal tx, ODistributedTxContext txContext) {
    //Sort and lock transaction entry in distributed environment
    Set<ORID> rids = new TreeSet<>();
    for (ORecordOperation entry : tx.getRecordOperations()) {
      rids.add(entry.getRID());
    }
    for (ORID rid : rids) {
      txContext.lock(rid);
    }
    //using OPair because there could be different types of values here, so falling back to lexicographic sorting
    Set<OPair<String, Object>> keys = new TreeSet<>();
    for (Map.Entry<String, OTransactionIndexChanges> change : tx.getIndexOperations().entrySet()) {
      OIndex<?> index = getMetadata().getIndexManager().getIndex(change.getKey());
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType()) || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name()
          .equals(index.getType()) || OClass.INDEX_TYPE.DICTIONARY.name().equals(index.getType())
          || OClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name().equals(index.getType())) {
        for (OTransactionIndexChangesPerKey changesPerKey : change.getValue().changesPerKey.values()) {
          keys.add(new OPair<>(String.valueOf(changesPerKey.key), changesPerKey.key));
        }
        if (!change.getValue().nullKeyChanges.entries.isEmpty()) {
          keys.add(new OPair<>("null", null));
        }
      }
    }
    for (OPair key : keys) {
      txContext.lockIndexKey(key.getValue());
    }

  }

  public void txFirstPhase(OSessionOperationId requestId, OTransactionInternal tx) {
  }

  public void txFirstPhase(OSessionOperationId operationId, List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes) {
    OTransactionOptimisticDistributed tx = new OTransactionOptimisticDistributed(this, new ArrayList<>());
    OSharedContextDistributed sharedContext = (OSharedContextDistributed) getSharedContext();
    sharedContext.getDistributedContext().registerTransaction(operationId, tx);
    tx.begin(operations, indexes);
    firstPhaseDataChecks(false, tx);
  }

  public OTransactionOptimisticDistributed txSecondPhase(OSessionOperationId operationId, boolean success) {
    OSharedContextDistributed sharedContext = (OSharedContextDistributed) getSharedContext();
    OTransactionContext context = sharedContext.getDistributedContext().getTransaction(operationId);
    try {
      if (success) {
        OTransactionInternal tx = context.getTransaction();
        tx.setDatabase(this);
        ((OAbstractPaginatedStorage) this.getStorage().getUnderlying()).commitPreAllocated(tx);
        return (OTransactionOptimisticDistributed) tx;
      } else {
        //FOR NOW ROLLBACK DO NOTHING ON THE STORAGE ONLY THE CLOSE IS NEEDED
      }
    } catch (OLowDiskSpaceException ex) {
      getStorageDistributed().getDistributedManager()
          .setDatabaseStatus(getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    } finally {
      sharedContext.getDistributedContext().closeTransaction(operationId);
    }
    return null;
  }

  public boolean beginDistributedTx(ODistributedRequestId requestId, OTransactionInternal tx, boolean local, int retryCount) {
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ONewDistributedTxContextImpl txContext = new ONewDistributedTxContextImpl((ODistributedDatabaseImpl) localDistributedDatabase,
        requestId, tx);
    try {
      internalBegin2pc(txContext, local);
      txContext.setStatus(SUCCESS);
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
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
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
      throw ex;
    } catch (OConcurrentModificationException ex) {
      if (retryCount >= 0 && retryCount < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        if (ex.getEnhancedRecordVersion() > ex.getEnhancedDatabaseVersion()) {
          OLogManager.instance()
              .info(this, "Persistent version not match, record:%s expected:%s actual:%s waiting for re-enqueue request",
                  ex.getRid(), ex.getEnhancedRecordVersion(), ex.getEnhancedDatabaseVersion());
          txContext.unlock();
          return false;
        }
      }
      txContext.setStatus(FAILED);
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
      throw ex;
    } catch (ORecordNotFoundException e) {
      // This error can happen only in deserialization before locks happen, no need to unlock
      if (retryCount >= 0 && retryCount < getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
        return false;
      }
      txContext.setStatus(FAILED);
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
      throw e;
    } catch (ODistributedLockException | OLockException ex) {
      txContext.unlock();
      /// ?? do i've to save this state as well ?
      txContext.setStatus(TIMEDOUT);
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
      throw ex;
    } catch (ORecordDuplicatedException ex) {
      txContext.setStatus(FAILED);
      getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
      throw ex;
    } catch (OLowDiskSpaceException ex) {
      getStorageDistributed().getDistributedManager()
          .setDatabaseStatus(getLocalNodeName(), getName(), ODistributedServerManager.DB_STATUS.OFFLINE);
      throw ex;
    }
    getStorageDistributed().getLocalDistributedDatabase().registerTxContext(requestId, txContext);
    return true;
  }

  /**
   * The Local commit is different from a remote commit due to local rid pre-allocation
   *
   * @param transactionId
   */
  public void commit2pcLocal(ODistributedRequestId transactionId) {
    commit2pc(transactionId);
  }

  public boolean commit2pc(ODistributedRequestId transactionId) {
    getStorageDistributed().resetLastValidBackup();
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ODistributedServerManager manager = getStorageDistributed().getDistributedManager();
    ONewDistributedTxContextImpl txContext = (ONewDistributedTxContextImpl) localDistributedDatabase.getTxContext(transactionId);
    if (txContext != null) {
      if (SUCCESS.equals(txContext.getStatus())) {
        try {
          txContext.commit(this);
          localDistributedDatabase.popTxContext(transactionId);
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
        } finally {
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }
        return true;
      } else if (TIMEDOUT.equals(txContext.getStatus())) {
        for (int i = 0; i < 10; i++) {
          try {
            internalBegin2pc(txContext, false);
            txContext.setStatus(SUCCESS);
            break;
          } catch (Exception ex) {
            OLogManager.instance()
                .warn(ODatabaseDocumentDistributed.this, "Error beginning timed out transaction: %s", ex, transactionId);
          }
        }
        if (!SUCCESS.equals(txContext.getStatus())) {
          Orient.instance().submit(() -> {
            OLogManager.instance()
                .warn(ODatabaseDocumentDistributed.this, "Reached limit of retry for commit tx:%s forcing database re-install",
                    transactionId);
            manager.installDatabase(false, ODatabaseDocumentDistributed.this.getName(), true, true);
          });
        }
        try {
          txContext.commit(this);
          localDistributedDatabase.popTxContext(transactionId);
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
        } finally {
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
        }

      } else {
        Orient.instance().submit(() -> {
          OLogManager.instance()
              .warn(ODatabaseDocumentDistributed.this, "Reached limit of retry for commit tx:%s forcing database re-install",
                  transactionId);
          manager.installDatabase(false, ODatabaseDocumentDistributed.this.getName(), true, true);
        });
      }
    }
    return false;
  }

  public boolean rollback2pc(ODistributedRequestId transactionId) {
    ODistributedDatabase localDistributedDatabase = getStorageDistributed().getLocalDistributedDatabase();
    ODistributedTxContext txContext = localDistributedDatabase.popTxContext(transactionId);
    if (txContext != null) {
      synchronized (txContext) {
        txContext.destroy();
      }
      OLiveQueryHook.removePendingDatabaseOps(this);
      OLiveQueryHookV2.removePendingDatabaseOps(this);
      return true;
    }
    return false;
  }

  public void internalCommit2pc(ONewDistributedTxContextImpl txContext) {
    try {
      OTransactionInternal tx = txContext.getTransaction();
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
      ((OTransactionOptimistic) transaction).begin();
    }

    acquireLocksForTx(transaction, txContext);

    firstPhaseDataChecks(local, transaction);

  }

  private void firstPhaseDataChecks(boolean local, OTransactionInternal transaction) {
    if (!local) {
      ((OAbstractPaginatedStorage) getStorage().getUnderlying()).preallocateRids(transaction);
    }

    for (Map.Entry<String, OTransactionIndexChanges> change : transaction.getIndexOperations().entrySet()) {
      OIndex<?> index = getSharedContext().getIndexManager().getRawIndex(change.getKey());
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType()) || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name()
          .equals(index.getType())) {
        if (!change.getValue().nullKeyChanges.entries.isEmpty()) {
          OIdentifiable old = (OIdentifiable) index.get(null);
          Object newValue = change.getValue().nullKeyChanges.entries.get(change.getValue().nullKeyChanges.entries.size() - 1).value;
          if (old != null && !old.equals(newValue)) {
            throw new ORecordDuplicatedException(String
                .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                    newValue, null, getName(), old.getIdentity()), getName(), old.getIdentity(), null);
          }
        }

        for (OTransactionIndexChangesPerKey changesPerKey : change.getValue().changesPerKey.values()) {
          OIdentifiable old = (OIdentifiable) index.get(changesPerKey.key);
          if (!changesPerKey.entries.isEmpty()) {
            Object newValue = changesPerKey.entries.get(changesPerKey.entries.size() - 1).value;
            if (old != null && !old.equals(newValue)) {
              throw new ORecordDuplicatedException(String
                  .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                      newValue, changesPerKey.key, getName(), old.getIdentity()), getName(), old.getIdentity(), changesPerKey.key);
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
  }

  public void afterCreateOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterCreate(doc, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().createdFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          getMetadata().getSecurity().incrementVersion();
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceCreated(this, doc);
        }
        if (clazz.isScheduler()) {
          getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterCreate(doc, this);
        }
      }
    }
    callbackHooks(ORecordHook.TYPE.AFTER_CREATE, id);
  }

  public void afterUpdateOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterUpdate((ODocument) id, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().updatedFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          getMetadata().getSecurity().incrementVersion();
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceUpdated(this, doc);
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterUpdate(doc, this);
        }
      }
    }
    callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, id);
  }

  public void afterDeleteOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterDelete(doc, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().droppedFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          getMetadata().getSecurity().incrementVersion();
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceDropped(this, doc);
        }
        if (clazz.isScheduler()) {
          final String eventName = doc.field(OScheduledEvent.PROP_NAME);
          getSharedContext().getScheduler().removeEventInternal(eventName);
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterDelete(doc, this);
        }
      }
    }
    callbackHooks(ORecordHook.TYPE.AFTER_DELETE, id);
  }

  @Override
  public OView getViewFromCluster(int cluster) {
    OImmutableSchema schema = getMetadata().getImmutableSchemaSnapshot();
    OView view = schema.getViewByClusterId(cluster);
    if (view == null) {
      String viewName = ((OSharedContextDistributed) getSharedContext()).getViewManager().getViewFromOldCluster(cluster);
      if (viewName != null) {
        view = schema.getView(viewName);
      }
    }
    return view;
  }

}
