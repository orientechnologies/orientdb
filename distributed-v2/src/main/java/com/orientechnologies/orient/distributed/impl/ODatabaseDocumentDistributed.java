package com.orientechnologies.orient.distributed.impl;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.enterprise.OEnterpriseEndpoint;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OLowDiskSpaceException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLimitReachedException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OAutoshardedStorage;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ddl.ODDLQuerySubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OCreatedRecordResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OIndexOperationRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSequenceActionCoordinatorResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSequenceActionCoordinatorSubmit;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OTransactionResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OTransactionSubmit;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OUpdatedRecordResponse;
import com.orientechnologies.orient.distributed.impl.metadata.ODistributedContext;
import com.orientechnologies.orient.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.OServer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/** Created by tglman on 30/03/17. */
public class ODatabaseDocumentDistributed extends ODatabaseDocumentEmbedded {

  private final ONodeIdentity nodeIdentity;

  public ODatabaseDocumentDistributed(OStorage storage, ONodeIdentity nodeIdentity) {
    super(storage);
    this.nodeIdentity = nodeIdentity;
  }

  /**
   * return the name of local node in the cluster
   *
   * @return the name of local node in the cluster
   */
  public String getLocalNodeName() {
    return nodeIdentity.getName();
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
  public boolean isDistributed() {
    return true;
  }

  @Override
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentDistributed database =
        new ODatabaseDocumentDistributed(getStorage(), nodeIdentity);
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
    throw new UnsupportedOperationException("not yet implemented");
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

  @Override
  public void internalCommit(OTransactionInternal iTx) {
    if (OScenarioThreadLocal.INSTANCE.isRunModeDistributed() || iTx.isSequenceTransaction()) {
      // Exclusive for handling schema manipulation, remove after refactor for distributed schema
      super.internalCommit(iTx);
    } else {
      distributedCommitV2(iTx);
    }
  }

  @Override
  public <T> T sendSequenceAction(OSequenceAction action)
      throws ExecutionException, InterruptedException {
    OSubmitContext submitContext =
        ((OSharedContextDistributed) getSharedContext()).getDistributedContext().getSubmitContext();
    OSessionOperationId id = new OSessionOperationId();
    OSequenceActionCoordinatorSubmit submitAction = new OSequenceActionCoordinatorSubmit(action);
    Future<OSubmitResponse> future = submitContext.send(id, submitAction);
    try {
      OSequenceActionCoordinatorResponse response =
          (OSequenceActionCoordinatorResponse) future.get();
      if (!response.isSuccess()) {
        if (response.getFailedOn().size() > 0) {
          throw new ODatabaseException(
              "Sequence action failed on: " + response.getFailedOn() + " nodes");
        }
      }
      if (response.getLimitReachedOn().size() > 0) {
        if (response.getLimitReachedOn().size() == response.getNumberOfNodesInvolved()) {
          throw new OSequenceLimitReachedException(
              "Sequence limit reached on: " + response.getLimitReachedOn() + " nodes");
        } else {
          throw new ODatabaseException(
              "Inconsistent sequence limit reached on: " + response.getLimitReachedOn() + " nodes");
        }
      }

      Object a = response.getResultOfSenderNode();
      return (T) a;
    } catch (InterruptedException | ExecutionException e) {
      throw e;
    } catch (ODatabaseException exc) {
      // TODO think about rollback
      throw exc;
    }
  }

  private void distributedCommitV2(OTransactionInternal iTx) {
    OTransactionSubmit ts =
        new OTransactionSubmit(
            iTx.getRecordOperations(),
            OTransactionSubmit.genIndexes(iTx.getIndexOperations(), iTx));
    if (ts.isEmpty()) {
      return;
    }

    OSubmitContext submitContext =
        ((OSharedContextDistributed) getSharedContext()).getDistributedContext().getSubmitContext();
    OSessionOperationId id = new OSessionOperationId();
    Future<OSubmitResponse> future = submitContext.send(id, ts);
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
      OTransactionAbstract.updateCacheFromEntries(
          iTx.getDatabase(), iTx.getRecordOperations(), true);

    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void txFirstPhase(
      OSessionOperationId operationId,
      List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes) {
    OTransactionOptimisticDistributed tx =
        new OTransactionOptimisticDistributed(this, new ArrayList<>());
    tx.begin(operations, indexes);
    firstPhaseDataChecks(tx);
  }

  public OTransactionOptimisticDistributed txSecondPhase(
      OSessionOperationId operationId,
      List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes,
      boolean success) {
    // MAKE delta be used by default
    OTransactionOptimisticDistributed tx =
        new OTransactionOptimisticDistributed(this, new ArrayList<>());
    tx.begin(operations, indexes);
    try {
      if (success) {
        tx.setDatabase(this);
        ((OAbstractPaginatedStorage) this.getStorage().getUnderlying()).commitPreAllocated(tx);
        return (OTransactionOptimisticDistributed) tx;
      } else {
        // FOR NOW ROLLBACK DO NOTHING ON THE STORAGE ONLY THE CLOSE IS NEEDED
      }
    } catch (OLowDiskSpaceException ex) {
      throw ex;
    }
    return null;
  }

  private void firstPhaseDataChecks(final OTransactionInternal transaction) {
    // TODO: fix cast -> interface
    ((OAbstractPaginatedStorage) getStorage().getUnderlying()).preallocateRids(transaction);

    for (Map.Entry<String, OTransactionIndexChanges> change :
        transaction.getIndexOperations().entrySet()) {
      final String indexName = change.getKey();
      OIndex index = getSharedContext().getIndexManager().getRawIndex(indexName);
      if (OClass.INDEX_TYPE.UNIQUE.name().equals(index.getType())
          || OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name().equals(index.getType())) {
        if (!change.getValue().nullKeyChanges.entries.isEmpty()) {
          OIdentifiable old;
          try (Stream<ORID> rids = index.getInternal().getRids(null)) {
            old = rids.findFirst().orElse(null);
          }
          Object newValue =
              change
                  .getValue()
                  .nullKeyChanges
                  .entries
                  .get(change.getValue().nullKeyChanges.entries.size() - 1)
                  .value;
          if (old != null && !old.equals(newValue)) {
            throw new ORecordDuplicatedException(
                String.format(
                    "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                    newValue, null, getName(), old.getIdentity()),
                getName(),
                old.getIdentity(),
                null);
          }
        }

        for (OTransactionIndexChangesPerKey changesPerKey :
            change.getValue().changesPerKey.values()) {
          OIdentifiable old;
          // TODO: add new version check logic, API proposal
          final long version =
              ((OAbstractPaginatedStorage) getStorage().getUnderlying())
                  .getVersionForKey(indexName, changesPerKey.key);
          try (Stream<ORID> rids = index.getInternal().getRids(changesPerKey.key)) {
            old = rids.findFirst().orElse(null);
          }
          if (!changesPerKey.entries.isEmpty()) {
            Object newValue = changesPerKey.entries.get(changesPerKey.entries.size() - 1).value;
            if (old != null && !old.equals(newValue)) {
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

    for (ORecordOperation entry : transaction.getRecordOperations()) {
      if (entry.getType() != ORecordOperation.CREATED) {
        int changeVersion = entry.getRecord().getVersion();
        ORecordMetadata metadata = getStorage().getRecordMetadata(entry.getRID());
        if (metadata == null) {
          if (((OAbstractPaginatedStorage) getStorage().getUnderlying())
              .isDeleted(entry.getRID())) {
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
    OServer server = ((OrientDBDistributed) getSharedContext().getOrientDB()).getServer();
    return server.getPlugins().stream()
        .filter(OEnterpriseEndpoint.class::isInstance)
        .findFirst()
        .map(OEnterpriseEndpoint.class::cast)
        .orElse(null);
  }

  @Override
  public int addCluster(String iClusterName, Object... iParameters) {
    if (isRunLocal()) {
      final StringBuilder cmd = new StringBuilder("create cluster `");
      cmd.append(iClusterName);
      cmd.append("`");
      sendDDLCommand(cmd.toString());
      return getClusterIdByName(iClusterName);
    } else {
      return super.addCluster(iClusterName, iParameters);
    }
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId) {
    if (isRunLocal()) {
      final StringBuilder cmd = new StringBuilder("create cluster `");
      cmd.append(iClusterName);
      cmd.append("`");
      cmd.append(" ID ");
      cmd.append(iRequestedId);
      sendDDLCommand(cmd.toString());
      return iRequestedId;
    } else {
      return super.addCluster(iClusterName, iRequestedId);
    }
  }

  @Override
  protected boolean dropClusterInternal(String clusterName) {
    if (isRunLocal()) {
      final String cmd = "drop cluster `" + clusterName + "`";
      sendDDLCommand(cmd);
      return true;
    } else {
      return super.dropCluster(clusterName);
    }
  }

  @Override
  protected boolean dropClusterInternal(int clusterId) {
    if (isRunLocal()) {
      final String cmd = "drop cluster " + clusterId + "";
      sendDDLCommand(cmd);
      return true;
    } else {
      return super.dropCluster(clusterId);
    }
  }

  private boolean isDistributeVersionTwo() {
    return getConfiguration().getValueAsInteger(DISTRIBUTED_REPLICATION_PROTOCOL_VERSION) == 2;
  }

  protected boolean isRunLocal() {
    return isDistributeVersionTwo() && !isLocalEnv();
  }

  public boolean isLocalEnv() {
    return getStorage() instanceof OAutoshardedStorage
        && ((OAutoshardedStorage) getStorage()).isLocalEnv();
  }

  public void sendDDLCommand(String command) {
    ODistributedContext distributed =
        ((OSharedContextDistributed) getSharedContext()).getDistributedContext();
    Future<OSubmitResponse> response =
        distributed
            .getSubmitContext()
            .send(new OSessionOperationId(), new ODDLQuerySubmitRequest(command));
    try {
      response.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }
}
