package com.orientechnologies.orient.server.distributed.impl.task;

import static com.orientechnologies.orient.server.distributed.impl.TxContextStatus.TIMEDOUT;

import com.orientechnologies.orient.client.remote.message.OMessageHelper;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDeltaDistributed;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.OInvalidSequentialException;
import com.orientechnologies.orient.server.distributed.impl.ONewDistributedTxContextImpl;
import com.orientechnologies.orient.server.distributed.impl.OTransactionOptimisticDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentCreation;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxInvalidSequential;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxKeyLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxRecordLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxStillRunning;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxUniqueIndex;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimerTask;
import java.util.TreeSet;

/** @author luigi dell'aquila (l.dellaquila - at - orientdb.com) */
public class OTransactionPhase1Task extends OAbstractRemoteTask implements OLockKeySource {
  public static final int FACTORYID = 43;
  private volatile boolean hasResponse;
  private List<ORecordOperation> ops;
  private List<ORecordOperationRequest> operations;
  // Contains the set of <index-name, index-key> for keys (belonging to a unique index) that are
  // changed with this tx.
  // If a null key is allowed by the index, index-key could be null.
  private SortedSet<OTransactionUniqueKey> uniqueIndexKeys;
  private transient int retryCount = 0;
  private volatile boolean finished;
  private TimerTask notYetFinishedTask;
  private OTransactionId transactionId;

  public OTransactionPhase1Task() {
    ops = new ArrayList<>();
    operations = new ArrayList<>();
    uniqueIndexKeys = new TreeSet<>();
  }

  public OTransactionPhase1Task(
      List<ORecordOperation> ops,
      OTransactionId transactionId,
      SortedSet<OTransactionUniqueKey> uniqueIndexKeys) {
    this.ops = ops;
    operations = new ArrayList<>();
    this.uniqueIndexKeys = uniqueIndexKeys;
    this.transactionId = transactionId;
    genOps(ops);
  }

  public void genOps(List<ORecordOperation> ops) {
    for (ORecordOperation txEntry : ops) {
      if (txEntry.type == ORecordOperation.LOADED) continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
        case ORecordOperation.CREATED:
          request.setRecord(
              ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
          break;
        case ORecordOperation.UPDATED:
          if (request.getRecordType() == ODocument.RECORD_TYPE) {
            request.setRecord(
                ODocumentSerializerDeltaDistributed.instance()
                    .serializeDelta((ODocument) txEntry.getRecord()));
          } else {
            request.setRecord(
                ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
          }
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
          break;
        case ORecordOperation.DELETED:
          break;
      }
      operations.add(request);
    }
  }

  @Override
  public String getName() {
    return "TxPhase1";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {

    if (iManager != null) {
      iManager.messageBeforeOp("prepare1Phase", requestId);
    }
    convert(database);
    if (iManager != null) {
      iManager.messageAfterOp("prepare1Phase", requestId);
    }

    OTransactionOptimisticDistributed tx =
        new OTransactionOptimisticDistributed(database, ops, uniqueIndexKeys);
    // No need to increase the lock timeout here with the retry because this retries are not
    // deadlock retries
    OTransactionResultPayload res1;
    try {
      res1 =
          executeTransaction(
              requestId,
              transactionId,
              (ODatabaseDocumentDistributed) database,
              tx,
              false,
              retryCount);
    } catch (Exception e) {
      this.finished = true;
      if (this.notYetFinishedTask != null) {
        this.notYetFinishedTask.cancel();
      }
      throw e;
    }
    if (res1 == null) {
      final int autoRetryDelay =
          OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();
      retryCount++;
      ((ODatabaseDocumentDistributed) database)
          .getDistributedShared()
          .reEnqueue(
              requestId.getNodeId(),
              requestId.getMessageId(),
              database.getName(),
              this,
              retryCount,
              autoRetryDelay);
      hasResponse = false;
      return null;
    }

    hasResponse = true;
    this.finished = true;
    if (this.notYetFinishedTask != null) {
      this.notYetFinishedTask.cancel();
    }
    return new OTransactionPhase1TaskResult(res1);
  }

  @Override
  public boolean hasResponse() {
    return hasResponse;
  }

  public static OTransactionResultPayload executeTransaction(
      ODistributedRequestId requestId,
      OTransactionId id,
      ODatabaseDocumentDistributed database,
      OTransactionInternal tx,
      boolean isCoordinator,
      int retryCount) {
    OTransactionResultPayload payload;
    try {
      if (!isCoordinator) {
        ODistributedDatabase localDistributedDatabase = database.getDistributedShared();
        ValidationResult result = localDistributedDatabase.validate(id);
        if (result == ValidationResult.ALREADY_PROMISED
            || result == ValidationResult.MISSING_PREVIOUS) {
          ONewDistributedTxContextImpl txContext =
              new ONewDistributedTxContextImpl(
                  (ODistributedDatabaseImpl) localDistributedDatabase, requestId, tx, id);
          txContext.setStatus(TIMEDOUT);
          database.register(requestId, localDistributedDatabase, txContext);
          return new OTxInvalidSequential();
        } else if (result == ValidationResult.ALREADY_PRESENT) {
          ONewDistributedTxContextImpl txContext =
              new ONewDistributedTxContextImpl(
                  (ODistributedDatabaseImpl) localDistributedDatabase, requestId, tx, id);
          txContext.setStatus(TIMEDOUT);
          database.register(requestId, localDistributedDatabase, txContext);
          // This send OK to the sender even if already present, the second phase will skip the
          // apply if already present
          return new OTxInvalidSequential();
        }
      }
      if (database.beginDistributedTx(requestId, id, tx, isCoordinator, retryCount)) {
        payload = new OTxSuccess();
      } else {
        return null;
      }
    } catch (OConcurrentModificationException ex) {
      payload =
          new OTxConcurrentModification((ORecordId) ex.getRid(), ex.getEnhancedDatabaseVersion());
    } catch (ODistributedRecordLockedException ex) {
      payload = new OTxRecordLockTimeout(ex.getNode(), ex.getRid());
    } catch (ODistributedKeyLockedException ex) {
      payload = new OTxKeyLockTimeout(ex.getNode(), ex.getKey());
    } catch (ORecordDuplicatedException ex) {
      payload = new OTxUniqueIndex((ORecordId) ex.getRid(), ex.getIndexName(), ex.getKey());
    } catch (OConcurrentCreateException ex) {
      payload = new OTxConcurrentCreation(ex.getActualRid(), ex.getExpectedRid());
    } catch (OInvalidSequentialException ex) {
      payload = new OTxInvalidSequential();
    } catch (RuntimeException ex) {
      payload = new OTxException(ex);
    }
    return payload;
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    this.transactionId = OTransactionId.read(in);
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      ORecordOperationRequest req = OMessageHelper.readTransactionEntry(in);
      operations.add(req);
    }

    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    readTxUniqueIndexKeys(uniqueIndexKeys, serializer, in);
  }

  public static void readTxUniqueIndexKeys(
      SortedSet<OTransactionUniqueKey> uniqueIndexKeys,
      ORecordSerializerNetworkV37 serializer,
      DataInput in)
      throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      OTransactionUniqueKey entry = OTransactionUniqueKey.read(in, serializer);
      uniqueIndexKeys.add(entry);
    }
  }

  private void convert(ODatabaseDocumentInternal database) {
    for (ORecordOperationRequest req : operations) {
      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
        case ORecordOperation.CREATED:
          {
            record = ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
            ORecordInternal.setRecordSerializer(record, database.getSerializer());
            break;
          }
        case ORecordOperation.UPDATED:
          {
            if (req.getRecordType() == ODocument.RECORD_TYPE) {
              record = database.load(req.getId());
              if (record == null) {
                record = new ODocument();
              }
              ((ODocument) record).deserializeFields();
              ODocumentInternal.clearTransactionTrackData((ODocument) record);
              ODocumentSerializerDeltaDistributed.instance()
                  .deserializeDelta(req.getRecord(), (ODocument) record);
              /// Got record with empty deltas, at this level we mark the record dirty anyway.
              if (!req.isContentChanged()) {
                record.setDirtyNoChanged();
              } else {
                record.setDirty();
              }
            } else {
              record =
                  ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
              ORecordInternal.setRecordSerializer(record, database.getSerializer());
            }
            break;
          }
        case ORecordOperation.DELETED:
          {
            record = database.load(req.getId());
            if (record == null) {
              record =
                  Orient.instance()
                      .getRecordFactoryManager()
                      .newInstance(req.getRecordType(), req.getId().getClusterId(), database);
            }
            break;
          }
      }
      ORecordInternal.setIdentity(record, (ORecordId) req.getId());
      ORecordInternal.setVersion(record, req.getVersion());
      ORecordOperation op = new ORecordOperation(record, type);
      ops.add(op);
    }
    operations.clear();
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    transactionId.write(out);
    out.writeInt(operations.size());

    for (ORecordOperationRequest operation : operations) {
      OMessageHelper.writeTransactionEntry(out, operation);
    }

    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    writeTxUniqueIndexKeys(uniqueIndexKeys, serializer, out);
  }

  public static void writeTxUniqueIndexKeys(
      SortedSet<OTransactionUniqueKey> uniqueIndexKeys,
      ORecordSerializerNetworkV37 serializer,
      DataOutput out)
      throws IOException {
    out.writeInt(uniqueIndexKeys.size());
    for (OTransactionUniqueKey pair : uniqueIndexKeys) {
      pair.write(serializer, out);
    }
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public void init(final OTransactionId transactionId, final OTransactionInternal tx) {
    this.transactionId = transactionId;
    extractUniqueIndexOps(tx);
    this.ops = new ArrayList<>(tx.getRecordOperations());
    genOps(this.ops);
  }

  private void extractUniqueIndexOps(final OTransactionInternal tx) {
    if (tx.getIndexOperations().isEmpty()) {
      return;
    }

    final ODatabaseDocumentInternal database = tx.getDatabase();
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) database.getStorage();
    tx.getIndexOperations()
        .forEach(
            (index, changes) -> {
              OIndexInternal resolvedIndex =
                  changes.resolveAssociatedIndex(
                      index, database.getMetadata().getIndexManagerInternal(), database);
              if (resolvedIndex != null && resolvedIndex.isUnique()) {
                for (final Object keyWithChange : changes.changesPerKey.keySet()) {
                  int version = storage.getVersionForKey(index, keyWithChange);
                  Object keyChange = OTransactionPhase1Task.mapKey(keyWithChange);
                  uniqueIndexKeys.add(new OTransactionUniqueKey(index, keyChange, version));
                }
                if (!changes.nullKeyChanges.isEmpty()) {
                  int version = storage.getVersionForKey(index, null);
                  uniqueIndexKeys.add(new OTransactionUniqueKey(index, null, version));
                }
              }
            });
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public long getDistributedTimeout() {
    return super.getDistributedTimeout() + (operations.size() / 10);
  }

  public int getRetryCount() {
    return retryCount;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public List<ORecordOperation> getOps() {
    return ops;
  }

  @Override
  public void received(ODistributedRequest request, ODistributedDatabase distributedDatabase) {
    if (notYetFinishedTask == null) {

      notYetFinishedTask =
          Orient.instance()
              .scheduleTask(
                  new Runnable() {
                    @Override
                    public void run() {
                      Orient.instance()
                          .submit(
                              () -> {
                                if (!finished) {
                                  ODistributedDatabaseImpl.sendResponseBack(
                                      this,
                                      distributedDatabase.getManager(),
                                      request.getId(),
                                      new OTransactionPhase1TaskResult(new OTxStillRunning()));
                                }
                              });
                    }
                  },
                  getDistributedTimeout(),
                  getDistributedTimeout());
    }
    if (distributedDatabase instanceof ODistributedDatabaseImpl) {
      ((ODistributedDatabaseImpl) distributedDatabase).trackTransactions(transactionId);
    }
  }

  @Override
  public void finished(ODistributedDatabase distributedDatabase) {
    if (notYetFinishedTask != null) {
      notYetFinishedTask.cancel();
    }
    if (distributedDatabase instanceof ODistributedDatabaseImpl) {
      ((ODistributedDatabaseImpl) distributedDatabase).untrackTransactions(transactionId);
    }
  }

  public OTransactionId getTransactionId() {
    return transactionId;
  }

  @Override
  public SortedSet<ORID> getRids() {
    SortedSet<ORID> set = new TreeSet<ORID>();
    if (operations.size() > 0) {
      for (ORecordOperationRequest operation : operations) {
        mapRidOp(set, operation);
      }
    } else {
      for (ORecordOperation operation : ops) {
        mapRid(set, operation);
      }
    }
    return set;
  }

  @Override
  public SortedSet<OTransactionUniqueKey> getUniqueKeys() {
    return uniqueIndexKeys;
  }

  private static void mapRidOp(Set<ORID> set, ORecordOperationRequest operation) {
    if (operation.getType() == ORecordOperation.CREATED) {
      // This guarantee that two allocation on the same cluster are not executed at
      // the same time
      set.add(new ORecordId(operation.getId().getClusterId(), -1));
    }
    set.add(operation.getId().copy());
  }

  public static void mapRid(Set<ORID> set, ORecordOperation operation) {
    if (operation.getType() == ORecordOperation.CREATED) {
      // This guarantee that two allocation on the same cluster are not executed at
      // the same time
      set.add(new ORecordId(operation.getRID().getClusterId(), -1));
    }
    set.add(operation.getRID().copy());
  }

  public static Object mapKey(Object key) {
    if (key instanceof ORID) {
      if (((ORID) key).isNew()) {
        return new ORecordId(((ORID) key).getClusterId(), -1);
      } else {
        return ((ORID) key).getIdentity().copy();
      }
    } else if (key instanceof OCompositeKey) {
      OCompositeKey cKey = (OCompositeKey) key;
      OCompositeKey newKey = new OCompositeKey();
      for (Object subKey : cKey.getKeys()) {
        newKey.addKey(mapKey(subKey));
      }
      return newKey;
    } else {
      return key;
    }
  }
}
