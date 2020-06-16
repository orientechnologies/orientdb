package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.util.OPair;
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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDeltaDistributed;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.impl.ONewDistributedTxContextImpl;
import com.orientechnologies.orient.server.distributed.impl.OTransactionOptimisticDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedKeyLockedException;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.orientechnologies.orient.server.distributed.impl.ONewDistributedTxContextImpl.Status.TIMEDOUT;

/**
 * @author luigi dell'aquila (l.dellaquila - at - orientdb.com)
 */
public class OTransactionPhase1Task extends OAbstractReplicatedTask implements OLockKeySource {
  public static final int FACTORYID = 43;

  private volatile  boolean                                         hasResponse;
  private           OLogSequenceNumber                              lastLSN;
  private           List<ORecordOperation>                          ops;
  private           List<ORecordOperationRequest>                   operations;
  // Contains the set of <index-name, index-key> for keys (belonging to a unique index) that are changed with this tx.
  // If a null key is allowed by the index, index-key could be null.
  private           SortedSet<OPair<String, Object>>                uniqueIndexKeys;
  private           OCommandDistributedReplicateRequest.QUORUM_TYPE quorumType = OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  private transient int                                             retryCount = 0;
  private volatile  boolean                                         finished;
  private           TimerTask                                       notYetFinishedTask;
  private           OTransactionId                                  transactionId;

  public OTransactionPhase1Task() {
    ops = new ArrayList<>();
    operations = new ArrayList<>();
    uniqueIndexKeys = new TreeSet<>();
  }

  public OTransactionPhase1Task(List<ORecordOperation> ops, OTransactionId transactionId) {
    this.ops = ops;
    operations = new ArrayList<>();
    uniqueIndexKeys = new TreeSet<>();
    this.transactionId = transactionId;
    genOps(ops);
  }

  public void genOps(List<ORecordOperation> ops) {
    for (ORecordOperation txEntry : ops) {
      if (txEntry.type == ORecordOperation.LOADED)
        continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
        request.setRecord(ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      case ORecordOperation.UPDATED:
        if (request.getRecordType() == ODocument.RECORD_TYPE) {
          request.setRecord(ODocumentSerializerDeltaDistributed.instance().serializeDelta((ODocument) txEntry.getRecord()));
        } else {
          request.setRecord(ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
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
    return quorumType;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {

    if (iManager != null) {
      iManager.messageBeforeOp("prepare1Phase", requestId);
    }
    convert(database);
    if (iManager != null) {
      iManager.messageAfterOp("prepare1Phase", requestId);
    }

    OTransactionOptimisticDistributed tx = new OTransactionOptimisticDistributed(database, ops);
    //No need to increase the lock timeout here with the retry because this retries are not deadlock retries
    OTransactionResultPayload res1;
    try {
      res1 = executeTransaction(requestId, transactionId, (ODatabaseDocumentDistributed) database, tx, false, retryCount);
    } catch (Exception e) {
      this.finished = true;
      if (this.notYetFinishedTask != null) {
        this.notYetFinishedTask.cancel();
      }
      throw e;
    }
    if (res1 == null) {
      final int autoRetryDelay = OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();
      retryCount++;
      ((ODatabaseDocumentDistributed) database).getStorageDistributed().getLocalDistributedDatabase()
          .reEnqueue(requestId.getNodeId(), requestId.getMessageId(), database.getName(), this, retryCount, autoRetryDelay);
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

  public static OTransactionResultPayload executeTransaction(ODistributedRequestId requestId, OTransactionId id,
      ODatabaseDocumentDistributed database, OTransactionInternal tx, boolean local, int retryCount) {
    OTransactionResultPayload payload;
    try {
      if (!local) {
        ODistributedDatabase localDistributedDatabase = database.getStorageDistributed().getLocalDistributedDatabase();
        Optional<OTransactionId> result = localDistributedDatabase.validate(id);
        if (result.isPresent()) {
          ONewDistributedTxContextImpl txContext = new ONewDistributedTxContextImpl(
              (ODistributedDatabaseImpl) localDistributedDatabase, requestId, tx, id);
          txContext.setStatus(TIMEDOUT);
          database.register(requestId, localDistributedDatabase, txContext);
          return new OTxInvalidSequential(result.get());
        }
      }
      if (database.beginDistributedTx(requestId, id, tx, local, retryCount)) {
        payload = new OTxSuccess();
      } else {
        return null;
      }
    } catch (OConcurrentModificationException ex) {
      payload = new OTxConcurrentModification((ORecordId) ex.getRid(), ex.getEnhancedDatabaseVersion());
    } catch (ODistributedRecordLockedException ex) {
      payload = new OTxRecordLockTimeout(ex.getNode(), ex.getRid());
    } catch (ODistributedKeyLockedException ex) {
      payload = new OTxKeyLockTimeout(ex.getNode(), ex.getKey());
    } catch (ORecordDuplicatedException ex) {
      payload = new OTxUniqueIndex((ORecordId) ex.getRid(), ex.getIndexName(), ex.getKey());
    } catch (OConcurrentCreateException ex) {
      payload = new OTxConcurrentCreation(ex.getActualRid(), ex.getExpectedRid());
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

    lastLSN = new OLogSequenceNumber(in);
    if (lastLSN.getSegment() == -1 && lastLSN.getSegment() == -1) {
      lastLSN = null;
    }

    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    OMessageHelper.readTxUniqueIndexKeys(uniqueIndexKeys, serializer, in);
  }

  private void convert(ODatabaseDocumentInternal database) {
    for (ORecordOperationRequest req : operations) {
      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
      case ORecordOperation.CREATED: {
        record = ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
        ORecordInternal.setRecordSerializer(record, database.getSerializer());
        break;
      }
      case ORecordOperation.UPDATED: {
        if (req.getRecordType() == ODocument.RECORD_TYPE) {
          record = database.load(req.getId());
          if (record == null) {
            record = new ODocument();
          }
          ((ODocument) record).deserializeFields();
          ODocumentInternal.clearTransactionTrackData((ODocument) record);
          ODocumentSerializerDeltaDistributed.instance().deserializeDelta(req.getRecord(), (ODocument) record);
          /// Got record with empty deltas, at this level we mark the record dirty anyway.
          if (!req.isContentChanged()) {
            record.setDirtyNoChanged();
          } else {
            record.setDirty();
          }
        } else {
          record = ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
          ORecordInternal.setRecordSerializer(record, database.getSerializer());
        }
        break;
      }
      case ORecordOperation.DELETED: {
        record = database.load(req.getId());
        if (record == null) {
          record = Orient.instance().getRecordFactoryManager()
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
    if (lastLSN == null) {
      new OLogSequenceNumber(-1, -1).toStream(out);
    } else {
      lastLSN.toStream(out);
    }

    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    OMessageHelper.writeTxUniqueIndexKeys(uniqueIndexKeys, serializer, out);
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public void init(OTransactionId transactionId, OTransactionInternal operations) {
    this.transactionId = transactionId;
    final ODatabaseDocumentInternal database = operations.getDatabase();
    operations.getIndexOperations().forEach((index, changes) -> {
      if (changes.resolveAssociatedIndex(index, database.getMetadata().getIndexManagerInternal(), database).isUnique()) {
        quorumType = OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE_ALL_MASTERS;
        for (Object keyWithChange : changes.changesPerKey.keySet()) {
          uniqueIndexKeys.add(new OPair<>(index, keyWithChange));
        }
        if (!changes.nullKeyChanges.entries.isEmpty()) {
          uniqueIndexKeys.add(new OPair<>(index, null));
        }
      }
    });
    this.ops = new ArrayList<>(operations.getRecordOperations());
    genOps(this.ops);
  }

  public void setLastLSN(OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }

  @Override
  public OLogSequenceNumber getLastLSN() {
    return lastLSN;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public int[] getPartitionKey() {
    if (operations.size() > 0)
      return operations.stream().mapToInt((x) -> x.getId().getClusterId()).toArray();
    else
      return ops.stream().mapToInt((x) -> x.getRID().getClusterId()).toArray();
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

      notYetFinishedTask = Orient.instance().scheduleTask(new Runnable() {
        @Override
        public void run() {
          Orient.instance().submit(() -> {
            if (!finished) {
              ODistributedDatabaseImpl.sendResponseBack(this, distributedDatabase.getManager(), request.getId(),
                  new OTransactionPhase1TaskResult(new OTxStillRunning()));
            }
          });
        }
      }, getDistributedTimeout(), getDistributedTimeout());
    }
  }

  @Override
  public void finished() {
    if (notYetFinishedTask != null) {
      notYetFinishedTask.cancel();
    }
  }

  public OTransactionId getTransactionId() {
    return transactionId;
  }

  @Override
  public SortedSet<ORID> getRids() {
    Set<ORID> set;
    if (operations.size() > 0) {
      set = operations.stream().map((x) -> x.getId()).collect(Collectors.toSet());
    } else {
      set = ops.stream().map((x) -> x.getRID()).collect(Collectors.toSet());
    }
    return new TreeSet<ORID>(set);
  }

  @Override
  public SortedSet<OPair<String, Object>> getUniqueKeys() {
    return uniqueIndexKeys;
  }
}
