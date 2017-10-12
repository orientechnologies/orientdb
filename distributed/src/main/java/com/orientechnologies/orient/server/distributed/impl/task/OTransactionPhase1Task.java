package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.client.remote.message.OMessageHelper;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.OTransactionOptimisticDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedLockException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class OTransactionPhase1Task extends OAbstractReplicatedTask {
  public static final int FACTORYID = 43;

  private volatile boolean                hasResponse;
  private          OLogSequenceNumber     lastLSN;
  private          List<ORecordOperation> ops;

  public OTransactionPhase1Task() {
    ops = new ArrayList<>();
  }

  public OTransactionPhase1Task(List<ORecordOperation> ops) {
    this.ops = ops;
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
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    OTransactionOptimisticDistributed tx = new OTransactionOptimisticDistributed(database, ops);
    OTransactionResultPayload res1 = executeTransaction(requestId, (ODatabaseDocumentDistributed) database, tx, false);
    if (res1 == null) {
      ((ODatabaseDocumentDistributed) database).getStorageDistributed().getLocalDistributedDatabase()
          .reEnqueue(requestId.getNodeId(), requestId.getMessageId(), database.getName(), this);
      hasResponse = false;
      return null;
    }
    hasResponse = true;
    return new OTransactionPhase1TaskResult(res1);
  }

  @Override
  public boolean hasResponse() {
    return hasResponse;
  }

  public static OTransactionResultPayload executeTransaction(ODistributedRequestId requestId, ODatabaseDocumentDistributed database,
      OTransactionInternal tx, boolean local) {
    OTransactionResultPayload payload;
    try {
      if (database.beginDistributedTx(requestId, tx, local)) {
        payload = new OTxSuccess();
      } else {
        return null;
      }
    } catch (OConcurrentModificationException ex) {
      payload = new OTxConcurrentModification((ORecordId) ex.getRid(), ex.getEnhancedDatabaseVersion());
    } catch (ODistributedLockException | OLockException ex) {
      payload = new OTxLockTimeout();
    } catch (ORecordDuplicatedException ex) {
      //TODO:Check if can get out the key
      payload = new OTxUniqueIndex((ORecordId) ex.getRid(), ex.getIndexName(), null);
    } catch (RuntimeException ex) {
      payload = new OTxException(ex);
    }
    return payload;
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {

    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      ORecordOperationRequest req = OMessageHelper.readTransactionEntry(in);

      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
      case ORecordOperation.DELETED:
        record = ORecordSerializerNetworkV37.INSTANCE.fromStream(req.getRecord(), null, null);
        break;
      }
      ORecordInternal.setIdentity(record, (ORecordId) req.getId());
      ORecordInternal.setVersion(record, req.getVersion());
      ORecordOperation op = new ORecordOperation(record, type);
      ops.add(op);

    }

    lastLSN = new OLogSequenceNumber(in);
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(ops.size());

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
      case ORecordOperation.UPDATED:
      case ORecordOperation.DELETED:
        request.setRecord(ORecordSerializerNetworkV37.INSTANCE.toStream(txEntry.getRecord(), false));
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      }
      OMessageHelper.writeTransactionEntry(out, request);
    }
    lastLSN.toStream(out);
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public void init(OTransactionInternal operations) {
    this.ops = new ArrayList<>(operations.getRecordOperations());
  }

  public void setLastLSN(OLogSequenceNumber lastLSN) {
    this.lastLSN = lastLSN;
  }

  @Override
  public int[] getPartitionKey() {
    return ops.stream().mapToInt((x) -> x.getRID().getClusterId()).toArray();
  }
}
