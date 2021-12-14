package com.orientechnologies.orient.server.distributed.impl.task;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/** @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com) */
public class OTransactionPhase2Task extends OAbstractRemoteTask implements OLockKeySource {
  public static final int FACTORYID = 44;

  private OTransactionId transactionId;
  private ODistributedRequestId firstPhaseId;
  // whether to commit or abort.
  private boolean success;
  private SortedSet<ORID> involvedRids;
  private SortedSet<OTransactionUniqueKey> uniqueIndexKeys = new TreeSet<>();
  private boolean hasResponse = false;
  private volatile int retryCount = 0;

  public OTransactionPhase2Task(
      ODistributedRequestId firstPhaseId,
      boolean success,
      SortedSet<ORID> rids,
      SortedSet<OTransactionUniqueKey> uniqueIndexKeys,
      OTransactionId transactionId) {
    this.firstPhaseId = firstPhaseId;
    this.success = success;
    this.involvedRids = rids;
    this.uniqueIndexKeys = uniqueIndexKeys;
    this.transactionId = transactionId;
  }

  public OTransactionPhase2Task() {}

  @Override
  public String getName() {
    return "TxPhase2";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    this.transactionId = OTransactionId.read(in);
    int nodeId = in.readInt();
    long messageId = in.readLong();
    this.firstPhaseId = new ODistributedRequestId(nodeId, messageId);

    int length = in.readInt();
    this.involvedRids = new TreeSet<ORID>();
    for (int i = 0; i < length; i++) {
      involvedRids.add(ORecordId.deserialize(in));
    }
    this.success = in.readBoolean();
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

  @Override
  public void toStream(DataOutput out) throws IOException {
    this.transactionId.write(out);
    out.writeInt(firstPhaseId.getNodeId());
    out.writeLong(firstPhaseId.getMessageId());
    out.writeInt(involvedRids.size());
    for (ORID id : involvedRids) {
      ORecordId.serialize(id, out);
    }
    out.writeBoolean(success);
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
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    if (success) {
      if (!((ODatabaseDocumentDistributed) database).commit2pc(firstPhaseId, false, requestId)) {
        final int autoRetryDelay =
            OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();
        retryCount++;
        if (retryCount
            < database
                .getConfiguration()
                .getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
          OLogManager.instance()
              .info(
                  OTransactionPhase2Task.this,
                  "Received second phase but not yet first phase, re-enqueue second phase");
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
        } else {
          Orient.instance()
              .submit(
                  () -> {
                    OLogManager.instance()
                        .warn(
                            OTransactionPhase2Task.this,
                            "Reached limit of retry for commit tx:%s forcing database re-install",
                            firstPhaseId);
                    iManager.installDatabase(false, database.getName(), true, true);
                  });
          hasResponse = true;
          return "KO";
        }
      } else {
        hasResponse = true;
      }
    } else {
      if (!((ODatabaseDocumentDistributed) database).rollback2pc(firstPhaseId)) {
        final int autoRetryDelay =
            OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.getValueAsInteger();
        retryCount++;
        if (retryCount
            < database
                .getConfiguration()
                .getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
          OLogManager.instance()
              .info(
                  OTransactionPhase2Task.this,
                  "Received second phase but not yet first phase, re-enqueue second phase");
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
        } else {
          // ABORT THE OPERATION IF THERE IS A NOT VALID TRANSACTION ACTIVE WILL BE ROLLBACK ON
          // RE-INSTALL
          hasResponse = true;
          return "KO";
        }
      } else {
        hasResponse = true;
      }
    }
    return "OK";
  }

  public int getRetryCount() {
    return retryCount;
  }

  public ODistributedRequestId getFirstPhaseId() {
    return firstPhaseId;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Override
  public boolean hasResponse() {
    return hasResponse;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  @Override
  public SortedSet<ORID> getRids() {
    return involvedRids;
  }

  @Override
  public SortedSet<OTransactionUniqueKey> getUniqueKeys() {
    return uniqueIndexKeys;
  }

  public OTransactionId getTransactionId() {
    return transactionId;
  }
}
