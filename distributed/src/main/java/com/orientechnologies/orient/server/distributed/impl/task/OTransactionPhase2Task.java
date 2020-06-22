package com.orientechnologies.orient.server.distributed.impl.task;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.OMessageHelper;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/** @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com) */
public class OTransactionPhase2Task extends OAbstractReplicatedTask implements OLockKeySource {
  public static final int FACTORYID = 44;

  private ODistributedRequestId transactionId;
  private boolean success;
  private SortedSet<ORID> involvedRids;
  private SortedSet<OPair<String, Object>> uniqueIndexKeys = new TreeSet<>();
  private boolean hasResponse = false;
  private volatile int retryCount = 0;

  public OTransactionPhase2Task(
      ODistributedRequestId transactionId,
      boolean success,
      SortedSet<ORID> rids,
      SortedSet<OPair<String, Object>> uniqueIndexKeys,
      OLogSequenceNumber lsn) {
    this.transactionId = transactionId;
    this.success = success;
    this.involvedRids = rids;
    this.uniqueIndexKeys = uniqueIndexKeys;
    this.lastLSN = lsn;
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
    int nodeId = in.readInt();
    long messageId = in.readLong();
    this.transactionId = new ODistributedRequestId(nodeId, messageId);

    int length = in.readInt();
    this.involvedRids = new TreeSet<ORID>();
    for (int i = 0; i < length; i++) {
      involvedRids.add(ORecordId.deserialize(in));
    }
    this.success = in.readBoolean();
    this.lastLSN = new OLogSequenceNumber(in);
    if (lastLSN.getSegment() == -1 && lastLSN.getSegment() == -1) {
      lastLSN = null;
    }
    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    OMessageHelper.readTxUniqueIndexKeys(uniqueIndexKeys, serializer, in);
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(transactionId.getNodeId());
    out.writeLong(transactionId.getMessageId());
    out.writeInt(involvedRids.size());
    for (ORID id : involvedRids) {
      ORecordId.serialize(id, out);
    }
    out.writeBoolean(success);
    if (lastLSN == null) {
      new OLogSequenceNumber(-1, -1).toStream(out);
    } else {
      lastLSN.toStream(out);
    }
    ORecordSerializerNetworkDistributed serializer = ORecordSerializerNetworkDistributed.INSTANCE;
    OMessageHelper.writeTxUniqueIndexKeys(uniqueIndexKeys, serializer, out);
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    if (success) {
      if (!((ODatabaseDocumentDistributed) database).commit2pc(transactionId, false, requestId)) {
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
              .getStorageDistributed()
              .getLocalDistributedDatabase()
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
                            transactionId);
                    iManager.installDatabase(false, database.getName(), true, true);
                  });
          hasResponse = true;
          return "KO";
        }
      } else {
        hasResponse = true;
      }
    } else {
      if (!((ODatabaseDocumentDistributed) database).rollback2pc(transactionId)) {
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
              .getStorageDistributed()
              .getLocalDistributedDatabase()
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

  public ODistributedRequestId getTransactionId() {
    return transactionId;
  }

  @Override
  public OLogSequenceNumber getLastLSN() {
    return super.getLastLSN();
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
  public int[] getPartitionKey() {
    return null;
  }

  @Override
  public SortedSet<ORID> getRids() {
    return involvedRids;
  }

  @Override
  public SortedSet<OPair<String, Object>> getUniqueKeys() {
    return uniqueIndexKeys;
  }
}
