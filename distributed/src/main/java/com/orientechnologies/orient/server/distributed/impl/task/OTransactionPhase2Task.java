package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
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

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class OTransactionPhase2Task extends OAbstractReplicatedTask {
  public static final int FACTORYID = 44;

  private ODistributedRequestId transactionId;
  private boolean               success;
  private int[]                 involvedClusters;
  private          boolean hasResponse = false;
  private volatile int     retryCount  = 0;

  public OTransactionPhase2Task(ODistributedRequestId transactionId, boolean success, int[] involvedClusters,
      OLogSequenceNumber lsn) {
    this.transactionId = transactionId;
    this.success = success;
    this.involvedClusters = involvedClusters;
    this.lastLSN = lsn;
  }

  public OTransactionPhase2Task() {

  }

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
    this.involvedClusters = new int[length];
    for (int i = 0; i < length; i++) {
      this.involvedClusters[i] = in.readInt();
    }
    this.success = in.readBoolean();
    this.lastLSN = new OLogSequenceNumber(in);
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    out.writeInt(transactionId.getNodeId());
    out.writeLong(transactionId.getMessageId());
    out.writeInt(involvedClusters.length);
    for (int involvedCluster : involvedClusters) {
      out.writeInt(involvedCluster);
    }
    out.writeBoolean(success);
    lastLSN.toStream(out);
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    if (success) {
      if (!((ODatabaseDocumentDistributed) database).commit2pc(transactionId)) {
        retryCount++;
        if (retryCount < database.getConfiguration().getValueAsInteger(DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY)) {
          ((ODatabaseDocumentDistributed) database).getStorageDistributed().getLocalDistributedDatabase()
              .reEnqueue(requestId.getNodeId(), requestId.getMessageId(), database.getName(), this);
          hasResponse = false;
        } else {
          ((ODatabaseDocumentDistributed) database).rollback2pc(transactionId);
          Orient.instance().submit(() -> {
            iManager.installDatabase(false, database.getName(), true, true);
          });
          hasResponse = true;
          return "KO";
        }
      } else
        hasResponse = true;
    } else {
      ((ODatabaseDocumentDistributed) database).rollback2pc(transactionId);
      hasResponse = true;
    }
    return "OK"; //TODO
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
    return involvedClusters;
  }
}
