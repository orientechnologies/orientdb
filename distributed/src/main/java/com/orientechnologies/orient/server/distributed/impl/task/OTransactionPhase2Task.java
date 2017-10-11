package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OConcurrentCreateException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class OTransactionPhase2Task extends OAbstractReplicatedTask {
  public static final int FACTORYID = 44;

  private ODistributedRequestId transactionId;
  private boolean               success;
  private int[]                 involvedClusters;
  private boolean hasResponse = false;

  public OTransactionPhase2Task(ODistributedRequestId transactionId, boolean success, int[] involvedClusters) {
    this.transactionId = transactionId;
    this.success = success;
    this.involvedClusters = involvedClusters;
  }

  public OTransactionPhase2Task() {

  }

  @Override
  public String getName() {
    return "TxPhase2";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
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
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    if (success) {
      if (!((ODatabaseDocumentDistributed) database).commit2pc(transactionId)) {
        ((ODatabaseDocumentDistributed) database).getStorageDistributed().getLocalDistributedDatabase()
            .reEnqueue(requestId.getNodeId(), requestId.getMessageId(), database.getName(), this);
        hasResponse = false;
      } else
        hasResponse = true;
    } else {
      ((ODatabaseDocumentDistributed) database).rollback2pc(transactionId);
      hasResponse = true;
    }
    return "OK"; //TODO
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
