package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OBackgroundNewDelta;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class OSyncDatabaseNewDeltaTask extends OAbstractReplicatedTask {
  public static final int CHUNK_MAX_SIZE = 8388608;    // 8MB

  public static final int                        FACTORYID = 29;
  private             OTransactionSequenceStatus lastState;

  public OSyncDatabaseNewDeltaTask(OTransactionSequenceStatus lastState) {
    this.lastState = lastState;
  }

  public OSyncDatabaseNewDeltaTask() {
  }

  @Override
  public String getName() {
    return "TransactionId Delta Sync Database";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    ODistributedDatabase db = iManager.getMessageService().getDatabase(database.getName());
    List<OTransactionId> missing = db.missingTransactions(lastState);
    OBackgroundNewDelta delta = ((OAbstractPaginatedStorage) database.getStorage().getUnderlying())
        .extractTransactionsFromWal(missing);
    final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(delta, CHUNK_MAX_SIZE, null);

    if (chunk.last)
      // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
      iManager.setDatabaseStatus(iManager.getLocalNodeName(), database.getName(), ODistributedServerManager.DB_STATUS.ONLINE);
    ((ODistributedStorage) database.getStorage()).setLastValidBackup(delta);
    return new ONewDeltaTaskResponse(chunk);
  }

  @Override
  public void toStream(DataOutput out) throws IOException {
    lastState.writeNetwork(out);
  }

  @Override
  public void fromStream(DataInput in, ORemoteTaskFactory factory) throws IOException {
    lastState = OTransactionSequenceStatus.readNetwork(in);
  }

  public OTransactionSequenceStatus getLastState() {
    return lastState;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
