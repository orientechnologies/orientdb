package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OBackgroundNewDelta;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class OSyncDatabaseNewDeltaTask extends OAbstractRemoteTask {
  public static final int CHUNK_MAX_SIZE = 8388608; // 8MB

  public static final int FACTORYID = 57;
  private OTransactionSequenceStatus lastState;

  public OSyncDatabaseNewDeltaTask(OTransactionSequenceStatus lastState) {
    this.lastState = lastState;
  }

  public OSyncDatabaseNewDeltaTask() {}

  @Override
  public String getName() {
    return "transaction_id_delta_sync_database";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.ALL;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      OServer iServer,
      ODistributedServerManager iManager,
      ODatabaseDocumentInternal database)
      throws Exception {
    ODistributedDatabase db = iManager.getMessageService().getDatabase(database.getName());
    db.checkReverseSync(lastState);
    List<OTransactionId> missing = db.missingTransactions(lastState);
    if (!missing.isEmpty()) {
      Optional<OBackgroundNewDelta> delta =
          ((OAbstractPaginatedStorage) database.getStorage()).extractTransactionsFromWal(missing);
      if (delta.isPresent()) {
        ((ODistributedDatabaseImpl) db).setLastValidBackup(delta.get());
        return new ONewDeltaTaskResponse(
            new ODistributedDatabaseChunk(delta.get(), CHUNK_MAX_SIZE));
      } else {
        return new ONewDeltaTaskResponse(ONewDeltaTaskResponse.ResponseType.FULL_SYNC);
      }
    } else {
      return new ONewDeltaTaskResponse(ONewDeltaTaskResponse.ResponseType.NO_CHANGES);
    }
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

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }
}
