package com.orientechnologies.orient.distributed.impl.coordinator;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.database.operations.ODatabaseSyncRequest;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.log.OOperationLog;
import com.orientechnologies.orient.distributed.network.ODistributedNetwork;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ODistributedExecutor implements AutoCloseable {

  private final ODistributedNetwork network;
  private OOperationLog operationLog;
  private ExecutorService executor;
  private OrientDBInternal orientDB;
  private final String database;

  public ODistributedExecutor(
      ExecutorService executor,
      OOperationLog operationLog,
      OrientDBInternal orientDB,
      ODistributedNetwork network,
      String database) {
    this.operationLog = operationLog;
    this.executor = executor;
    this.orientDB = orientDB;
    this.database = database;
    this.network = network;
  }

  public void receive(ONodeIdentity member, OLogId opId, ONodeRequest request) {
    // TODO: sort for opId before execute and execute the operations only if is strictly sequential,
    // otherwise wait.
    executor.execute(
        () -> {
          if (operationLog.logReceived(opId, request)) {
            ONodeResponse response;
            try (ODatabaseDocumentInternal session = orientDB.openNoAuthorization(database)) {
              response = request.execute(member, opId, this, session);
            }
            network.sendResponse(member, database, opId, response);
          } else {
            resendRequest(member, operationLog.lastPersistentLog());
          }
        });
  }

  private void resendRequest(ONodeIdentity leader, OLogId opId) {
    network.send(leader, new ODatabaseSyncRequest(database, Optional.ofNullable(opId)));
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void notifyLastValidLog(ONodeIdentity leader, OLogId leaderLastValid) {
    // TODO: check leader
    executor.execute(
        () -> {
          OLogId logLast = operationLog.lastPersistentLog();
          if (logLast.compareTo(leaderLastValid) < 0) {
            resendRequest(leader, logLast);
          }
        });
  }
}
