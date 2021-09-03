package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.operations.OOperation;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.submit.OStructuralSubmitResponse;

public interface OCoordinatedExecutor {

  void executeOperationRequest(
      ONodeIdentity sender, String database, OLogId id, ONodeRequest request);

  void executeOperationResponse(
      ONodeIdentity sender, String database, OLogId id, ONodeResponse response);

  void executeSubmitResponse(
      ONodeIdentity sender,
      String database,
      OSessionOperationId operationId,
      OSubmitResponse response);

  void executeSubmitRequest(
      ONodeIdentity sender,
      String database,
      OSessionOperationId operationId,
      OSubmitRequest request);

  void executeStructuralSubmitRequest(
      ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitRequest request);

  void executeStructuralSubmitResponse(
      ONodeIdentity sender, OSessionOperationId id, OStructuralSubmitResponse response);

  void executePropagate(ONodeIdentity sender, OLogId id, ORaftOperation operation);

  void executeConfirm(ONodeIdentity sender, OLogId id);

  void executeAck(ONodeIdentity sender, OLogId id);

  void nodeConnected(ONodeIdentity identity);

  void nodeDisconnected(ONodeIdentity identity);

  void setLeader(ONodeIdentity leader, OLogId leaderLastValid);

  void setDatabaseLeader(ONodeIdentity leader, String database, OLogId leaderLastValid);

  void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid);

  void notifyLastDatabaseOperation(ONodeIdentity leader, String database, OLogId leaderLastValid);

  void executeOperation(ONodeIdentity sender, OOperation operation);
}
