package com.orientechnologies.orient.distributed.network;

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
import java.util.Collection;

public interface ODistributedNetwork {

  /** Structural forward */
  void submit(ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitRequest request);

  /** Structural forward reply */
  void reply(ONodeIdentity to, OSessionOperationId operationId, OStructuralSubmitResponse response);

  /** Structural Operation */
  void propagate(Collection<ONodeIdentity> to, OLogId id, ORaftOperation operation);

  /** Structural Operation ack */
  void ack(ONodeIdentity to, OLogId logId);

  /** Structural Operation confirmation */
  void confirm(Collection<ONodeIdentity> to, OLogId id);

  /** Data operation forward */
  void submit(
      ONodeIdentity coordinator,
      String database,
      OSessionOperationId operationId,
      OSubmitRequest request);

  /** Data operation reply */
  void replay(
      ONodeIdentity to, String database, OSessionOperationId operationId, OSubmitResponse response);

  /** Data Request */
  void sendResponse(ONodeIdentity to, String database, OLogId opId, ONodeResponse response);

  /** Data response */
  void sendRequest(Collection<ONodeIdentity> to, String database, OLogId id, ONodeRequest request);

  /** Structural Full sync */
  void send(ONodeIdentity identity, OOperation operation);

  /** Structural Full sync */
  void sendAll(Collection<ONodeIdentity> members, OOperation operation);

  /** Structural ping */
  void notifyLastStructuralOperation(ONodeIdentity leader, OLogId leaderLastValid);

  /** Database ping */
  void notifyLastDbOperation(ONodeIdentity leader, String database, OLogId leaderLastValid);
}
