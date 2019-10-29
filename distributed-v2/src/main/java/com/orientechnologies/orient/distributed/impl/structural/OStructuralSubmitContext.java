package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.concurrent.Future;

/**
 * manages client->follower->leader pass-through communication for write operations
 */
public interface OStructuralSubmitContext {

  Future<OStructuralSubmitResponse> send(OSessionOperationId requestId, OStructuralSubmitRequest response);

  OStructuralSubmitResponse sendAndWait(OSessionOperationId operationId, OStructuralSubmitRequest request);

  void receive(OSessionOperationId requestId, OStructuralSubmitResponse response);

  void setLeader(ODistributedChannel channel);

}
