package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedChannel;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

import java.util.concurrent.Future;

public interface OStructuralSubmitContext {

  Future<OStructuralSubmitResponse> send(OSessionOperationId requestId, OStructuralSubmitRequest response);

  void receive(OSessionOperationId requestId, OStructuralSubmitResponse response);

  void setLeader(ODistributedChannel channel);

  void receive(OSessionOperationId operationId);
}
