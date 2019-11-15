package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

public interface OCoordinatedExecutor {

  void executeOperationRequest(ONodeIdentity sender, OOperationRequest request);

  void executeOperationResponse(ONodeIdentity sender, OOperationResponse response);

  void executeSubmitResponse(ONodeIdentity sender, ONetworkSubmitResponse response);

  void executeSubmitRequest(ONodeIdentity sender, ONetworkSubmitRequest request);

  void executeStructuralSubmitRequest(ONodeIdentity sender, ONetworkStructuralSubmitRequest request);

  void executeStructuralSubmitResponse(ONodeIdentity sender, ONetworkStructuralSubmitResponse response);

  void executePropagate(ONodeIdentity sender, ONetworkPropagate propagate);

  void executeConfirm(ONodeIdentity sender, ONetworkConfirm confirm);

  void executeAck(ONodeIdentity sender, ONetworkAck ack);
}
