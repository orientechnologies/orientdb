package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.raft.ORaftOperation;

public interface OCoordinatedExecutor {

  void executeOperationRequest(OOperationRequest request);

  void executeOperationResponse(OOperationResponse response);

  void executeSubmitResponse(ONetworkSubmitResponse response);

  void executeSubmitRequest(ONetworkSubmitRequest request);

  void executeStructuralOperationRequest(OStructuralOperationRequest request);

  void executeStructuralOperationResponse(OStructuralOperationResponse response);

  void executeStructuralSubmitRequest(ONetworkStructuralSubmitRequest request);

  void executeStructuralSubmitResponse(ONetworkStructuralSubmitResponse response);

  void executePropagate(ONetworkPropagate propagate);

  void executeConfirm(ONetworkConfirm confirm);

  void executeAck(ONetworkAck ack);
}
