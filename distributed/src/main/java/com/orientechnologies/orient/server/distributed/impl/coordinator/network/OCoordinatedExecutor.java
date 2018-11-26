package com.orientechnologies.orient.server.distributed.impl.coordinator.network;

public interface OCoordinatedExecutor {

  void executeOperationRequest(OOperationRequest request);

  void executeOperationResponse(OOperationResponse response);

  void executeSubmitResponse(ONetworkSubmitResponse response);

  void executeSubmitRequest(ONetworkSubmitRequest request);

  void executeStructuralOperationRequest(OStructuralOperationRequest request);

  void executeStructuralOperationResponse(OStructuralOperationResponse response);

  void executeStructuralSubmitRequest(ONetworkStructuralSubmitRequest request);

  void executeStructuralSubmitResponse(ONetworkStructuralSubmitResponse response);
}
