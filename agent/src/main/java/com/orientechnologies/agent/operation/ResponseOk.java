package com.orientechnologies.agent.operation;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

public class ResponseOk implements NodeResponse {
  private NodeOperationResponse payload;

  public ResponseOk(NodeOperationResponse payload) {
    this.payload = payload;
  }

  @Override
  public int getResponseType() {
    return 1;
  }

  public NodeOperationResponse getPayload() {
    return payload;
  }
}
