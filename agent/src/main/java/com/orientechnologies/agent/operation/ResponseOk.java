package com.orientechnologies.agent.operation;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

public class ResponseOk implements NodeResponse {
  private NodeOperationResponse nodeOperationResponse;

  public ResponseOk(NodeOperationResponse nodeOperationResponse) {
    this.nodeOperationResponse = nodeOperationResponse;
  }

  @Override
  public int getResponseType() {
    return 1;
  }
}
