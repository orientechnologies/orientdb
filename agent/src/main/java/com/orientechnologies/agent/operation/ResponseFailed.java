package com.orientechnologies.agent.operation;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

public class ResponseFailed implements NodeResponse {
  private NodeOperationResponse nodeOperationResponse;

  public ResponseFailed(NodeOperationResponse nodeOperationResponse) {
    this.nodeOperationResponse = nodeOperationResponse;
  }

  @Override
  public int getResponseType() {
    return 3;
  }
}
