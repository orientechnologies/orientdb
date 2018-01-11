package com.orientechnologies.agent.operation;

import com.orientechnologies.agent.operation.NodeResponse;

public class OperationResponseFromNode {
  private String       senderNodeName;
  private NodeResponse nodeResponse;

  public OperationResponseFromNode(String senderNodeName, NodeResponse nodeResponse) {
    this.senderNodeName = senderNodeName;
    this.nodeResponse = nodeResponse;
  }

  public String getSenderNodeName() {
    return senderNodeName;
  }

  public NodeResponse getNodeResponse() {
    return nodeResponse;
  }
}
