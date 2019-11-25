package com.orientechnologies.agent.operation;

public class OperationResponseFromNode {
  private String       senderNodeName;
  private NodeResponse nodeResponse;

  public OperationResponseFromNode(final String senderNodeName, final NodeResponse nodeResponse) {
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
