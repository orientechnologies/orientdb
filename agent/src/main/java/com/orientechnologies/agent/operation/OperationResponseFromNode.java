package com.orientechnologies.agent.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;

public class OperationResponseFromNode {
  private ONodeId senderNodeId;
  private NodeResponse nodeResponse;

  public OperationResponseFromNode(final ONodeId senderNodeName, final NodeResponse nodeResponse) {
    this.senderNodeId = senderNodeName;
    this.nodeResponse = nodeResponse;
  }

  public ONodeId getSenderNodeId() {
    return senderNodeId;
  }

  public String getSenderNodeName() {
    return senderNodeId.getNode();
  }

  public NodeResponse getNodeResponse() {
    return nodeResponse;
  }
}
