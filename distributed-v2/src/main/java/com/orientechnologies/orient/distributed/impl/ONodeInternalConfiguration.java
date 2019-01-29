package com.orientechnologies.orient.distributed.impl;

import java.util.UUID;

public class ONodeInternalConfiguration {

  private UUID nodeId;
  private String connectionUsername;
  private String connectionPassword;

  public ONodeInternalConfiguration(UUID nodeId, String connectionUsername, String connectionPassword) {
    this.nodeId = nodeId;
    this.connectionUsername = connectionUsername;
    this.connectionPassword = connectionPassword;
  }

  public UUID getNodeId() {
    return nodeId;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }

  public String getConnectionUsername() {
    return connectionUsername;
  }
}
