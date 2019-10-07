package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;

public class ONodeInternalConfiguration {

  private ONodeIdentity nodeIdentity;
  private String        connectionUsername;
  private String        connectionPassword;

  public ONodeInternalConfiguration(ONodeIdentity nodeIdentity, String connectionUsername,
      String connectionPassword) {
    this.nodeIdentity = nodeIdentity;
    this.connectionUsername = connectionUsername;
    this.connectionPassword = connectionPassword;
  }

  public ONodeIdentity getNodeIdentity() {
    return nodeIdentity;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }

  public String getConnectionUsername() {
    return connectionUsername;
  }
}
