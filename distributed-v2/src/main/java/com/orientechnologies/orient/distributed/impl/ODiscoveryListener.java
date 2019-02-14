package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

public interface ODiscoveryListener {
  static class NodeData {
    ONodeIdentity identity;
    String        address;
    int           port;
    boolean       master;
    int           term;
    long          lastPingTimestamp;
    String        connectionUsername;
    String        connectionPassword;

    public ONodeIdentity getNodeIdentity() {
      return identity;
    }
  }

  void nodeConnected(NodeData data);

  void nodeDisconnected(NodeData data);

  default void leaderElected(NodeData data) {
    //TODO
  }

}
