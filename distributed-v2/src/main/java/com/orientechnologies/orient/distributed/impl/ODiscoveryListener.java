package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

public interface ODiscoveryListener {
  static class NodeData {
    protected ONodeIdentity identity;
    protected String        address;
    protected int           port;
    protected boolean       leader;
    protected int           term;
    protected long          lastPingTimestamp;
    protected String        connectionUsername;
    protected String        connectionPassword;

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
