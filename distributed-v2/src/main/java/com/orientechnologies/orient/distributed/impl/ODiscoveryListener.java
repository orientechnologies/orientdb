package com.orientechnologies.orient.distributed.impl;

public interface ODiscoveryListener {
  static class NodeData {
    String name;
    String address;
    int    port;
    long   lastPingTimestamp;
  }

  void nodeJoined(NodeData data);

  public void nodeLeft(NodeData data);
}
