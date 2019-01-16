package com.orientechnologies.orient.distributed.impl;

public interface ODiscoveryListener {
  static class NodeData {
    String  name;
    String  address;
    int     port;
    boolean master;
    int     term;
    long    lastPingTimestamp;
  }

  void nodeJoined(NodeData data);

  void nodeLeft(NodeData data);

  default void leaderElected(NodeData data){
    //TODO
  }

}
