package com.orientechnologies.orient.core.config;

public class OConfigDistributed {
  private boolean enabled;
  private String group;
  private String key;
  private String node;
  private int networkMinimumQuorum = 1;
  private OConfigDistributedTopology topology;

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setNetworkMinimumQuorum(int networkMinimumQuorum) {
    this.networkMinimumQuorum = networkMinimumQuorum;
  }

  public int getNetworkMinimumQuorum() {
    return networkMinimumQuorum;
  }
}
