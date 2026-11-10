package com.orientechnologies.orient.core.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class OConfigDistributed {
  private boolean enabled;
  private String group;
  private String key;
  private String node;
  private int networkMinimumQuorum = 1;
  private OConfigDistributedTopology topology;

  public OConfigDistributed() {}

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

  public boolean isEnabled() {
    return enabled;
  }

  public String getGroup() {
    return group;
  }

  public String getKey() {
    return key;
  }

  public String getNode() {
    return node;
  }

  public OConfigDistributedTopology getTopology() {
    return topology;
  }
}
