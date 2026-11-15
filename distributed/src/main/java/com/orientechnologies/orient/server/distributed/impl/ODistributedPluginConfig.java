package com.orientechnologies.orient.server.distributed.impl;

public class ODistributedPluginConfig {

  private String nodeName;
  private String config;
  private String hazelcast;

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getHazelcast() {
    return hazelcast;
  }

  public void setHazelcast(String hazelcast) {
    this.hazelcast = hazelcast;
  }

  public String getConfig() {
    return config;
  }

  public void setConfig(String config) {
    this.config = config;
  }
}
