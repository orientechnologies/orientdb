package com.orientechnologies.orient.core.db.config;

import java.util.UUID;

public class ONodeConfiguration {
  private String nodeName;
  private String groupName     = "OrientDB";
  private String groupPassword = "OrientDB";

  private int quorum = 2;

  private Integer tcpPort;

  private OMulticastConfguration multicast = new OMulticastConfguration();

  protected ONodeConfiguration() {
    nodeName = "node-" + UUID.randomUUID().toString();
  }

  protected ONodeConfiguration(String nodeName, String groupName, String groupPassword, int quorum, Integer tcpPort,
      OMulticastConfguration multicast) {
    this.nodeName = nodeName;
    this.groupName = groupName;
    this.groupPassword = groupPassword;
    this.quorum = quorum;
    this.tcpPort = tcpPort;
    this.multicast = multicast;
  }

  public int getQuorum() {
    return quorum;
  }

  protected void setQuorum(int quorum) {
    this.quorum = quorum;
  }

  public String getNodeName() {
    return nodeName;
  }

  protected void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public Integer getTcpPort() {
    return tcpPort;
  }

  public void setTcpPort(int tcpPort) {
    this.tcpPort = tcpPort;
  }

  public String getGroupPassword() {
    return groupPassword;
  }

  protected void setGroupPassword(String groupPassword) {
    this.groupPassword = groupPassword;
  }

  public OMulticastConfguration getMulticast() {
    return multicast;
  }

  protected void setMulticast(OMulticastConfguration multicast) {
    this.multicast = multicast;
  }

  public static ONodeConfigurationBuilder builder() {
    return new ONodeConfigurationBuilder();
  }
}
