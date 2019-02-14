package com.orientechnologies.orient.core.db.config;

import java.util.UUID;

public class ONodeConfiguration {
  private ONodeIdentity          nodeIdentity;
  private String                 groupName;
  private String                 groupPassword;
  private int                    quorum;
  private Integer                tcpPort;
  private OMulticastConfguration multicast = new OMulticastConfguration();

  protected ONodeConfiguration() {
    nodeIdentity = new ONodeIdentity(UUID.randomUUID().toString(), "");
  }

  protected ONodeConfiguration(String nodeName, String groupName, String groupPassword, int quorum, Integer tcpPort,
      OMulticastConfguration multicast) {
    this.nodeIdentity = new ONodeIdentity(UUID.randomUUID().toString(), nodeName);
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

  public ONodeIdentity getNodeIdentity() {
    return nodeIdentity;
  }

  protected void setNodeIdentity(ONodeIdentity nodeIdentity) {
    this.nodeIdentity = nodeIdentity;
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
