package com.orientechnologies.orient.core.db.config;

public class ONodeConfigurationBuilder {

  private int                    quorum                = 2;
  private String                 nodeName              = "OrientDB";
  private String                 groupName             = "OrientDB";
  private int                    tcpPort               = 2424;
  private String                 groupPassword         = "OrientDB";
  private OMulticastConfguration multicastConfguration = new OMulticastConfguration();

  protected ONodeConfigurationBuilder() {
  }

  public ONodeConfigurationBuilder setQuorum(int quorum) {
    this.quorum = quorum;
    return this;
  }

  public ONodeConfigurationBuilder setNodeName(String nodeName) {
    this.nodeName = nodeName;
    return this;
  }

  public ONodeConfigurationBuilder setGroupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  public ONodeConfigurationBuilder setTcpPort(int tcpPort) {
    this.tcpPort = tcpPort;
    return this;
  }

  public ONodeConfigurationBuilder setGroupPassword(String groupPassword) {
    this.groupPassword = groupPassword;
    return this;
  }

  public ONodeConfigurationBuilder setMulticast(OMulticastConfguration multicast) {
    multicastConfguration = multicast;
    return this;
  }

  public ONodeConfiguration build() {
    return new ONodeConfiguration(nodeName, groupName, groupPassword, quorum, tcpPort, multicastConfguration);
  }
}
