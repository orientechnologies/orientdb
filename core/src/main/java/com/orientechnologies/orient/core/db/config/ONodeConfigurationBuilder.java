package com.orientechnologies.orient.core.db.config;

import java.util.UUID;

public class ONodeConfigurationBuilder {

  private int quorum = 2;
  private String nodeName = UUID.randomUUID().toString();
  private String groupName = "OrientDB";
  private Integer tcpPort = null;
  private String groupPassword = "OrientDB";
  private OMulticastConfguration multicastConfguration;
  private OUDPUnicastConfiguration unicastConfiguration;

  protected ONodeConfigurationBuilder() {}

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

  public ONodeConfigurationBuilder setUnicast(OUDPUnicastConfiguration config) {
    this.unicastConfiguration = config;
    return this;
  }

  public ONodeConfiguration build() {
    if (multicastConfguration != null) {
      return new ONodeConfiguration(
          nodeName, groupName, groupPassword, quorum, tcpPort, multicastConfguration);
    } else if (unicastConfiguration != null) {
      return new ONodeConfiguration(
          nodeName, groupName, groupPassword, quorum, tcpPort, unicastConfiguration);
    } else {
      // empty multicast as fallback... review...
      return new ONodeConfiguration(
          nodeName, groupName, groupPassword, quorum, tcpPort, new OMulticastConfguration());
    }
  }
}
