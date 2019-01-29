package com.orientechnologies.orient.core.db.config;

public class ONodeConfigurationBuilder {

  ONodeConfiguration configuration;

  protected ONodeConfigurationBuilder() {
    configuration = new ONodeConfiguration();
  }

  public ONodeConfigurationBuilder setQuorum(int quorum) {
    configuration.setQuorum(quorum);
    return this;
  }

  public ONodeConfigurationBuilder setNodeName(String nodeName) {
    configuration.setNodeName(nodeName);
    return this;
  }

  public ONodeConfigurationBuilder setGroupName(String groupName) {
    configuration.setGroupName(groupName);
    return this;
  }

  public ONodeConfigurationBuilder setTcpPort(int tcpPort) {
    configuration.setTcpPort(tcpPort);
    return this;
  }

  public ONodeConfigurationBuilder setGroupPassword(String groupPassword) {
    configuration.setGroupPassword(groupPassword);
    return this;
  }

  public ONodeConfigurationBuilder setMulticast(OMulticastConfguration multicast) {
    configuration.setMulticast(multicast);
    return this;
  }

  public ONodeConfiguration build() {
    return new ONodeConfiguration(configuration.getNodeName(), configuration.getGroupName(), configuration.getGroupPassword(),
        configuration.getQuorum(), configuration.getTcpPort(), configuration.getMulticast());
  }
}
