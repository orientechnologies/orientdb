package com.orientechnologies.orient.core.db.config;

public class ONodeConfiguration {
  // Node name is redundant because it can come also from user configuration appart been stored in
  // the node identity
  private final String nodeName;
  private final String groupName;
  private final String groupPassword;
  private final int quorum;
  private final Integer tcpPort;
  private final OMulticastConfguration multicast;
  private final OUDPUnicastConfiguration udpUnicast;
  private final OLocalBinaryListenersConfig listeners;

  protected ONodeConfiguration(
      String nodeName,
      String groupName,
      String groupPassword,
      int quorum,
      Integer tcpPort,
      OMulticastConfguration multicast,
      OLocalBinaryListenersConfig listeners) {
    this.nodeName = nodeName;
    this.groupName = groupName;
    this.groupPassword = groupPassword;
    this.quorum = quorum;
    this.tcpPort = tcpPort;
    this.multicast = multicast;
    this.udpUnicast = null;
    this.listeners = listeners;
  }

  protected ONodeConfiguration(
      String nodeName,
      String groupName,
      String groupPassword,
      int quorum,
      Integer tcpPort,
      OUDPUnicastConfiguration unicastConfig,
      OLocalBinaryListenersConfig listeners) {
    this.nodeName = nodeName;
    this.groupName = groupName;
    this.groupPassword = groupPassword;
    this.quorum = quorum;
    this.tcpPort = tcpPort;
    this.multicast = null;
    this.udpUnicast = unicastConfig;
    this.listeners = listeners;
  }

  public int getQuorum() {
    return quorum;
  }

  public String getGroupName() {
    return groupName;
  }

  public Integer getTcpPort() {
    return tcpPort;
  }

  public String getGroupPassword() {
    return groupPassword;
  }

  public OMulticastConfguration getMulticast() {
    return multicast;
  }

  public static ONodeConfigurationBuilder builder() {
    return new ONodeConfigurationBuilder();
  }

  public String getNodeName() {
    return nodeName;
  }

  public OUDPUnicastConfiguration getUdpUnicast() {
    return udpUnicast;
  }

  public OLocalBinaryListenersConfig getListeners() {
    return listeners;
  }
}
