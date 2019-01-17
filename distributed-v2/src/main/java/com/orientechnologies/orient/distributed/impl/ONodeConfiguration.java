package com.orientechnologies.orient.distributed.impl;

public class ONodeConfiguration {
  private String nodeName;
  private String groupName;
  private String groupPassword;

  private int quorum;

  private String connectionUsername;
  private String connectionPassword;
  private int    tcpPort;

  public int getQuorum() {
    return quorum;
  }

  public void setQuorum(int quorum) {
    this.quorum = quorum;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public String getConnectionUsername() {
    return connectionUsername;
  }

  public void setConnectionUsername(String connectionUsername) {
    this.connectionUsername = connectionUsername;
  }

  public String getConnectionPassword() {
    return connectionPassword;
  }

  public void setConnectionPassword(String connectionPassword) {
    this.connectionPassword = connectionPassword;
  }

  public int getTcpPort() {
    return tcpPort;
  }

  public void setTcpPort(int tcpPort) {
    this.tcpPort = tcpPort;
  }

  public String getGroupPassword() {
    return groupPassword;
  }

  public void setGroupPassword(String groupPassword) {
    this.groupPassword = groupPassword;
  }
}
