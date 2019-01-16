package com.orientechnologies.orient.distributed.impl;

public class ONodeConfiguration {
  private String nodeName;
  private String groupName;
  private int    quorum;

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
}
