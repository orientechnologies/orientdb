package com.orientechnologies.orient.core.osystem;

import com.orientechnologies.orient.core.id.ONodeId;

public interface OSystemInfo {

  public ONodeId getNodeId();

  public String getNodeName();

  public void setNodeName(String nodeName);
}
