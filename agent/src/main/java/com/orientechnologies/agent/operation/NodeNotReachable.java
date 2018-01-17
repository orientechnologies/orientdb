package com.orientechnologies.agent.operation;

public class NodeNotReachable implements NodeResponse {
  @Override
  public int getResponseType() {
    return 2;
  }
}
