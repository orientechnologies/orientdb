package com.orientechnologies.orient.core.config;

public class OConfigDistributed {
  private String group;
  private String key;
  private String node;
  private int networkMinimumQuorum = 1;
  private OConfigDistributedTopology topology;
}
