package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;

public class OStorageClusterConfigurationRemote implements OStorageClusterConfiguration {
  private final int id;
  private final String name;

  public OStorageClusterConfigurationRemote(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getLocation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDataSegmentId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public STATUS getStatus() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStatus(STATUS iStatus) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBinaryVersion() {
    throw new UnsupportedOperationException();
  }
}
