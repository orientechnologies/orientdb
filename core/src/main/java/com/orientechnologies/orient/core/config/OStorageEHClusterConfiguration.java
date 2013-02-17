package com.orientechnologies.orient.core.config;

import java.io.Serializable;

/**
 * @author Andrey Lomakin
 * @since 11.02.13
 */
public class OStorageEHClusterConfiguration implements OStorageClusterConfiguration, Serializable {
  public transient OStorageConfiguration root;

  public int                             id;
  public String                          name;
  public String                          location;
  public int                             dataSegmentId;

  public OStorageEHClusterConfiguration(OStorageConfiguration root, int id, String name, String location, int dataSegmentId) {
    this.root = root;
    this.id = id;
    this.name = name;
    this.location = location;
    this.dataSegmentId = dataSegmentId;
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
    return location;
  }

  @Override
  public int getDataSegmentId() {
    return dataSegmentId;
  }
}
