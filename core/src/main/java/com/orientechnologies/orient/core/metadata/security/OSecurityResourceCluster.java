package com.orientechnologies.orient.core.metadata.security;

import java.util.Objects;

public class OSecurityResourceCluster extends OSecurityResource {

  public static final OSecurityResourceCluster ALL_CLUSTERS =
      new OSecurityResourceCluster("database.cluster.*", "*");
  public static final OSecurityResourceCluster SYSTEM_CLUSTERS =
      new OSecurityResourceCluster("database.systemclusters", "");

  private final String clusterName;

  public OSecurityResourceCluster(String resourceString, String clusterName) {
    super(resourceString);
    this.clusterName = clusterName;
  }

  public String getClusterName() {
    return clusterName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSecurityResourceCluster that = (OSecurityResourceCluster) o;
    return Objects.equals(clusterName, that.clusterName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterName);
  }
}
