package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 03.08.12
 */
public interface OStoragePhysicalClusterConfiguration extends OStorageClusterConfiguration {
  public OStorageFileConfiguration[] getInfoFiles();

  public String getMaxSize();
}
