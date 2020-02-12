package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.config.OStorageConfiguration;

import java.util.Set;

public interface OStorageInfo {

  // MISC
  OStorageConfiguration getConfiguration();

  boolean isAssigningClusterIds();

  Set<String> getClusterNames();

  int getClusters();

  int getDefaultClusterId();

  String getURL();
}
